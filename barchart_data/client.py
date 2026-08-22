"""Authenticated transport for Barchart OnDemand REST endpoint families."""

from __future__ import annotations

import io
import json
import os
from collections.abc import Mapping
from typing import Any, Literal, TypeAlias

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import (
    BarchartAPIError,
    BarchartAuthenticationError,
    BarchartDecodeError,
    BarchartTransportError,
)

OutputFormat: TypeAlias = Literal["df", "json", "text"]
ResponsePayload: TypeAlias = dict[str, Any] | list[Any] | pd.DataFrame | str
DEFAULT_BASE_URL = "https://ondemand.websol.barchart.com"
DEFAULT_USER_AGENT = "barchart-data/0.3.0"


class BarchartDataClient:
    """Modern, resource-oriented client for Barchart OnDemand.

    The client does not call the network during construction. An API key can
    be passed explicitly or supplied through BARCHART_API_KEY. Endpoint
    methods return pandas DataFrames by default, while raw JSON remains
    available for fields that have not received a dedicated resource wrapper.
    """

    def __init__(
        self,
        api_key: str | None = None,
        *,
        base_url: str = DEFAULT_BASE_URL,
        timeout: float | tuple[float, float] = 15.0,
        max_retries: int = 2,
        session: requests.Session | None = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        self.api_key = api_key or os.getenv("BARCHART_API_KEY")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": DEFAULT_USER_AGENT})
        self._configure_session(max_retries)

        from .resources import FundamentalResource, MarketResource, MetadataResource

        self.market = MarketResource(self)
        self.fundamentals = FundamentalResource(self)
        self.metadata = MetadataResource(self)

    def _configure_session(self, max_retries: int) -> None:
        if not hasattr(self.session, "mount"):
            return
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=0.25,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET", "POST"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def close(self) -> None:
        """Close the underlying HTTP session."""

        self.session.close()

    def __enter__(self) -> "BarchartDataClient":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    def request(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        output: OutputFormat = "df",
    ) -> ResponsePayload:
        """Call any OnDemand endpoint and normalize its response.

        Endpoint names may be supplied as getQuote, getQuote.json, or
        getQuote.csv. The API key is managed by the client and cannot be
        overridden through params.
        """

        if output not in {"df", "json", "text"}:
            raise ValueError("output must be one of: df, json, text")
        normalized_endpoint = endpoint.strip().strip("/")
        if not normalized_endpoint:
            raise ValueError("endpoint must not be empty")
        if "apikey" in (params or {}):
            raise ValueError("pass api_key to the client, not through params")
        if not self.api_key:
            raise BarchartAuthenticationError(
                "No Barchart API key was supplied. Pass api_key= or set "
                "BARCHART_API_KEY."
            )

        if normalized_endpoint.endswith((".json", ".csv", ".xml")):
            request_path = normalized_endpoint
            response_format = normalized_endpoint.rsplit(".", 1)[-1]
        else:
            request_path = f"{normalized_endpoint}.json"
            response_format = "json"
        request_params = dict(params or {})
        request_params["apikey"] = self.api_key
        url = f"{self.base_url}/{request_path}"

        try:
            response = self.session.get(
                url,
                params=request_params,
                timeout=self.timeout,
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            raise BarchartAPIError(
                f"Barchart returned HTTP status {status_code}.",
                status_code=status_code,
                endpoint=normalized_endpoint,
            ) from exc
        except requests.RequestException as exc:
            raise BarchartTransportError(
                f"Barchart request failed for {normalized_endpoint}: {exc}"
            ) from exc

        if output == "text":
            return response.text
        if response_format == "csv":
            if output == "json":
                return response.text
            try:
                return pd.read_csv(io.StringIO(response.text))
            except (pd.errors.ParserError, ValueError) as exc:
                raise BarchartDecodeError(
                    f"Could not decode CSV from {normalized_endpoint}."
                ) from exc

        try:
            payload = response.json()
        except (ValueError, json.JSONDecodeError) as exc:
            raise BarchartDecodeError(
                f"Could not decode JSON from {normalized_endpoint}."
            ) from exc
        self._check_api_status(payload, endpoint=normalized_endpoint)
        if output == "json":
            return payload
        return _payload_to_frame(payload)

    def call(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        output: OutputFormat = "df",
    ) -> ResponsePayload:
        """Alias for request, useful for endpoint families not wrapped yet."""

        return self.request(endpoint, params=params, output=output)

    @staticmethod
    def _check_api_status(payload: Any, *, endpoint: str) -> None:
        if not isinstance(payload, Mapping):
            return
        status = payload.get("status")
        if not isinstance(status, Mapping):
            return
        code = status.get("code")
        if code is None or str(code) == "200":
            return
        raise BarchartAPIError(
            f"Barchart API error on {endpoint}: {status.get('message', code)}",
            status_code=int(code) if str(code).isdigit() else None,
            endpoint=endpoint,
            payload=payload,
        )


def _payload_to_frame(payload: Any) -> pd.DataFrame:
    if isinstance(payload, Mapping):
        records = payload.get("results", payload.get("data", payload))
    else:
        records = payload
    if records is None:
        return pd.DataFrame()
    if isinstance(records, Mapping):
        return pd.json_normalize(records)
    if isinstance(records, list):
        if not records:
            return pd.DataFrame()
        if all(isinstance(record, Mapping) for record in records):
            return pd.DataFrame(records)
        return pd.json_normalize(records)
    raise BarchartDecodeError("Barchart response does not contain tabular results.")


OnDemandClient = BarchartDataClient
