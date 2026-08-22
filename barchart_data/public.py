"""No-login access to data embedded in public Barchart quote pages.

This adapter reads the JSON block that Barchart publishes in quote-page HTML.
It is intentionally separate from the authenticated OnDemand client because
the public page feed is smaller, may be delayed, and can change independently.
"""

from __future__ import annotations

import html
import json
import re
from collections.abc import Mapping, Sequence
from typing import Any, Literal

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import (
    BarchartDecodeError,
    BarchartPublicPageError,
    BarchartTransportError,
)

PUBLIC_ROOT = "https://www.barchart.com"
INLINE_DATA_ID = "barchart-www-inline-data"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125 Safari/537.36"
)
PublicOutput = pd.DataFrame | list[dict[str, Any]]
_OUTPUTS = {"df", "json"}
_ASSET_CLASS = re.compile(r"^[A-Za-z0-9_-]+$")
_INLINE_SCRIPT = re.compile(
    r"<script(?=[^>]*\bid=[\"']"
    + re.escape(INLINE_DATA_ID)
    + r"[\"'])[^>]*>(.*?)</script>",
    flags=re.IGNORECASE | re.DOTALL,
)


class PublicBarchartClient:
    """Read quote-page data from Barchart without an API key or login.

    The client uses public overview pages such as
    /futures/quotes/ZCU26/overview. It does not bypass authentication or call
    undocumented authenticated endpoints.
    """

    def __init__(
        self,
        *,
        session: requests.Session | Any | None = None,
        web_base_url: str = PUBLIC_ROOT,
        user_agent: str | None = None,
        request_timeout: float | tuple[float, float] = 15.0,
        max_retries: int = 2,
        retry_backoff_factor: float = 0.25,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if retry_backoff_factor < 0:
            raise ValueError("retry_backoff_factor must be >= 0")
        self.request_timeout = _validate_timeout(request_timeout)
        self.web_base_url = web_base_url.rstrip("/")
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": user_agent or DEFAULT_USER_AGENT})
        self._configure_retries(max_retries, retry_backoff_factor)
        self._history_client: Any | None = None

    def _configure_retries(self, max_retries: int, backoff_factor: float) -> None:
        mount = getattr(self.session, "mount", None)
        if mount is None:
            return
        retry = Retry(
            total=max_retries,
            connect=max_retries,
            read=max_retries,
            status=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        mount("http://", adapter)
        mount("https://", adapter)

    def page_url(self, symbol: str, *, asset_class: str = "futures") -> str:
        """Return the public overview URL for a symbol."""
        symbol = _validate_symbol(symbol)
        asset_class = _validate_asset_class(asset_class)
        return f"{self.web_base_url}/{asset_class}/quotes/{symbol}/overview"

    def page(self, symbol: str, *, asset_class: str = "futures") -> dict[str, Any]:
        """Fetch and decode the complete embedded page payload."""
        symbol = _validate_symbol(symbol)
        url = self.page_url(symbol, asset_class=asset_class)
        try:
            response = self.session.get(url, timeout=self.request_timeout)
            response.raise_for_status()
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            raise BarchartPublicPageError(
                f"Barchart public page returned HTTP status {status_code} for {symbol}.",
                status_code=status_code,
                url=url,
            ) from exc
        except requests.RequestException as exc:
            raise BarchartTransportError(
                f"Barchart public page request failed for {url}: {exc}"
            ) from exc

        match = _INLINE_SCRIPT.search(response.text)
        if match is None:
            raise BarchartPublicPageError(
                f"Barchart public page has no {INLINE_DATA_ID!r} block for {symbol}.",
                status_code=response.status_code,
                url=url,
            )
        try:
            payload = json.loads(html.unescape(match.group(1).strip()))
        except json.JSONDecodeError as exc:
            raise BarchartDecodeError(
                f"Barchart public page contains invalid embedded JSON for {symbol}."
            ) from exc
        if not isinstance(payload, Mapping):
            raise BarchartDecodeError(
                f"Barchart public page payload for {symbol} must be an object."
            )
        return dict(payload)

    def quote(
        self,
        symbols: str | Sequence[str],
        *,
        asset_class: str = "futures",
        output: Literal["df", "json"] = "df",
    ) -> PublicOutput:
        """Return current quote fields plus instrument metadata."""
        records = []
        for requested_symbol in _symbols(symbols):
            payload = self.page(requested_symbol, asset_class=asset_class)
            entry = _select_symbol(payload, requested_symbol)
            quote = entry.get("quote", {})
            instrument = entry.get("instrument", {})
            if not isinstance(quote, Mapping) or not isinstance(instrument, Mapping):
                raise BarchartDecodeError(
                    f"Barchart public page payload for {requested_symbol} "
                    "has invalid quote or instrument data."
                )
            record: dict[str, Any] = {
                "symbol": requested_symbol,
                "asset_class": asset_class,
            }
            record.update(dict(quote))
            record.update(
                {f"instrument_{key}": value for key, value in instrument.items()}
            )
            records.append(record)
        return _format_output(records, output)

    def profile(
        self,
        symbols: str | Sequence[str],
        *,
        asset_class: str = "futures",
        output: Literal["df", "json"] = "df",
    ) -> PublicOutput:
        """Return instrument metadata embedded in public quote pages."""
        records = []
        for requested_symbol in _symbols(symbols):
            payload = self.page(requested_symbol, asset_class=asset_class)
            entry = _select_symbol(payload, requested_symbol)
            instrument = entry.get("instrument", {})
            if not isinstance(instrument, Mapping):
                raise BarchartDecodeError(
                    f"Barchart public page payload for {requested_symbol} "
                    "has invalid instrument data."
                )
            record = dict(instrument)
            record["symbol"] = requested_symbol
            record["asset_class"] = asset_class
            records.append(record)
        return _format_output(records, output)

    def history(self, symbol: str, **kwargs: Any) -> Any:
        """Use the existing public historical adapter without an API key.

        History has a separate endpoint and session handshake, so it is
        created lazily and only when this method is used.
        """
        if self._history_client is None:
            from .legacy import PublicWebHistoryClient

            self._history_client = PublicWebHistoryClient()
        return self._history_client.history(symbol, **kwargs)


PublicWebClient = PublicBarchartClient
BarchartPublicClient = PublicBarchartClient


def _validate_symbol(symbol: str) -> str:
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string")
    if "/" in symbol or "\\" in symbol:
        raise ValueError("symbol must not contain path separators")
    return symbol.strip()


def _validate_asset_class(asset_class: str) -> str:
    if not isinstance(asset_class, str) or not _ASSET_CLASS.fullmatch(asset_class):
        raise ValueError(
            "asset_class must contain only letters, numbers, underscores, or hyphens"
        )
    return asset_class


def _symbols(symbols: str | Sequence[str]) -> list[str]:
    if isinstance(symbols, str):
        values = [symbols]
    else:
        values = list(symbols)
    if not values:
        raise ValueError("symbols must not be empty")
    return [_validate_symbol(symbol) for symbol in values]


def _select_symbol(payload: Mapping[str, Any], requested_symbol: str) -> Mapping[str, Any]:
    if requested_symbol in payload and isinstance(payload[requested_symbol], Mapping):
        return payload[requested_symbol]
    requested_casefold = requested_symbol.casefold()
    for key, value in payload.items():
        if str(key).casefold() == requested_casefold and isinstance(value, Mapping):
            return value
    if "quote" in payload or "instrument" in payload:
        return payload
    raise BarchartDecodeError(
        f"Barchart public page payload has no entry for {requested_symbol}."
    )


def _format_output(
    records: list[dict[str, Any]], output: Literal["df", "json"]
) -> PublicOutput:
    if output not in _OUTPUTS:
        raise ValueError(f"output must be one of: {sorted(_OUTPUTS)}")
    return pd.DataFrame(records) if output == "df" else records


def _validate_timeout(
    value: float | tuple[float, float],
) -> float | tuple[float, float]:
    if isinstance(value, tuple):
        if len(value) != 2 or any(part <= 0 for part in value):
            raise ValueError("request_timeout tuple values must be > 0")
        return value
    if value <= 0:
        raise ValueError("request_timeout must be > 0")
    return value


__all__ = [
    "BarchartPublicClient",
    "PublicBarchartClient",
    "PublicWebClient",
]
