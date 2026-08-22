"""Typed, session-backed access to Barchart historical time-series data."""

from __future__ import annotations

import csv
import io
import json
import urllib.parse
from collections.abc import Mapping
from typing import Any, Literal, TypeAlias

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .exceptions import (
    BarchartDecodeError,
    BarchartResponseError,
    BarchartTransportError,
)

ROOT = "https://www.barchart.com"
API_EOD = f"{ROOT}/proxies/timeseries/historical/queryeod.ashx"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125 Safari/537.36"
)
HistoryOutput: TypeAlias = pd.DataFrame | list[dict[str, Any]] | dict[str, Any] | str
Timeout: TypeAlias = float | tuple[float, float]
_ALLOWED_FREQUENCIES = {"daily", "weekly", "monthly"}
_ALLOWED_OUTPUTS = {"df", "dict", "text"}


class BarchartClient(requests.Session):
    """HTTP client for Barchart historical data.

    The class remains a requests.Session for backwards compatibility, but
    adds a bounded timeout, retry policy, typed errors, and a deterministic
    response decoder. The web-session handshake is enabled by default to match
    the original package behavior. Pass handshake=False when the caller
    manages cookies or is testing the decoder offline.
    """

    def __init__(
        self,
        *,
        ua: str | None = None,
        handshake: bool = True,
        handshake_timeout: Timeout = 10.0,
        request_timeout: Timeout = 15.0,
        max_retries: int = 2,
        retry_backoff_factor: float = 0.25,
    ) -> None:
        super().__init__()
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if retry_backoff_factor < 0:
            raise ValueError("retry_backoff_factor must be >= 0")

        self.request_timeout = _validate_timeout(request_timeout, "request_timeout")
        self.headers.update({"User-Agent": ua or DEFAULT_USER_AGENT})
        self._configure_retries(max_retries, retry_backoff_factor)

        if handshake:
            response = self._request(
                ROOT,
                timeout=_validate_timeout(handshake_timeout, "handshake_timeout"),
            )
            if not self._xsrf_cookie():
                raise RuntimeError(
                    "Barchart did not return an XSRF-TOKEN cookie during the "
                    "authentication handshake."
                )

    def _configure_retries(self, max_retries: int, backoff_factor: float) -> None:
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
        self.mount("http://", adapter)
        self.mount("https://", adapter)

    def _request(self, url: str, **kwargs: Any) -> requests.Response:
        try:
            response = self.get(url, **kwargs)
            response.raise_for_status()
        except requests.HTTPError as exc:
            status_code = getattr(exc.response, "status_code", None)
            raise BarchartResponseError(
                f"Barchart returned HTTP status {status_code}.",
                status_code=status_code,
            ) from exc
        except requests.RequestException as exc:
            raise BarchartTransportError(
                f"Barchart request failed for {url}: {exc}"
            ) from exc
        return response

    def _xsrf_cookie(self) -> str | None:
        for cookie in self.cookies:
            if cookie.name == "XSRF-TOKEN":
                return cookie.value
        return None

    def _xsrf_header(self) -> dict[str, str]:
        token = self._xsrf_cookie()
        if not token:
            raise RuntimeError(
                "Barchart session has no XSRF-TOKEN cookie. "
                "Construct the client with handshake=True or provide a "
                "prepared session first."
            )
        return {"X-XSRF-TOKEN": urllib.parse.unquote(token)}

    def history(
        self,
        symbol: str,
        *,
        maxrecords: int | None = 640,
        data: Literal["daily", "weekly", "monthly"] = "daily",
        out: Literal["df", "dict", "text"] = "df",
        startDate: str | None = None,
        endDate: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        volume: Literal["total", "contract"] = "total",
        order: Literal["asc", "desc"] = "asc",
        dividends: Literal["true", "false"] = "false",
        backadjust: Literal["true", "false"] = "false",
        daystoexpiration: int | None = 1,
        contractroll: Literal["none", "combined"] = "combined",
        **extra_params: Any,
    ) -> HistoryOutput:
        """Fetch and decode one symbol's historical response.

        start_date and end_date are the preferred snake-case names.
        startDate and endDate remain supported for existing callers.
        Additional endpoint parameters can be supplied as keyword arguments,
        but they cannot silently replace the stable arguments above.
        """

        symbol = symbol.strip()
        if not symbol:
            raise ValueError("symbol must not be empty")
        if data not in _ALLOWED_FREQUENCIES:
            raise ValueError(f"data must be one of: {sorted(_ALLOWED_FREQUENCIES)}")
        if out not in _ALLOWED_OUTPUTS:
            raise ValueError(f"out must be one of: {sorted(_ALLOWED_OUTPUTS)}")
        if maxrecords is not None and maxrecords < 1:
            raise ValueError("maxrecords must be >= 1 or None")
        if daystoexpiration is not None and daystoexpiration < 0:
            raise ValueError("daystoexpiration must be >= 0 or None")

        start = _coalesce_date(start_date, startDate, "start_date", "startDate")
        end = _coalesce_date(end_date, endDate, "end_date", "endDate")
        if start is not None and end is not None:
            if pd.Timestamp(start) > pd.Timestamp(end):
                raise ValueError("start_date must be earlier than or equal to end_date")

        params: dict[str, Any] = {
            "symbol": symbol,
            "data": data,
            "volume": volume,
            "order": order,
            "dividends": dividends,
            "backadjust": backadjust,
            "daystoexpiration": daystoexpiration,
            "contractroll": contractroll,
        }
        if start is not None:
            params["startDate"] = start
        elif maxrecords is not None:
            params["maxrecords"] = maxrecords
        if end is not None:
            params["endDate"] = end

        conflicts = set(params).intersection(extra_params)
        if conflicts:
            names = ", ".join(sorted(conflicts))
            raise ValueError(f"extra parameters cannot override: {names}")
        params.update(extra_params)

        response = self._request(
            API_EOD,
            params=params,
            headers={
                "Accept": "text/plain, application/json",
                "Referer": f"{ROOT}/futures/quotes/{symbol}/historical-data",
                **self._xsrf_header(),
            },
            timeout=self.request_timeout,
        )
        decoded = self._decode_response(
            response.text,
            content_type=response.headers.get("content-type", ""),
            symbol=symbol,
            out=out,
            status_code=response.status_code,
        )
        if out == "df" and isinstance(decoded, pd.DataFrame):
            return _clip_frame(decoded, start=start, end=end)
        return decoded

    @classmethod
    def _decode_response(
        cls,
        body: str,
        *,
        content_type: str,
        symbol: str,
        out: Literal["df", "dict", "text"],
        status_code: int,
    ) -> HistoryOutput:
        body = body.strip()
        if not body:
            raise BarchartDecodeError(
                f"Barchart returned an empty response for {symbol} "
                f"(status {status_code})."
            )
        if body.lower().startswith("error:"):
            raise BarchartDecodeError(f"Barchart API error for {symbol}: {body}")
        if out == "text":
            return body

        media_type = (content_type or "").split(";", 1)[0].strip().lower()
        if media_type == "application/json" or body[0] in "[{":
            try:
                payload: Any = json.loads(body)
            except json.JSONDecodeError as exc:
                raise BarchartDecodeError(
                    f"Barchart returned invalid JSON for {symbol}: {exc}"
                ) from exc
            if isinstance(payload, Mapping) and (
                "error" in payload or "errors" in payload
            ):
                raise BarchartDecodeError(
                    f"Barchart API returned an error for {symbol}: {payload}"
                )
            if out == "dict":
                return payload
            records = _records_from_json(payload)
            return pd.DataFrame(records)

        frame = cls._decode_csv(body, symbol=symbol)
        return frame if out == "df" else frame.to_dict(orient="records")

    @staticmethod
    def _decode_csv(body: str, *, symbol: str) -> pd.DataFrame:
        rows = [row for row in csv.reader(io.StringIO(body)) if any(row)]
        if not rows:
            raise BarchartDecodeError(f"Barchart returned no CSV rows for {symbol}.")

        first = [field.strip().lower().lstrip("﻿") for field in rows[0]]
        if "date" in first:
            raw = pd.read_csv(io.StringIO(body))
            lower = {str(column).strip().lower(): column for column in raw.columns}

            def source(*names: str) -> Any:
                for name in names:
                    if name in lower:
                        return lower[name]
                return None

            columns = {
                "symbol": source("symbol"),
                "date": source("date", "tradedate", "timestamp"),
                "open": source("open"),
                "high": source("high"),
                "low": source("low"),
                "close": source("close"),
                "volume": source("volume", "vol"),
                "openInterest": source(
                    "openinterest", "open_interest", "open interest", "oi"
                ),
            }
            if columns["date"] is None:
                raise BarchartDecodeError(
                    f"Barchart CSV has no date column for {symbol}."
                )
            frame = pd.DataFrame(
                {
                    name: raw[column]
                    for name, column in columns.items()
                    if column is not None
                }
            )
        else:
            widths = {len(row) for row in rows}
            if len(widths) != 1:
                raise BarchartDecodeError(
                    f"Barchart CSV rows have inconsistent widths for {symbol}."
                )
            width = widths.pop()
            if width == 8:
                names = [
                    "symbol",
                    "date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "openInterest",
                ]
            elif width == 7:
                names = ["symbol", "date", "open", "high", "low", "close", "volume"]
            elif width == 6:
                names = ["date", "open", "high", "low", "close", "volume"]
            else:
                raise BarchartDecodeError(
                    f"Unsupported Barchart CSV width {width} for {symbol}; "
                    "expected 6, 7, or 8 fields."
                )
            frame = pd.read_csv(io.StringIO(body), header=None, names=names)

        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        if frame["date"].isna().any():
            raise BarchartDecodeError(
                f"Barchart CSV contains an invalid date for {symbol}."
            )
        for column in ("open", "high", "low", "close", "volume", "openInterest"):
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        return frame


def _validate_timeout(value: Timeout, name: str) -> Timeout:
    if isinstance(value, tuple):
        if len(value) != 2 or any(part <= 0 for part in value):
            raise ValueError(f"{name} tuple values must be > 0")
        return value
    if value <= 0:
        raise ValueError(f"{name} must be > 0")
    return value


def _coalesce_date(
    preferred: str | None,
    legacy: str | None,
    preferred_name: str,
    legacy_name: str,
) -> str | None:
    if preferred is not None and legacy is not None and preferred != legacy:
        raise ValueError(f"{preferred_name} and {legacy_name} disagree")
    return preferred if preferred is not None else legacy


def _clip_frame(
    frame: pd.DataFrame,
    *,
    start: str | None,
    end: str | None,
) -> pd.DataFrame:
    """Enforce the requested inclusive date window on decoded history."""
    if "date" not in frame.columns:
        return frame

    dates = pd.to_datetime(
        frame["date"],
        errors="coerce",
        utc=True,
    ).dt.tz_convert(None)
    mask = dates.notna()
    if start is not None:
        start_timestamp = pd.Timestamp(start)
        if start_timestamp.tzinfo is not None:
            start_timestamp = start_timestamp.tz_convert("UTC").tz_localize(None)
        mask &= dates >= start_timestamp
    if end is not None:
        end_timestamp = pd.Timestamp(end)
        if end_timestamp.tzinfo is not None:
            end_timestamp = end_timestamp.tz_convert("UTC").tz_localize(None)
        mask &= dates <= end_timestamp
    return frame.loc[mask].reset_index(drop=True)


def _records_from_json(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        if not all(isinstance(row, Mapping) for row in payload):
            raise BarchartDecodeError("Barchart JSON list must contain objects.")
        return [dict(row) for row in payload]
    if isinstance(payload, Mapping):
        for key in ("data", "results", "rows"):
            nested = payload.get(key)
            if isinstance(nested, list):
                if not all(isinstance(row, Mapping) for row in nested):
                    raise BarchartDecodeError(
                        f"Barchart JSON field {key!r} must contain objects."
                    )
                return [dict(row) for row in nested]
            if isinstance(nested, Mapping):
                return [dict(nested)]
        return [dict(payload)]
    raise BarchartDecodeError("Barchart JSON payload must be an object or list.")


# Backwards-compatible public name used by the original package.
BarchartHistoricalData = BarchartClient
