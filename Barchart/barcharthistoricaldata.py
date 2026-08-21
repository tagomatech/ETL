"""Small client for Barchart historical time-series responses."""

from __future__ import annotations

import csv
import io
import json
import urllib.parse
from typing import Any, Literal, Union

import pandas as pd
import requests

ROOT = "https://www.barchart.com"
API_EOD = f"{ROOT}/proxies/timeseries/historical/queryeod.ashx"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/125 Safari/537.36"
)

HistoryOutput = Union[pd.DataFrame, list[dict[str, Any]], str]


class BarchartHistoricalData(requests.Session):
    """Session-backed client for Barchart historical data.

    By default the constructor performs the Barchart cookie handshake. Set
    ``handshake=False`` when a caller owns the session setup, or when testing
    response handling without making a network request.
    """

    def __init__(
        self,
        *,
        ua: str | None = None,
        handshake: bool = True,
        handshake_timeout: float = 10.0,
        request_timeout: float = 15.0,
    ) -> None:
        super().__init__()
        self.request_timeout = request_timeout
        self.headers.update({"User-Agent": ua or DEFAULT_USER_AGENT})

        if handshake:
            response = self.get(ROOT, timeout=handshake_timeout)
            response.raise_for_status()
            if not self.cookies.get("XSRF-TOKEN"):
                raise RuntimeError(
                    "Barchart did not return an XSRF-TOKEN cookie during the "
                    "authentication handshake."
                )

    def _xsrf_header(self) -> dict[str, str]:
        token = self.cookies.get("XSRF-TOKEN")
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
        volume: Literal["total", "contract"] = "total",
        order: Literal["asc", "desc"] = "asc",
        dividends: Literal["true", "false"] = "false",
        backadjust: Literal["true", "false"] = "false",
        daystoexpiration: int | None = 1,
        contractroll: Literal["none", "combined"] = "combined",
        **extra_params: Any,
    ) -> HistoryOutput:
        """Fetch and decode historical data for one symbol.

        ``extra_params`` is retained for endpoint options that are not part of
        this wrapper's stable signature. Explicit arguments are assembled
        first and can still be overridden deliberately through that mapping.
        """
        if not symbol or not symbol.strip():
            raise ValueError("symbol must not be empty")
        if out not in {"df", "dict", "text"}:
            raise ValueError("out must be one of: df, dict, text")

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

        if startDate:
            params["startDate"] = startDate
        elif maxrecords is not None:
            params["maxrecords"] = maxrecords

        if endDate:
            params["endDate"] = endDate

        params.update(extra_params)

        headers = {
            "Accept": "text/plain, application/json",
            "Referer": f"{ROOT}/futures/quotes/{symbol}/historical-data",
            **self._xsrf_header(),
        }
        response = self.get(
            API_EOD,
            params=params,
            headers=headers,
            timeout=self.request_timeout,
        )
        response.raise_for_status()

        return self._decode_response(
            response.text,
            content_type=response.headers.get("content-type", ""),
            symbol=symbol,
            out=out,
            status_code=response.status_code,
        )

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
            raise ValueError(
                f"Barchart returned an empty response for {symbol} "
                f"(status {status_code})."
            )
        if body.startswith("Error:"):
            raise ValueError(f"Barchart API error for {symbol}: {body}")
        if out == "text":
            return body

        media_type = content_type.split(";", 1)[0].strip().lower()
        if media_type == "application/json" or body[0] in "[{":
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Barchart returned invalid JSON for {symbol}: {exc}"
                ) from exc
            if out == "dict":
                return payload
            if isinstance(payload, dict):
                payload = [payload]
            return pd.DataFrame(payload)

        frame = cls._decode_csv(body, symbol=symbol)
        return frame if out == "df" else frame.to_dict(orient="records")

    @staticmethod
    def _decode_csv(body: str, *, symbol: str) -> pd.DataFrame:
        rows = [row for row in csv.reader(io.StringIO(body)) if any(row)]
        if not rows:
            raise ValueError(f"Barchart returned no CSV rows for {symbol}.")

        first = [field.strip().lower() for field in rows[0]]
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
                "openInterest": source("openinterest", "open_interest", "oi"),
            }
            if columns["date"] is None:
                raise ValueError(f"Barchart CSV has no date column for {symbol}.")
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
                raise ValueError(f"Barchart CSV rows have inconsistent widths for {symbol}.")
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
            elif width == 6:
                names = ["date", "open", "high", "low", "close", "volume"]
            else:
                raise ValueError(
                    f"Unsupported Barchart CSV width {width} for {symbol}; "
                    "expected 6 or 8 fields."
                )
            frame = pd.read_csv(io.StringIO(body), header=None, names=names)

        frame["date"] = pd.to_datetime(frame["date"], errors="coerce")
        if frame["date"].isna().any():
            raise ValueError(f"Barchart CSV contains an invalid date for {symbol}.")
        for column in ["open", "high", "low", "close", "volume", "openInterest"]:
            if column in frame.columns:
                frame[column] = pd.to_numeric(frame[column], errors="coerce")
        return frame
