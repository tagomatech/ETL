"""
2025-07-13 tagoma: barcharthistoricaldata.py
"""

from __future__ import annotations

import io
import json
import urllib.parse
from typing import Literal, Union, Any
import pandas as pd
import requests

# --------------------------------------------------------------------------- #
ROOT = "https://www.barchart.com"
API_EOD = f"{ROOT}/proxies/timeseries/historical/queryeod.ashx"

class BarchartHistoricalData(requests.Session):
    def __init__(self, *, ua: str | None = None) -> None:
        super().__init__()
        self.headers.update(
            {
                "User-Agent": ua
                or (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125 Safari/537.36"
                )
            }
        )
        self.get(ROOT, timeout=10)
        if "XSRF-TOKEN" not in self.cookies:
            raise RuntimeError(
                "No XSRF-TOKEN cookie – Barchart auth handshake failed."
            )

    def _xsrf_header(self) -> dict[str, str]:
        token = urllib.parse.unquote(self.cookies["XSRF-TOKEN"])
        return {"X-XSRF-TOKEN": token}

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
    ) -> Union[pd.DataFrame, list[dict], str]:
        params = {
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
            if "maxrecords" in params:
                del params["maxrecords"]
        elif maxrecords is not None:
            params["maxrecords"] = maxrecords

        if endDate:
            params["endDate"] = endDate

        params.update(extra_params)

        hdrs = {
            "Accept": "text/plain, application/json",
            "Referer": f"{ROOT}/futures/quotes/{symbol}/historical-data",
            **self._xsrf_header(),
        }

        resp = self.get(API_EOD, params=params, headers=hdrs, timeout=15)
        print(f"Calling URL: {resp.url}") # Keep this for debugging

        resp.raise_for_status()
        body = resp.text.strip()
        
        # Check for Barchart's "Error: " response directly in the body
        if body.startswith("Error:"):
            raise ValueError(f"Barchart API returned an error for symbol {symbol}: {body}")

        first_line = body.splitlines()[0]

        is_plain_text = resp.headers.get("content-type", "").startswith("text")
        if is_plain_text or "," in first_line:
            if out == "text":
                return body

            cols = (
                ["symbol", "date", "open", "high", "low", "close", "volume", "openInterest"]
                if first_line.startswith(symbol)
                else ["date", "open", "high", "low", "close", "volume"]
            )
            
            df = pd.read_csv(
                io.StringIO(body),
                header=None,
                names=cols,
                parse_dates=["date"],
            )
            return df if out == "df" else df.to_dict(orient="records")

        if body and body[0] in "[{":
            payload = json.loads(body)
            if out == "text":
                return body
            return pd.DataFrame(payload) if out == "df" else payload

        preview = body[:60].replace("\n", r"\n")
        raise ValueError(
            f"Unrecognised payload – status {resp.status_code}, "
            f"first bytes: '{preview}'"
        )
