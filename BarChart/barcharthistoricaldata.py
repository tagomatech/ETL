"""
2025-06-14 tagoma: barcharthistoricaldata.py
"""

from __future__ import annotations

import io
import json
import urllib.parse
from typing import Literal, Union

import pandas as pd
import requests

# --------------------------------------------------------------------------- #
BARROOT = "https://www.barchart.com"
API_EOD = f"{BARROOT}/proxies/timeseries/historical/queryeod.ashx"


class BarchartHistoricalData(requests.Session):
    """
    A requests.Session that knows how to talk to Barchart anonymously.

    Example
    -------
    >>> bc = BarchartSession()
    >>> df = bc.history("XRK21")      # DataFrame with symbol/date/ohlc/vol/OI
    >>> df.head()
    """

    # --------------------------------------------------------------------- #
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

        # 1️⃣  Hit any front-end page once so the server sets 'XSRF-TOKEN' cookie
        self.get(BARROOT, timeout=10)
        if "XSRF-TOKEN" not in self.cookies:
            raise RuntimeError(
                "No XSRF-TOKEN cookie – Barchart auth handshake failed."
            )

    # --------------------------------------------------------------------- #
    def _xsrf_header(self) -> dict[str, str]:
        """Return the required X-XSRF-TOKEN header (URL-decoded cookie value)."""
        token = urllib.parse.unquote(self.cookies["XSRF-TOKEN"])
        return {"X-XSRF-TOKEN": token}

    # --------------------------------------------------------------------- #
    def history(
        self,
        symbol: str,
        *,
        maxrecords: int = 640,
        data: Literal["daily", "weekly", "monthly"] = "daily",
        out: Literal["df", "dict", "text"] = "df",
        **overrides,
    ) -> Union[pd.DataFrame, list[dict], str]:
        """
        Pull historical prices for *symbol*.

        Parameters
        ----------
        symbol       : 'XRK21', 'AAPL', etc.
        maxrecords   : number of rows to fetch (ignored if you supply startDate)
        data         : granularity ('daily', 'weekly', 'monthly')
        out          : 'df'   → pandas.DataFrame (default)
                       'dict' → list of dict rows
                       'text' → raw CSV string
        overrides    : any extra query-string param Barchart supports.

        Returns
        -------
        DataFrame | list[dict] | str   (depending on *out*)
        """
        params = {
            "symbol": symbol,
            "data": data,
            "maxrecords": maxrecords,
            "volume": "total",
            "order": "asc",
            "dividends": "false",
            "backadjust": "false",
            "daystoexpiration": 1,
            "contractroll": "combined",
        } | overrides

        hdrs = {
            "Accept": "text/plain, application/json",
            "Referer": f"{BARROOT}/futures/quotes/{symbol}/historical-data",
            **self._xsrf_header(),
        }

        resp = self.get(API_EOD, params=params, headers=hdrs, timeout=15)
        resp.raise_for_status()
        body = resp.text.strip()
        first_line = body.splitlines()[0]

        # ── CSV payload (the normal case) ────────────────────────────────── #
        is_plain_text = resp.headers.get("content-type", "").startswith("text")
        if is_plain_text or "," in first_line:
            if out == "text":
                return body

            # Futures CSV has 8 columns (symbol first), equities have 6 (date first)
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

        # ── JSON payload (rare endpoints) ────────────────────────────────── #
        if body and body[0] in "[{":
            payload = json.loads(body)
            if out == "text":
                return body
            return pd.DataFrame(payload) if out == "df" else payload

        # ── Unknown payload type ─────────────────────────────────────────── #
        preview = body[:60].replace("\n", r"\n")
        raise ValueError(
            f"Unrecognised payload – status {resp.status_code}, "
            f"first bytes: '{preview}'"
        )


if __name__ == "__main__":

    df = bc.history("XRK21", maxrecords=10)   # ← LIMIT TO 10 rows
    print(df)

