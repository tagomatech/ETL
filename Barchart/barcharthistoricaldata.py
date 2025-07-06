"""
2025-06-14 tagoma: barcharthistoricaldata.py
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
    """
    A requests.Session that knows how to talk to Barchart anonymously.

    Example
    -------
    >>> bc = BarchartHistoricalData()
    >>>
    >>> # Get daily data for AAPL with default settings
    >>> df_aapl_daily = bc.history("AAPL")
    >>> print("AAPL Daily Data (default maxrecords=640):")
    >>> print(df_aapl_daily.head())
    >>>
    >>> # Get weekly data for ZCK26 with 100 records and specific start date
    >>> df_zck_weekly = bc.history(
    ...     "ZCK26",
    ...     data="weekly",
    ...     maxrecords=100,
    ...     startDate="2023-01-01"
    ... )
    >>> print("\\nZCK26 Weekly Data (100 records from 2023-01-01):")
    >>> print(df_zck_weekly.head())
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
        self.get(ROOT, timeout=10)
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
        maxrecords: int | None = 640,
        data: Literal["daily", "weekly", "monthly"] = "daily",
        out: Literal["df", "dict", "text"] = "df",
        startDate: str | None = None,  # Added as explicit parameter
        endDate: str | None = None,    # Added as explicit parameter
        volume: Literal["total", "contract"] = "total", # Added as explicit parameter
        order: Literal["asc", "desc"] = "asc",         # Added as explicit parameter
        dividends: Literal["true", "false"] = "false", # Added as explicit parameter
        backadjust: Literal["true", "false"] = "false",# Added as explicit parameter
        daystoexpiration: int | None = 1, # Added as explicit parameter
        contractroll: Literal["none", "combined"] = "combined", # Added as explicit parameter
        # Keep **extra_params for any truly custom, less common params
        **extra_params: Any,
    ) -> Union[pd.DataFrame, list[dict], str]:
        """
        Pull historical prices for *symbol*.

        Parameters
        ----------
        symbol           : 'XRK21', 'AAPL', etc. (Required)
        maxrecords       : Number of rows to fetch (default: 640). Ignored if startDate is provided.
        data             : Granularity ('daily', 'weekly', 'monthly') (default: 'daily').
        out              : Output format: 'df' (pandas.DataFrame, default), 'dict' (list of dict rows), 'text' (raw CSV string).
        startDate        : Fetch data from this date (e.g., "YYYY-MM-DD"). Overrides maxrecords if present.
        endDate          : Fetch data up to this date (e.g., "YYYY-MM-DD").
        volume           : Type of volume to return ('total', 'contract') (default: 'total').
        order            : Order of results ('asc' for ascending date, 'desc' for descending date) (default: 'asc').
        dividends        : Include dividend adjustments ('true', 'false') (default: 'false').
        backadjust       : Back-adjust prices ('true', 'false') (default: 'false').
        daystoexpiration : Minimum days to expiration for futures contracts (default: 1).
        contractroll     : How to handle contract rolls for futures ('none', 'combined') (default: 'combined').
        extra_params     : Any additional query-string parameters Barchart supports not explicitly listed.

        Returns
        -------
        DataFrame | list[dict] | str    (depending on *out*)
        """
        # Build the parameters dictionary dynamically based on provided inputs
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

        # Handle mutually exclusive/dependent parameters
        if startDate:
            params["startDate"] = startDate
            # If startDate is provided, maxrecords is typically ignored by the API
            # So we ensure it's not sent if not explicitly needed with startDate
            if "maxrecords" in params:
                 del params["maxrecords"]
        elif maxrecords is not None:
            params["maxrecords"] = maxrecords # Only add if startDate not present

        if endDate:
            params["endDate"] = endDate

        # Merge any extra parameters provided by the user
        params.update(extra_params)

        hdrs = {
            "Accept": "text/plain, application/json",
            "Referer": f"{ROOT}/futures/quotes/{symbol}/historical-data",
            **self._xsrf_header(),
        }

        resp = self.get(API_EOD, params=params, headers=hdrs, timeout=15)

        # Print the URL here
        print(f"Calling URL: {resp.url}")

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
