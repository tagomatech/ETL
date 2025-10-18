# bbgfuturesroll.py
# tagomatech Oct25

from __future__ import annotations
from typing import Sequence, Mapping, Optional, Union, Literal
from datetime import date, datetime
import pandas as pd
from blp import blp

def _to_bbg_date(d: Union[str, date, datetime]) -> str:
    ts = pd.to_datetime(d, errors="coerce")
    if pd.isna(ts):
        raise ValueError(f"Invalid date: {d!r}")
    return ts.strftime("%Y%m%d")  # Bloomberg BDH expects YYYYMMDD

def get_rolled_bbg_data(
    start_date: Union[str, date, datetime],
    end_date: Union[str, date, datetime],
    ticker: str,
    fields: Union[str, Sequence[str]],
    *,
    roll_type: Literal["backward", "forward"] = "backward",
    close_field: str = "PX_LAST",
    field_rename_map: Optional[Mapping[str, str]] = None,
) -> pd.DataFrame:
    """
    Build a continuous, roll-adjusted series from Bloomberg futures.

    roll_type:
      - "backward"  -> anchor to the most recent contract (adjust older history UP)
      - "forward"   -> anchor to the first contract (adjust newer history DOWN)
    """
    # --- inputs & required fields
    if isinstance(fields, str):
        fields = [fields]
    else:
        fields = list(fields)

    # Default rename map -> use BBG-style 'Last' instead of 'Close'
    if field_rename_map is None:
        field_rename_map = {
            "PX_OPEN": "Open",
            "PX_HIGH": "High",
            "PX_LOW":  "Low",
            close_field: "Last",  # map whichever close-like field to 'Last'
            "FUT_CUR_GEN_TICKER": "Ticker",
        }

    required_fields = {"FUT_CUR_GEN_TICKER", close_field}
    request_fields = sorted(set(fields).union(required_fields))

    # --- Bloomberg date format
    start_s = _to_bbg_date(start_date)
    end_s = _to_bbg_date(end_date)

    # --- fetch
    bquery = blp.BlpQuery().start()
    df = bquery.bdh(
        ticker,
        fields=request_fields,
        start_date=start_s,
        end_date=end_s,
    )
    if df is None or df.empty:
        raise ValueError("Bloomberg returned no data for the given inputs.")

    # --- clean / standardize
    if "date" not in df.columns:
        raise KeyError("Expected 'date' in BDH output.")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date").reset_index(drop=True)

    # rename to Open/High/Low/Last/Ticker
    present_map = {k: v for k, v in field_rename_map.items() if k in df.columns}
    df = df.rename(columns=present_map)

    if "Ticker" not in df.columns:
        raise KeyError("Missing FUT_CUR_GEN_TICKER → 'Ticker' in output.")
    if "Last" not in df.columns:
        raise KeyError(f"Missing close-like field '{close_field}' mapped to 'Last'.")

    # coerce numerics
    price_cols = [c for c in ("Open", "High", "Low", "Last") if c in df.columns]
    for c in price_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # need Ticker + Last for adjustment math
    df = df.dropna(subset=["Ticker", "Last"]).reset_index(drop=True)

    # --- contract segmentation
    df["Contract_ID"] = df["Ticker"].ne(df["Ticker"].shift()).cumsum()

    # --- roll gaps (next first - current last) measured on 'Last'
    grp = df.groupby("Contract_ID", sort=True)["Last"]
    last_per = grp.last()
    first_next = grp.first().shift(-1)
    gaps = (first_next - last_per).fillna(0.0)  # index: Contract_ID, len = N

    # Backward (anchor to last): sum of *future* gaps from i..end
    backward_adj = gaps.iloc[::-1].cumsum().iloc[::-1]

    # Forward (anchor to first): negative sum of *past* gaps up to i-1
    forward_adj = -(gaps.cumsum().shift(1).fillna(0.0))

    adj_by_contract = backward_adj if roll_type == "backward" else forward_adj
    df["Roll_Adjustment"] = df["Contract_ID"].map(adj_by_contract).astype(float)

    # adjusted = raw + adjustment  (map encodes direction)
    for c in price_cols:
        df[f"{c}_Adj"] = df[c] + df["Roll_Adjustment"]

    # --- column order: High, Low, Open, Last (BBG-friendly), then *_Adj the same order
    ordered_raw = [c for c in ("High", "Low", "Open", "Last") if c in df.columns]
    ordered_adj = [f"{c}_Adj" for c in ordered_raw if f"{c}_Adj" in df.columns]
    front = ["date", "Ticker", "Contract_ID", "Roll_Adjustment"] + ordered_raw + ordered_adj
    remaining = [c for c in df.columns if c not in front]

    return df.loc[:, front + remaining].reset_index(drop=True)
