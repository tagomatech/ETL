# futurescontinuoustimeseriesbuilder.py
# ET Nov25

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple, Union

import pandas as pd
import random
import re
import time

# ==============================
# Symbol & month utilities
# ==============================

_MONTH_CODE = {"F":1,"G":2,"H":3,"J":4,"K":5,"M":6,"N":7,"Q":8,"U":9,"V":10,"X":11,"Z":12}
_MONTH_CODE_INV = {v:k for k,v in _MONTH_CODE.items()}
_CONTRACT_RE = re.compile(r"^([A-Z]+)([FGHJKMNQUVXZ])(\d{2})$")

def parse_symbol(sym: str) -> Tuple[str, int, int, str]:
    """
    'KCZ25' -> ('KC', 12, 2025, 'Z')
    """
    m = _CONTRACT_RE.match(sym)
    if not m:
        raise ValueError(f"Cannot parse contract symbol: {sym}")
    root, mcode, yy = m.groups()
    year2 = int(yy)
    year4 = 2000 + year2 if year2 <= 69 else 1900 + year2
    return root, _MONTH_CODE[mcode], year4, mcode

def expiry_key(sym: str) -> int:
    _, m, y, _ = parse_symbol(sym)
    return y * 12 + m

def month_letters_to_nums(months: Iterable[str]) -> List[int]:
    return [_MONTH_CODE[m.strip().upper()] for m in months]

def step_symbol(sym: str, steps: int, cycle_months: List[int]) -> str:
    root, m, y, _ = parse_symbol(sym)
    pos = cycle_months.index(m)
    pos2 = pos + steps
    new_m = cycle_months[pos2 % len(cycle_months)]
    year_bump = pos2 // len(cycle_months)
    new_y = y + year_bump
    return f"{root}{_MONTH_CODE_INV[new_m]}{str(new_y)[-2:]}"


# Defaults for roots that don’t trade all 12 months (extend as needed)
DEFAULT_ROOT_CYCLES: Dict[str, List[int]] = {
    "KC": [3,5,7,9,12],  # ICE Coffee: H, K, N, U, Z
}


# ==============================
# Fetcher interface + adapter
# ==============================

class BaseFetcher:
    """
    Implement .fetch_one(symbol, start, end) -> DataFrame
    DataFrame must include a 'date' column and any subset of:
    'open','high','low','close','settlement','last', optional 'volume','openInterest'.
    """
    def fetch_one(self, symbol: str, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
        raise NotImplementedError


class BarchartFetcher(BaseFetcher):
    """
    Adapter for your BarchartHistoricalData client (has .history(...)->df).
    """
    def __init__(self, client, *, data: str = "daily", maxrecords: int = 640,
                 order: str = "asc", out: str = "df"):
        self.client = client
        self.data = data
        self.maxrecords = maxrecords
        self.order = order
        self.out = out

    def fetch_one(self, symbol: str, start: Optional[str], end: Optional[str]) -> pd.DataFrame:
        # throttle: fixed + random jitter
        time.sleep(1.0 + random.uniform(1.0, 4.5))

        return self.client.history(
            symbol=symbol,
            data=self.data,
            maxrecords=self.maxrecords,
            order=self.order,
            out=self.out,
            startDate=start,
            endDate=end,
        )


# ==============================
# Continuous nearby builder (simplified + anchored padding)
# ==============================

@dataclass(frozen=True)
class Segment:
    segment_start: pd.Timestamp
    segment_end: pd.Timestamp
    line: int
    source_symbol: str
    n_rows: int


class ContinuousFuturesBuilder:
    """
    Build continuous futures series (nearby k) by per-date expiry ranking.

    Simple API:
      - build_from_root(root, line_number, start, end, months=None, current_front=None, return_segments=False)
      - build(contracts_or_data, line_number, start=None, end=None, return_segments=False)

    Keeps all price fields it finds; does NOT synthesize a 'value' column.
    """

    def __init__(
        self,
        fetcher: Optional[BaseFetcher] = None,
        *,
        min_volume: Optional[int] = None,       # drop rows with volume <= min_volume (if volume exists)
        drop_incomplete_days: bool = True,      # drop dates with < k active contracts
        verbose: bool = True,
    ):
        self.fetcher = fetcher
        self.min_volume = min_volume
        self.drop_incomplete_days = drop_incomplete_days
        self.verbose = verbose

    # ---------- High-level: build from a ROOT (infers contracts for the window) ----------

    def build_from_root(
        self,
        root: str,
        *,
        line_number: int,
        start: str,
        end: str,
        months: Optional[Union[str, Iterable[str]]] = None,
        current_front: Optional[str] = None,   # <-- NEW: user-declared current 1st nearby (e.g., "KCZ25")
        return_segments: bool = False,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame]]:
        if self.fetcher is None:
            raise ValueError("build_from_root requires a fetcher (e.g., BarchartFetcher).")

        if months is None:
            cycle = DEFAULT_ROOT_CYCLES.get(root.upper(), list(range(1, 13)))
        else:
            if isinstance(months, str):
                months = [m for m in months.split() if m]
            cycle = month_letters_to_nums(months)

        root = root.upper()
        s_dt = pd.to_datetime(start).normalize()
        e_dt = pd.to_datetime(end).normalize()

        # 1) Base ladder over [start, end]
        ladder = self._generate_symbol_ladder(root, cycle, s_dt, e_dt)

        # 2) Decide which symbol should be the "front at end"
        if current_front is not None:
            r2, _, _, _ = parse_symbol(current_front)
            if r2.upper() != root:
                raise ValueError(f"current_front '{current_front}' root != '{root}'")
            front_at_end = current_front
        else:
            front_at_end = self._front_symbol_at_date(root, cycle, e_dt)

        # 3) Pad FORWARD just enough so nearby-k exists at end
        #    We need to reach 'front_at_end' from the ladder's last symbol (if not already),
        #    then add (k - 1) more months after that.
        if not ladder:
            # Start from the front_at_end and walk backward? Simpler: start from a seed just before front_at_end.
            # But since we only fetch forward months minimally, seed with front_at_end directly.
            ladder = [front_at_end]

        last_sym = ladder[-1]
        pads = self._steps_forward(last_sym, front_at_end, cycle) + max(line_number - 1, 0)

        to_add: List[str] = []
        cur = last_sym
        for _ in range(pads):
            cur = step_symbol(cur, 1, cycle)
            to_add.append(cur)

        # Merge, de-duplicate (preserve order)
        merged = ladder + to_add
        seen: set[str] = set()
        symbols = [s for s in merged if not (s in seen or seen.add(s))]

        # 4) Delegate to regular build using this inferred list
        return self.build(symbols, line_number=line_number, start=start, end=end, return_segments=return_segments)

    # ---------- Build from explicit symbols (list) or prefetched dict ----------

    def build(
        self,
        contracts_or_data: Union[Iterable[str], Dict[str, pd.DataFrame]],
        *,
        line_number: int,
        start: Optional[str] = None,
        end: Optional[str] = None,
        return_segments: bool = False,
    ) -> Union[pd.DataFrame, Tuple[pd.DataFrame, pd.DataFrame]]:
        if line_number < 1:
            raise ValueError("line_number must be >= 1")

        data = self._as_data_dict(contracts_or_data, start, end)
        if not data:
            empty = self._empty_series_df()
            return (empty, pd.DataFrame()) if return_segments else empty

        all_df = self._concat_with_expiry(data)

        # Clip
        if start is not None:
            all_df = all_df[all_df["date"] >= pd.to_datetime(start)]
        if end is not None:
            all_df = all_df[all_df["date"] <= pd.to_datetime(end)]
        if all_df.empty:
            empty = self._empty_series_df()
            return (empty, pd.DataFrame()) if return_segments else empty

        # Optional volume filter
        if self.min_volume is not None and "volume" in all_df.columns:
            all_df = all_df[all_df["volume"] > int(self.min_volume)]

        # De-dupe
        all_df = all_df.sort_values(["symbol","date"]).drop_duplicates(subset=["symbol","date"], keep="last")

        # Per-date ranking by expiry (earliest first)
        counts = all_df.groupby("date")["source_symbol"].nunique().rename("active_count")
        all_df = all_df.merge(counts, on="date", how="left")
        all_df["rank"] = all_df.groupby("date")["exp_key"].rank(method="first", ascending=True)

        # Select nearby-k
        k = float(line_number)
        pick = all_df[all_df["rank"] == k].copy()
        if self.drop_incomplete_days:
            pick = pick[pick["active_count"] >= line_number]

        pick["line"] = line_number

        # Output columns: keep what exists, no synthetic 'value'
        base_cols = ["date","line","source_symbol","symbol"]
        price_cols = [c for c in ["open","high","low","close","settlement","last"] if c in pick.columns]
        other_cols = [c for c in ["volume","openInterest"] if c in pick.columns]
        out_cols = [c for c in base_cols + price_cols + other_cols if c in pick.columns]

        series_df = pick[out_cols].sort_values("date").reset_index(drop=True)

        if not return_segments:
            return series_df

        seg_df = self._segments_from_series(series_df, line_number=line_number)
        return series_df, seg_df

    # ---------- Internals ----------

    @staticmethod
    def _generate_symbol_ladder(root: str, cycle_months: List[int], start_dt: pd.Timestamp, end_dt: pd.Timestamp) -> List[str]:
        month_set = set(cycle_months)
        symbols: List[str] = []
        seen: set[str] = set()
        for dt in pd.date_range(start_dt, end_dt, freq="MS"):   # month starts within window
            if dt.month in month_set:
                sym = f"{root}{_MONTH_CODE_INV[dt.month]}{dt.year%100:02d}"
                if sym not in seen:
                    symbols.append(sym); seen.add(sym)
        return symbols

    @staticmethod
    def _front_symbol_at_date(root: str, cycle_months: List[int], date_dt: pd.Timestamp) -> str:
        """
        Compute which contract month would be 'front' around a calendar date,
        assuming roll to the *next cycle month* after the last one in/before `date_dt`.
        """
        y = date_dt.year
        m = date_dt.month
        after_or_equal = [cm for cm in sorted(cycle_months) if cm >= m]
        if after_or_equal:
            front_m = after_or_equal[0]
            front_y = y
        else:
            front_m = sorted(cycle_months)[0]
            front_y = y + 1
        return f"{root}{_MONTH_CODE_INV[front_m]}{front_y%100:02d}"

    @staticmethod
    def _steps_forward(from_sym: str, to_sym: str, cycle_months: List[int]) -> int:
        """
        Non-negative steps from `from_sym` to `to_sym` along the cycle.
        If `to_sym` is not ahead, returns 0.
        """
        from_key = expiry_key(from_sym)
        to_key = expiry_key(to_sym)
        if to_key <= from_key:
            return 0
        steps = 0
        cur = from_sym
        # cycles are short; a small safe cap avoids accidental loops
        for _ in range(48):
            if cur == to_sym:
                return steps
            cur = step_symbol(cur, 1, cycle_months)
            steps += 1
        raise RuntimeError(f"Could not reach {to_sym} from {from_sym} with cycle {cycle_months}")

    def _as_data_dict(
        self,
        contracts_or_data: Union[Iterable[str], Dict[str, pd.DataFrame]],
        start: Optional[str],
        end: Optional[str],
    ) -> Dict[str, pd.DataFrame]:
        if isinstance(contracts_or_data, dict):
            data: Dict[str, pd.DataFrame] = {}
            for sym, df in contracts_or_data.items():
                n = self._normalize_contract_df(df, sym)
                if not n.empty:
                    data[sym] = n
            if not data and self.verbose:
                print("No contracts retained after normalization.")
            return data

        if self.fetcher is None:
            raise ValueError("No fetcher provided. Pass a dict, or initialize with a fetcher.")

        symbols = sorted(set(contracts_or_data), key=expiry_key)
        out: Dict[str, pd.DataFrame] = {}
        for sym in symbols:
            if self.verbose:
                print(f"Downloading {sym}…")
            df = self.fetcher.fetch_one(sym, start, end)
            if df is None or len(df) == 0:
                if self.verbose:
                    print(f"Skipping {sym} — empty fetch.")
                continue
            n = self._normalize_contract_df(df, sym)
            if not n.empty:
                out[sym] = n
            elif self.verbose:
                print(f"Skipping {sym} — empty after normalization.")
        return out

    @staticmethod
    def _to_naive_datetime(s: pd.Series) -> pd.Series:
        return pd.to_datetime(s, utc=True, errors="coerce").dt.tz_convert(None)

    def _normalize_contract_df(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        out = df.copy()

        # date
        date_col = next((c for c in ("date","tradeDate","timestamp","Date") if c in out.columns), None)
        if date_col is None:
            raise KeyError("No date-like column found (expected one of: date, tradeDate, timestamp, Date).")
        out["date"] = self._to_naive_datetime(out[date_col])
        out = out[out["date"].notna()]

        # lower-case mapping for flexible renames
        lower = {c.lower(): c for c in out.columns}
        def pull(*aliases: str) -> Optional[str]:
            for a in aliases:
                if a in lower:
                    return lower[a]
            return None

        OPEN  = pull("open")
        HIGH  = pull("high")
        LOW   = pull("low")
        CLOSE = pull("close","last")
        SETTL = pull("settlement","settle","set","sett")
        LAST  = pull("last")
        VOL   = pull("volume","vol")
        OI    = pull("openinterest","open_interest","oi")

        cols = ["date", "symbol"]
        out["symbol"] = symbol

        rename_map = {}
        for src, dst in [(OPEN,"open"), (HIGH,"high"), (LOW,"low"),
                         (CLOSE,"close"), (SETTL,"settlement"), (LAST,"last"),
                         (VOL,"volume"), (OI,"openinterest")]:
            if src:
                rename_map[src] = dst
                cols.append(src)

        out = out[cols].rename(columns=rename_map)

        # numeric coercion
        for c in ["open","high","low","close","settlement","last","volume","openinterest"]:
            if c in out.columns:
                out[c] = pd.to_numeric(out[c], errors="coerce")

        if "openinterest" in out.columns:
            out = out.rename(columns={"openinterest":"openInterest"})

        return out.sort_values("date").dropna(subset=["date"]).reset_index(drop=True)

    @staticmethod
    def _concat_with_expiry(data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        frames = []
        for sym, df in data.items():
            ek = expiry_key(sym)
            f = df.copy()
            f["source_symbol"] = sym
            f["exp_key"] = ek
            frames.append(f)
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _segments_from_series(series_df: pd.DataFrame, *, line_number: int) -> pd.DataFrame:
        rows: List[Segment] = []
        if not series_df.empty:
            s = series_df.copy()
            s["prev_sym"] = s["source_symbol"].shift()
            bounds = s.index[(s["source_symbol"] != s["prev_sym"])].tolist() + [len(s)]
            i = 0
            for j in bounds:
                if i == j:
                    i = j
                    continue
                chunk = s.iloc[i:j]
                rows.append(
                    Segment(
                        segment_start=chunk["date"].min().normalize(),
                        segment_end=chunk["date"].max().normalize(),
                        line=series_df["line"].iat[0],
                        source_symbol=chunk["source_symbol"].iat[0],
                        n_rows=len(chunk),
                    )
                )
                i = j
        return pd.DataFrame([r.__dict__ for r in rows])

    @staticmethod
    def _empty_series_df() -> pd.DataFrame:
        return pd.DataFrame(columns=[
            "date","line","source_symbol","symbol",
            "open","high","low","close","settlement","last",
            "volume","openInterest"
        ])
