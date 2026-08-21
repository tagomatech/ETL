"""Build nearby futures series from contract histories.

The module keeps the original functional API while separating three concerns:
contract-symbol rules, data fetching, and continuous-series construction.
"""

from __future__ import annotations

import logging
import random
import re
import time
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from typing import Any, Callable, Protocol, TypeAlias, runtime_checkable

import pandas as pd

from .exceptions import FuturesDataError

_MONTH_CODE = {
    "F": 1,
    "G": 2,
    "H": 3,
    "J": 4,
    "K": 5,
    "M": 6,
    "N": 7,
    "Q": 8,
    "U": 9,
    "V": 10,
    "X": 11,
    "Z": 12,
}
_MONTH_CODE_INV = {value: key for key, value in _MONTH_CODE.items()}
_CONTRACT_RE = re.compile("^([A-Z]+)([FGHJKMNQUVXZ])([0-9]{2})$")
_PRICE_COLUMNS = (
    "open",
    "high",
    "low",
    "close",
    "settlement",
    "last",
)
_METADATA_COLUMNS = ("volume", "openInterest")
SeriesResult: TypeAlias = pd.DataFrame | tuple[pd.DataFrame, pd.DataFrame]

DEFAULT_ROOT_CYCLES: dict[str, list[int]] = {
    "KC": [3, 5, 7, 9, 12],
    "ZC": [3, 5, 7, 9, 12],
}


def parse_symbol(symbol: str) -> tuple[str, int, int, str]:
    """Parse KCZ25 into ("KC", 12, 2025, "Z")."""
    normalized = symbol.strip().upper()
    match = _CONTRACT_RE.fullmatch(normalized)
    if not match:
        raise ValueError(f"Cannot parse contract symbol: {symbol}")
    root, month_code, year_code = match.groups()
    year_number = int(year_code)
    year = 2000 + year_number if year_number <= 69 else 1900 + year_number
    return root, _MONTH_CODE[month_code], year, month_code


def expiry_key(symbol: str) -> int:
    """Return a sortable year/month key for a contract symbol."""
    _, month, year, _ = parse_symbol(symbol)
    return year * 12 + month


def month_letters_to_nums(months: Iterable[str]) -> list[int]:
    """Convert futures month codes to unique month numbers."""
    values: list[int] = []
    for month in months:
        code = month.strip().upper()
        if code not in _MONTH_CODE:
            raise ValueError(f"Unknown futures month code: {month}")
        values.append(_MONTH_CODE[code])
    if not values or len(values) != len(set(values)):
        raise ValueError("cycle months must contain one or more unique month codes")
    return values


def step_symbol(symbol: str, steps: int, cycle_months: Iterable[int]) -> str:
    """Move a symbol forward by steps entries in its contract cycle."""
    if steps < 0:
        raise ValueError("steps must be >= 0")
    cycle = _validate_cycle(cycle_months)
    root, month, year, _ = parse_symbol(symbol)
    if month not in cycle:
        raise ValueError(f"Month {month} from {symbol} is not present in cycle_months")
    position = cycle.index(month)
    new_position = position + steps
    new_month = cycle[new_position % len(cycle)]
    year_bump = new_position // len(cycle)
    return f"{root}{_MONTH_CODE_INV[new_month]}{(year + year_bump) % 100:02d}"


def canonical_symbol(symbol: str) -> str:
    """Return a normalized contract symbol with a two-digit year."""
    root, _, year, month_code = parse_symbol(symbol)
    return f"{root}{month_code}{year % 100:02d}"


@dataclass(frozen=True)
class ContractCycle:
    """Validated contract-month rules for one futures root."""

    root: str
    months: tuple[int, ...]

    def __post_init__(self) -> None:
        normalized_root = self.root.strip().upper()
        if not normalized_root:
            raise ValueError("root must not be empty")
        object.__setattr__(self, "root", normalized_root)
        object.__setattr__(self, "months", tuple(_validate_cycle(self.months)))

    @classmethod
    def for_root(
        cls,
        root: str,
        months: str | Iterable[str] | None = None,
    ) -> "ContractCycle":
        normalized_root = root.strip().upper()
        if months is None:
            values = DEFAULT_ROOT_CYCLES.get(
                normalized_root,
                list(range(1, 13)),
            )
        else:
            if isinstance(months, str):
                months = re.split("[, ]+", months.strip())
            values = month_letters_to_nums(months)
        return cls(normalized_root, tuple(sorted(values)))

    def symbol_ladder(
        self,
        start: pd.Timestamp,
        end: pd.Timestamp,
    ) -> list[str]:
        if start > end:
            raise ValueError("start must be earlier than or equal to end")
        symbols: list[str] = []
        for date in pd.date_range(
            start.to_period("M").to_timestamp(),
            end.to_period("M").to_timestamp(),
            freq="MS",
        ):
            if date.month in self.months:
                symbols.append(
                    f"{self.root}{_MONTH_CODE_INV[date.month]}{date.year % 100:02d}"
                )
        return list(dict.fromkeys(symbols))

    def front_symbol_at(self, date: pd.Timestamp) -> str:
        for month in self.months:
            if month >= date.month:
                return f"{self.root}{_MONTH_CODE_INV[month]}{date.year % 100:02d}"
        return f"{self.root}{_MONTH_CODE_INV[self.months[0]]}{(date.year + 1) % 100:02d}"


@runtime_checkable
class BaseFetcher(Protocol):
    """Minimal fetcher contract required by ContinuousFuturesBuilder."""

    def fetch_one(
        self,
        symbol: str,
        start: str | None,
        end: str | None,
    ) -> pd.DataFrame:
        """Return a contract history containing a date-like column."""


class BarchartFetcher:
    """Adapt a Barchart history client to the builder fetcher protocol."""

    def __init__(
        self,
        client: Any,
        *,
        data: str = "daily",
        maxrecords: int = 640,
        order: str = "asc",
        out: str = "df",
        min_delay_seconds: float = 2.0,
        max_delay_seconds: float = 5.5,
        sleep: Callable[[float], None] = time.sleep,
        random_source: random.Random | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        if maxrecords < 1:
            raise ValueError("maxrecords must be >= 1")
        if min_delay_seconds < 0:
            raise ValueError("min_delay_seconds must be >= 0")
        if max_delay_seconds < min_delay_seconds:
            raise ValueError("max_delay_seconds must be >= min_delay_seconds")
        if out != "df":
            raise ValueError("BarchartFetcher requires out='df'")
        self.client = client
        self.data = data
        self.maxrecords = maxrecords
        self.order = order
        self.out = out
        self.min_delay_seconds = min_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.sleep = sleep
        self.random_source = random_source or random.Random()
        self.logger = logger or logging.getLogger(__name__)

    def fetch_one(
        self,
        symbol: str,
        start: str | None,
        end: str | None,
    ) -> pd.DataFrame:
        delay = self.random_source.uniform(
            self.min_delay_seconds,
            self.max_delay_seconds,
        )
        if delay > 0:
            self.sleep(delay)

        try:
            result = self.client.history(
                symbol=symbol,
                data=self.data,
                maxrecords=self.maxrecords,
                order=self.order,
                out=self.out,
                startDate=start,
                endDate=end,
            )
        except Exception as exc:
            raise FuturesDataError(
                f"Failed to fetch contract history for {symbol}: {exc}"
            ) from exc
        if not isinstance(result, pd.DataFrame):
            raise TypeError("BarchartFetcher requires the client's out='df' result")
        return result


@dataclass(frozen=True)
class Segment:
    """A contiguous run of one source contract in a nearby series."""

    segment_start: pd.Timestamp
    segment_end: pd.Timestamp
    line: int
    source_symbol: str
    n_rows: int

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class ContinuousFuturesBuilder:
    """Construct nearby futures series by expiry rank on each trade date.

    The builder accepts either a mapping of already-fetched contract histories
    or an iterable of symbols plus a BaseFetcher. It preserves the source
    contract in every output row and can return roll segments for auditability.
    """

    def __init__(
        self,
        fetcher: BaseFetcher | None = None,
        *,
        min_volume: int | None = None,
        drop_incomplete_days: bool = True,
        verbose: bool = True,
        logger: logging.Logger | None = None,
    ) -> None:
        if min_volume is not None and min_volume < 0:
            raise ValueError("min_volume must be >= 0 or None")
        self.fetcher = fetcher
        self.min_volume = min_volume
        self.drop_incomplete_days = drop_incomplete_days
        self.verbose = verbose
        self.logger = logger or logging.getLogger(__name__)

    def build_from_root(
        self,
        root: str,
        *,
        line_number: int,
        start: str,
        end: str,
        months: str | Iterable[str] | None = None,
        current_front: str | None = None,
        return_segments: bool = False,
    ) -> SeriesResult:
        """Infer a contract ladder for a root and build its nearby series."""
        if self.fetcher is None:
            raise ValueError("build_from_root requires a fetcher")
        if line_number < 1:
            raise ValueError("line_number must be >= 1")

        cycle = ContractCycle.for_root(root, months)
        start_dt = _to_naive_timestamp(start)
        end_dt = _to_naive_timestamp(end)
        if start_dt is None or end_dt is None:
            raise ValueError("start and end are required")
        if start_dt > end_dt:
            raise ValueError("start must be earlier than or equal to end")

        ladder = cycle.symbol_ladder(start_dt, end_dt)
        if current_front is None:
            front_at_end = cycle.front_symbol_at(end_dt)
        else:
            front_root, _, _, _ = parse_symbol(current_front)
            if front_root != cycle.root:
                raise ValueError(
                    f"current_front {current_front!r} root != {cycle.root!r}"
                )
            front_at_end = canonical_symbol(current_front)
            if parse_symbol(front_at_end)[1] not in cycle.months:
                raise ValueError(
                    f"current_front {current_front!r} is outside the contract cycle"
                )

        if not ladder:
            ladder = [front_at_end]

        pads = _steps_forward(ladder[-1], front_at_end, cycle.months)
        pads += max(line_number - 1, 0)
        current = ladder[-1]
        for _ in range(pads):
            current = step_symbol(current, 1, cycle.months)
            ladder.append(current)

        symbols = list(dict.fromkeys(ladder))
        return self.build(
            symbols,
            line_number=line_number,
            start=start,
            end=end,
            return_segments=return_segments,
        )

    def build(
        self,
        contracts_or_data: Iterable[str] | Mapping[str, pd.DataFrame],
        *,
        line_number: int,
        start: str | None = None,
        end: str | None = None,
        return_segments: bool = False,
    ) -> SeriesResult:
        """Build a nearby line from symbols or normalized contract data."""
        if line_number < 1:
            raise ValueError("line_number must be >= 1")
        start_dt = _to_naive_timestamp(start)
        end_dt = _to_naive_timestamp(end)
        if start_dt is not None and end_dt is not None and start_dt > end_dt:
            raise ValueError("start must be earlier than or equal to end")

        data = self._as_data_dict(contracts_or_data, start, end)
        if not data:
            return self._result_for_empty(return_segments)

        all_df = self._concat_with_expiry(data)
        if start_dt is not None:
            all_df = all_df[all_df["date"] >= start_dt]
        if end_dt is not None:
            all_df = all_df[all_df["date"] <= end_dt]
        if self.min_volume is not None and "volume" in all_df.columns:
            all_df = all_df[all_df["volume"] > self.min_volume]
        if all_df.empty:
            return self._result_for_empty(return_segments)

        all_df = (
            all_df.sort_values(["symbol", "date"], kind="stable")
            .drop_duplicates(subset=["symbol", "date"], keep="last")
            .sort_values(["date", "exp_key", "source_symbol"], kind="stable")
        )
        all_df["active_count"] = all_df.groupby("date")["source_symbol"].transform(
            "nunique"
        )
        all_df["rank"] = all_df.groupby("date").cumcount() + 1

        picked = all_df[all_df["rank"] == line_number].copy()
        if self.drop_incomplete_days:
            picked = picked[picked["active_count"] >= line_number]
        picked["line"] = line_number

        base_columns = ["date", "line", "source_symbol", "symbol"]
        price_columns = [column for column in _PRICE_COLUMNS if column in picked]
        metadata_columns = [
            column for column in _METADATA_COLUMNS if column in picked
        ]
        columns = [
            column
            for column in base_columns + price_columns + metadata_columns
            if column in picked
        ]
        series = picked[columns].sort_values("date").reset_index(drop=True)
        if not return_segments:
            return series
        return series, self._segments_from_series(series, line_number=line_number)

    def _as_data_dict(
        self,
        contracts_or_data: Iterable[str] | Mapping[str, pd.DataFrame],
        start: str | None,
        end: str | None,
    ) -> dict[str, pd.DataFrame]:
        if isinstance(contracts_or_data, Mapping):
            data: dict[str, pd.DataFrame] = {}
            for symbol, frame in contracts_or_data.items():
                canonical = canonical_symbol(symbol)
                normalized = self._normalize_contract_df(frame, canonical)
                if not normalized.empty:
                    data[canonical] = normalized
            return data

        if isinstance(contracts_or_data, (str, bytes)):
            raise TypeError("contracts_or_data must be symbols or a symbol-to-frame mapping")
        if self.fetcher is None:
            raise ValueError(
                "No fetcher provided. Pass a mapping, or initialize with a fetcher."
            )

        symbols = sorted(
            {canonical_symbol(symbol) for symbol in contracts_or_data},
            key=expiry_key,
        )
        output: dict[str, pd.DataFrame] = {}
        for symbol in symbols:
            self._log(f"Fetching {symbol}")
            try:
                frame = self.fetcher.fetch_one(symbol, start, end)
            except Exception as exc:
                if isinstance(exc, FuturesDataError):
                    raise
                raise FuturesDataError(
                    f"Failed to fetch contract history for {symbol}: {exc}"
                ) from exc
            if frame is None or frame.empty:
                self._log(f"Skipping {symbol}: empty response")
                continue
            normalized = self._normalize_contract_df(frame, symbol)
            if normalized.empty:
                self._log(f"Skipping {symbol}: no valid rows")
                continue
            output[symbol] = normalized
        return output

    def _log(self, message: str) -> None:
        if self.verbose:
            self.logger.info(message)

    @staticmethod
    def _normalize_contract_df(
        frame: pd.DataFrame,
        symbol: str,
    ) -> pd.DataFrame:
        if not isinstance(frame, pd.DataFrame):
            raise TypeError("contract data must be a pandas DataFrame")
        if frame.empty:
            return frame.copy()

        lower = {str(column).strip().lower(): column for column in frame.columns}
        date_column = next(
            (
                lower[name]
                for name in ("date", "tradedate", "timestamp")
                if name in lower
            ),
            None,
        )
        if date_column is None:
            raise KeyError(
                "No date-like column found "
                "(expected date, tradeDate, or timestamp)"
            )

        output = pd.DataFrame()
        output["date"] = _to_naive_datetime(frame[date_column])

        def source(*aliases: str) -> Any:
            for alias in aliases:
                if alias in lower:
                    return frame[lower[alias]]
            return None

        for target, aliases in {
            "open": ("open",),
            "high": ("high",),
            "low": ("low",),
            "close": ("close",),
            "settlement": ("settlement", "settle", "set", "sett"),
            "last": ("last",),
            "volume": ("volume", "vol"),
            "openInterest": ("openinterest", "open_interest", "open interest", "oi"),
        }.items():
            values = source(*aliases)
            if values is not None:
                output[target] = values

        if "close" not in output and "last" in output:
            output["close"] = output["last"]

        output["symbol"] = symbol
        output = output[output["date"].notna()]
        numeric_columns = [
            column for column in _PRICE_COLUMNS + _METADATA_COLUMNS if column in output
        ]
        for column in numeric_columns:
            output[column] = pd.to_numeric(output[column], errors="coerce")

        available_prices = [
            column for column in _PRICE_COLUMNS if column in output
        ]
        if not available_prices:
            raise ValueError(
                f"Contract {symbol} has no supported price columns "
                f"({_PRICE_COLUMNS})."
            )
        output = output.dropna(subset=available_prices, how="all")
        return (
            output.sort_values("date")
            .drop_duplicates(subset=["date"], keep="last")
            .reset_index(drop=True)
        )

    @staticmethod
    def _concat_with_expiry(data: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []
        for symbol, frame in data.items():
            enriched = frame.copy()
            enriched["source_symbol"] = symbol
            enriched["exp_key"] = expiry_key(symbol)
            frames.append(enriched)
        return pd.concat(frames, ignore_index=True)

    @staticmethod
    def _segments_from_series(
        series: pd.DataFrame,
        *,
        line_number: int,
    ) -> pd.DataFrame:
        segments: list[Segment] = []
        if not series.empty:
            changes = series["source_symbol"].ne(
                series["source_symbol"].shift()
            )
            starts = series.index[changes].tolist()
            bounds = starts[1:] + [len(series)]
            for start, end in zip(starts, bounds):
                chunk = series.iloc[start:end]
                segments.append(
                    Segment(
                        segment_start=chunk["date"].min().normalize(),
                        segment_end=chunk["date"].max().normalize(),
                        line=line_number,
                        source_symbol=chunk["source_symbol"].iat[0],
                        n_rows=len(chunk),
                    )
                )
        columns = [
            "segment_start",
            "segment_end",
            "line",
            "source_symbol",
            "n_rows",
        ]
        return pd.DataFrame(
            [segment.as_dict() for segment in segments],
            columns=columns,
        )

    @staticmethod
    def _empty_series_df() -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "date",
                "line",
                "source_symbol",
                "symbol",
                *(_PRICE_COLUMNS + _METADATA_COLUMNS),
            ]
        )

    def _result_for_empty(
        self,
        return_segments: bool,
    ) -> SeriesResult:
        empty = self._empty_series_df()
        return (empty, pd.DataFrame()) if return_segments else empty


def _validate_cycle(cycle_months: Iterable[int]) -> list[int]:
    values = [int(month) for month in cycle_months]
    if not values or len(values) != len(set(values)):
        raise ValueError("cycle months must contain one or more unique months")
    if any(month not in _MONTH_CODE_INV for month in values):
        raise ValueError("cycle months must contain months from 1 through 12")
    return values


def _to_naive_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, utc=True, errors="coerce").dt.tz_convert(None)


def _to_naive_timestamp(value: str | pd.Timestamp | None) -> pd.Timestamp | None:
    if value is None:
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp


def _steps_forward(
    from_symbol: str,
    to_symbol: str,
    cycle_months: Iterable[int],
) -> int:
    """Return non-negative cycle steps from one symbol to another."""
    if expiry_key(to_symbol) <= expiry_key(from_symbol):
        return 0
    current = canonical_symbol(from_symbol)
    target = canonical_symbol(to_symbol)
    for steps in range(49):
        if current == target:
            return steps
        current = step_symbol(current, 1, cycle_months)
    raise RuntimeError(
        f"Could not reach {to_symbol} from {from_symbol} with cycle {list(cycle_months)}"
    )
