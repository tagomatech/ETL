"""Small, explicit normalizations for comparing futures price paths."""

from __future__ import annotations

from collections.abc import Mapping

import pandas as pd


def rebase_to_base(values: pd.Series, *, base: float = 100.0) -> pd.Series:
    """Rebase a numeric series to base at its first valid observation.

    This is an index transformation, not a total-return calculation. It
    preserves missing values and the original index.
    """

    if base <= 0:
        raise ValueError("base must be > 0")
    numeric = pd.to_numeric(values, errors="coerce").astype(float)
    valid = numeric.dropna()
    if valid.empty:
        raise ValueError("values must contain at least one numeric observation")
    first = float(valid.iloc[0])
    if first == 0:
        raise ValueError("the first valid observation must not be zero")
    return numeric / first * base


def rebase_frame(
    frame: pd.DataFrame,
    *,
    value_column: str = "close",
    output_column: str = "index_100",
    base: float = 100.0,
) -> pd.DataFrame:
    """Copy a frame and add a rebased value column."""

    if value_column not in frame.columns:
        raise KeyError(f"Missing value column: {value_column}")
    output = frame.copy()
    output[output_column] = rebase_to_base(output[value_column], base=base)
    return output


def rebase_many(
    frames: Mapping[str, pd.DataFrame],
    *,
    date_column: str = "date",
    value_column: str = "close",
    name_column: str = "series",
    output_column: str = "index_100",
    base: float = 100.0,
) -> pd.DataFrame:
    """Return multiple rebased frames in long form for plotting or storage."""

    results: list[pd.DataFrame] = []
    for name, frame in frames.items():
        if date_column not in frame.columns:
            raise KeyError(f"Missing date column: {date_column}")
        normalized = rebase_frame(
            frame,
            value_column=value_column,
            output_column=output_column,
            base=base,
        )
        results.append(
            normalized[[date_column, output_column]].assign(**{name_column: name})
        )
    if not results:
        return pd.DataFrame(columns=[date_column, name_column, output_column])
    return pd.concat(results, ignore_index=True)[
        [date_column, name_column, output_column]
    ]
