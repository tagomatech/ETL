"""Curated agricultural futures roots and Barchart front-month shortcuts.

The catalog separates a Barchart root from an exchange symbol because the
identifiers are not always the same. Barchart's "*1" shortcut is used for
the current front month; "*0" is a liquidity-led lead-month shortcut and is
intentionally not used for first-nearby comparisons.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

import pandas as pd

CommodityCategory = Literal["grains", "oilseeds", "livestock", "vegetable_oils"]
_MONTH_CODES = frozenset("FGHJKMNQUVXZ")


@dataclass(frozen=True)
class CommodityRoot:
    """Metadata for one agricultural futures root listed by Barchart."""

    name: str
    root: str
    category: CommodityCategory
    venue: str
    exchange_symbol: str
    units: str
    contract_months: tuple[str, ...]
    notes: str = ""
    comparison_default: bool = True

    def __post_init__(self) -> None:
        root = self.root.strip().upper()
        exchange_symbol = self.exchange_symbol.strip().upper()
        if not root:
            raise ValueError("root must not be empty")
        if not exchange_symbol:
            raise ValueError("exchange_symbol must not be empty")
        if self.category not in {
            "grains",
            "oilseeds",
            "livestock",
            "vegetable_oils",
        }:
            raise ValueError(f"Unsupported commodity category: {self.category}")

        months = tuple(code.strip().upper() for code in self.contract_months)
        if not months or any(code not in _MONTH_CODES for code in months):
            raise ValueError("contract_months must contain valid futures month codes")
        if len(months) != len(set(months)):
            raise ValueError("contract_months must not contain duplicates")

        object.__setattr__(self, "root", root)
        object.__setattr__(self, "exchange_symbol", exchange_symbol)
        object.__setattr__(self, "contract_months", months)

    def barchart_symbol(self, rank: int = 1) -> str:
        """Return Barchart's current nearby shortcut for this root.

        rank=1 is the front month. Higher ranks are useful for explicitly
        requesting the second or third nearby contract.
        """

        return barchart_nearby_symbol(self.root, rank)

    def as_record(self) -> dict[str, object]:
        """Return a display-friendly row for documentation and notebooks."""

        record = asdict(self)
        record["contract_months"] = " ".join(self.contract_months)
        record["front_shortcut"] = self.barchart_symbol()
        return record


def barchart_nearby_symbol(root: str, rank: int = 1) -> str:
    """Build a Barchart nearby shortcut such as ZC*1."""

    normalized = root.strip().upper()
    if not normalized:
        raise ValueError("root must not be empty")
    if rank < 0:
        raise ValueError("rank must be >= 0")
    return f"{normalized}*{rank}"


# This is a practical major-agriculture catalog, not every agricultural quote
# on Barchart. Less liquid carcass products are documented but excluded from
# the default comparison so a first run remains readable.
AGRICULTURAL_CATALOG: tuple[CommodityRoot, ...] = (
    CommodityRoot("Corn", "ZC", "grains", "CBOT", "ZC", "5,000 bushels", ("H", "K", "N", "U", "Z")),
    CommodityRoot("Chicago SRW Wheat", "ZW", "grains", "CBOT", "ZW", "5,000 bushels", ("H", "K", "N", "U", "Z")),
    CommodityRoot("KC HRW Wheat", "KE", "grains", "CBOT", "KE", "5,000 bushels", ("H", "K", "N", "U", "Z")),
    CommodityRoot("Spring Wheat", "MW", "grains", "MIAX/MWE", "MW", "5,000 bushels", ("H", "K", "N", "U", "Z")),
    CommodityRoot("Hard Red Spring Wheat", "KW", "grains", "CME", "KW", "5,000 bushels", ("H", "K", "N", "U", "Z")),
    CommodityRoot("Oats", "ZO", "grains", "CBOT", "ZO", "5,000 bushels", ("H", "K", "N", "U", "Z")),
    CommodityRoot("Rough Rice", "ZR", "grains", "CBOT", "ZR", "2,000 hundredweight", ("F", "H", "K", "N", "U", "X")),
    CommodityRoot("Euronext Milling Wheat", "ML", "grains", "Euronext Matif", "EBM", "50 metric tonnes", ("H", "K", "U", "Z")),
    CommodityRoot("Soybeans", "ZS", "oilseeds", "CBOT", "ZS", "5,000 bushels", ("F", "H", "K", "N", "Q", "U", "X")),
    CommodityRoot("Soybean Meal", "ZM", "oilseeds", "CBOT", "ZM", "100 short tons", ("F", "H", "K", "N", "Q", "U", "V", "Z")),
    CommodityRoot("Soybean Oil", "ZL", "oilseeds", "CBOT", "ZL", "60,000 pounds", ("F", "H", "K", "N", "Q", "U", "V", "Z")),
    CommodityRoot("ICE Canola", "RS", "oilseeds", "ICE Canada", "RS", "20 metric tonnes", ("F", "H", "K", "N", "X")),
    CommodityRoot("Euronext Rapeseed", "XR", "oilseeds", "Euronext Matif", "XR", "50 metric tonnes", ("G", "K", "Q", "X")),
    CommodityRoot("Live Cattle", "LE", "livestock", "CME", "LE", "40,000 pounds", ("G", "J", "M", "Q", "V", "Z")),
    CommodityRoot("Feeder Cattle", "GF", "livestock", "CME", "GF", "50,000 pounds", ("F", "H", "J", "K", "N", "U", "V", "X")),
    CommodityRoot("Lean Hogs", "HE", "livestock", "CME", "HE", "40,000 pounds", ("G", "J", "K", "M", "N", "Q", "V", "Z")),
    CommodityRoot(
        "Pork Cutout",
        "KM",
        "livestock",
        "CME",
        "PRK",
        "40,000 pounds",
        ("G", "J", "K", "M", "N", "Q", "V", "Z"),
        notes="Listed livestock product; excluded from the default index comparison.",
        comparison_default=False,
    ),
    CommodityRoot(
        "Lean Beef Trim 90",
        "L8",
        "livestock",
        "CME",
        "BTN",
        "50,000 pounds",
        ("F", "H", "K", "N", "U", "X"),
        notes="Listed livestock product; excluded from the default index comparison.",
        comparison_default=False,
    ),
    CommodityRoot(
        "USD Malaysian Crude Palm Oil Calendar",
        "CU",
        "vegetable_oils",
        "CME; Bursa FCPO underlying",
        "CPO",
        "25 metric tonnes",
        ("F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"),
        notes="CME USD wrapper with Bursa Malaysia Derivatives FCPO as the underlying reference.",
    ),
    CommodityRoot(
        "Crude Palm Kernel Oil",
        "KP",
        "vegetable_oils",
        "MDEX",
        "FPKO",
        "25 metric tonnes",
        ("F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"),
    ),
    CommodityRoot(
        "RBD Palm Olein",
        "M5",
        "vegetable_oils",
        "MDEX",
        "FPOL",
        "25 metric tonnes",
        ("F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"),
    ),
)


def agricultural_catalog(*, comparison_only: bool = False) -> tuple[CommodityRoot, ...]:
    """Return the catalog, optionally limited to the default comparison set."""

    if comparison_only:
        return tuple(item for item in AGRICULTURAL_CATALOG if item.comparison_default)
    return AGRICULTURAL_CATALOG


def catalog_frame(
    catalog: tuple[CommodityRoot, ...] = AGRICULTURAL_CATALOG,
) -> pd.DataFrame:
    """Return catalog metadata as a stable, notebook-friendly DataFrame."""

    return pd.DataFrame([item.as_record() for item in catalog])
