"""Modern Barchart market-data client.

The authenticated client targets Barchart OnDemand endpoint families. The
public-web history adapter is exported separately as a compatibility path and
does not represent the official API.
"""

from .client import BarchartDataClient, OnDemandClient
from .catalog import (
    AGRICULTURAL_CATALOG,
    CommodityRoot,
    agricultural_catalog,
    catalog_frame,
)
from .exceptions import (
    BarchartAPIError,
    BarchartAuthenticationError,
    BarchartDataError,
    BarchartDecodeError,
    BarchartPublicPageError,
    BarchartTransportError,
)
from .legacy import PublicWebHistoryClient
from .normalization import rebase_frame, rebase_many, rebase_to_base
from .public import BarchartPublicClient, PublicBarchartClient, PublicWebClient
from .resources import FundamentalResource, MarketResource, MetadataResource

__all__ = [
    "BarchartAPIError",
    "BarchartAuthenticationError",
    "BarchartDataClient",
    "BarchartDataError",
    "BarchartDecodeError",
    "BarchartPublicClient",
    "BarchartPublicPageError",
    "BarchartTransportError",
    "AGRICULTURAL_CATALOG",
    "CommodityRoot",
    "FundamentalResource",
    "MarketResource",
    "MetadataResource",
    "OnDemandClient",
    "PublicWebHistoryClient",
    "PublicBarchartClient",
    "PublicWebClient",
    "agricultural_catalog",
    "catalog_frame",
    "rebase_frame",
    "rebase_many",
    "rebase_to_base",
]

__version__ = "0.4.0"
