"""Modern Barchart market-data client.

The authenticated client targets Barchart OnDemand endpoint families. The
public-web history adapter is exported separately as a compatibility path and
does not represent the official API.
"""

from .client import BarchartDataClient, OnDemandClient
from .exceptions import (
    BarchartAPIError,
    BarchartAuthenticationError,
    BarchartDataError,
    BarchartDecodeError,
    BarchartTransportError,
)
from .legacy import PublicWebHistoryClient
from .resources import FundamentalResource, MarketResource, MetadataResource

__all__ = [
    "BarchartAPIError",
    "BarchartAuthenticationError",
    "BarchartDataClient",
    "BarchartDataError",
    "BarchartDecodeError",
    "BarchartTransportError",
    "FundamentalResource",
    "MarketResource",
    "MetadataResource",
    "OnDemandClient",
    "PublicWebHistoryClient",
]

__version__ = "0.3.0"
