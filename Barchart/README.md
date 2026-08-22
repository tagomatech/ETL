# Barchart compatibility utilities

The Barchart folder contains the compatibility modules used by the
barchart-data project. It turns Barchart historical responses into
analysis-ready contract histories and auditable continuous futures series,
and contains a maintained catalog of major agricultural futures roots.

The canonical install now lives at the repository root:

~~~powershell
python -m pip install -e .
~~~

Install the notebook and Screamer extras with:

~~~powershell
python -m pip install -e '.[demo]'
~~~

## Design

The package is split into three replaceable layers:

- **Client**: BarchartClient handles the web-session handshake, retries,
  timeouts, HTTP errors, and CSV/JSON decoding. BarchartHistoricalData remains
  as a backwards-compatible alias.
- **Fetcher**: BaseFetcher is a small protocol. BarchartFetcher adapts the
  client to the builder and leaves rate limiting/configuration at the boundary.
- **Builder**: ContinuousFuturesBuilder handles contract cycles, date
  normalization, nearby selection, duplicate removal, and roll-segment output.

This makes it possible to derive another client or fetcher without changing the
continuous-series logic. A database-backed fetcher only needs a
fetch_one(symbol, start, end) -> pandas.DataFrame method.

## Compatibility install

Install the complete project from the repository root:

~~~
python -m pip install -e .
~~~

The legacy imports remain available:

~~~
from Barchart import BarchartClient
~~~

## Example

~~~
from Barchart import BarchartClient, BarchartFetcher, ContinuousFuturesBuilder

client = BarchartClient()
fetcher = BarchartFetcher(client)
builder = ContinuousFuturesBuilder(fetcher=fetcher, verbose=False)

series, rolls = builder.build_from_root(
    "ZC",
    line_number=1,
    start="2024-01-01",
    end="2024-12-31",
    return_segments=True,
)
~~~

The output keeps source_symbol on every row and returns roll ranges in rolls,
so downstream analytics can audit which contract supplied each value.

## Corn futures demo

Open corn_futures_demo.ipynb in Jupyter or VS Code. It focuses on the actual
CME/CBOT September 2026 Corn contract ZCU26. The notebook fetches its Barchart
OHLCV and open-interest history, draws candlesticks, and adds Screamer
Bollinger Bands, ATR, RSI, and rolling volume mean indicators. It contains no
synthetic data and does not stitch multiple contracts together.

Install requirements-demo.txt before running the notebook. Screamer requires
Python 3.11 or newer.

The client applies the requested inclusive start and end dates after decoding
the response, because the upstream endpoint can return rows outside the
requested window.

The client uses Barchart's web-session cookie handshake and endpoint, so it is
subject to Barchart availability and any access terms that apply to your use of
the service. No credentials are stored by this package.

## Agricultural catalog and relative comparison

commoditycatalog.py is the source of truth for the agriculture roots. Each
entry keeps the Barchart root, exchange symbol, venue, contract units, common
contract months, and notes together. It includes:

- CBOT/CME grains and oilseeds
- ICE Canada canola
- Euronext Matif milling wheat and rapeseed
- CME live cattle, feeder cattle, and lean hogs
- CME/Bursa-referenced palm oil plus MDEX palm kernel oil and palm olein

The catalog also documents the KM Pork Cutout and L8 Lean Beef Trim 90 roots,
but leaves them out of the default comparison because they are less standard
for a first cross-market index.

Use CommodityRoot.barchart_symbol() or barchart_nearby_symbol() to create the
current nearby shortcut. "*1" means front month, while Barchart's "*0"
shortcut is a liquidity-led lead month and is not the same as first nearby.
For example, "ZC*1" currently resolves to "ZCU26".

futuresnormalization.py provides rebase_to_base() and rebase_frame(). The
notebook uses them to start each current front contract at 100. This is an
indexed price-path comparison, not a currency-adjusted return series and not
a continuous roll.

The catalog roots and nearby behavior are grounded in the
[Barchart futures search](https://www.barchart.com/search),
[grain contract specifications](https://www.barchart.com/futures/contract-specifications/grains),
and [meat contract specifications](https://www.barchart.com/futures/contract-specifications/meats).
The CU entry is labeled carefully: it is Barchart's CME USD Malaysian crude
palm oil calendar contract, with Bursa Malaysia FCPO as the underlying
reference, rather than a direct Ringgit FCPO quote.

## Tests

~~~
python -m unittest discover -s Barchart/tests -v
~~~

The CI workflow runs the same suite across supported Python versions.
