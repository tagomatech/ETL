# Barchart utilities

The Barchart folder is a small, dependency-light library for turning Barchart
historical responses into analysis-ready contract histories and auditable
continuous futures series.

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

## Install

~~~
python -m pip install -r Barchart/requirements.txt
~~~

For the rendered Corn futures demo:

~~~
python -m pip install -r Barchart/requirements-demo.txt
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

Open corn_futures_demo.ipynb in Jupyter or VS Code. It uses CME/CBOT Corn
futures (ZC) and demonstrates contract-level histories, first and second nearby
lines, roll segments, and nearby spreads. The notebook uses deterministic
offline data by default; set USE_LIVE_DATA = True for the optional live
Barchart path.

The client uses Barchart's web-session cookie handshake and endpoint, so it is
subject to Barchart availability and any access terms that apply to your use of
the service. No credentials are stored by this package.

## Tests

~~~
python -m unittest discover -s Barchart/tests -v
~~~

The CI workflow runs the same suite across supported Python versions.
