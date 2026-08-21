# Barchart utilities

The `Barchart` package contains:

- `BarchartHistoricalData`, a session-based client for Barchart historical
  time-series responses.
- `ContinuousFuturesBuilder`, which builds nearby futures series from explicit
  contract data or a fetcher.

## Install

Install the runtime dependencies with:

```powershell
python -m pip install -r Barchart/requirements.txt
```

For the rendered Corn futures demo, install the optional notebook dependencies:

```powershell
python -m pip install -r Barchart/requirements-demo.txt
```

## Example

```python
from Barchart import BarchartFetcher, BarchartHistoricalData, ContinuousFuturesBuilder

client = BarchartHistoricalData()
fetcher = BarchartFetcher(client)
builder = ContinuousFuturesBuilder(fetcher=fetcher, verbose=False)

series = builder.build_from_root(
    "KC",
    line_number=1,
    start="2024-01-01",
    end="2024-12-31",
)
```

## Corn futures demo

Open `corn_futures_demo.ipynb` in Jupyter or VS Code. It uses CME/CBOT Corn
futures (`ZC`) and demonstrates:

- contract-level histories with realistic availability windows;
- first and second nearby lines;
- roll segments and nearby spreads; and
- an optional live Barchart path controlled by `USE_LIVE_DATA`.

The notebook runs on deterministic offline data by default, so it does not
require credentials or a live network request to display the workflow.

The client uses Barchart's web-session cookie handshake and endpoint, so it is
subject to Barchart availability and any access terms that apply to your use
of the service. No credentials are stored by this package.

## Tests

Run the Barchart tests with:

```powershell
python -m unittest discover -s Barchart/tests -v
```
