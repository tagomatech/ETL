# ETL utilities

This repository contains small data-ingestion and transformation utilities.
The folders are intentionally independent so that a data source can be used
without installing an unrelated project.

## Barchart

The `Barchart` package contains:

- `BarchartHistoricalData`, a session-based client for Barchart historical
  time-series responses.
- `ContinuousFuturesBuilder`, which builds nearby futures series from explicit
  contract data or a fetcher.

Install the Barchart runtime dependencies with:

```powershell
python -m pip install -r Barchart/requirements.txt
```

Example:

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

The client uses Barchart's web-session cookie handshake and endpoint, so it is
subject to Barchart availability and any access terms that apply to your use
of the service. No credentials are stored by this package.

Run the Barchart tests with:

```powershell
python -m unittest discover -s Barchart/tests -v
```
