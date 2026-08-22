# barchart-data and ETL utilities

This repository contains small data-ingestion and transformation utilities.
The Barchart component is now a root-level installable Python project named
barchart-data. The other folders remain independent ETL components.

## Components

- [barchart-data](Barchart/README.md): modern Barchart market and financial
  data client, historical futures utilities, and commodity metadata.

## Install

From a checkout:

~~~powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e .
~~~

For the notebook and Screamer demonstration dependencies:

~~~powershell
python -m pip install -e '.[demo]'
~~~

## Client direction

The primary client uses Barchart OnDemand and expects the BARCHART_API_KEY
environment variable or an explicit API key. It returns pandas DataFrames by
default and keeps raw JSON available for endpoint fields that do not yet have
a dedicated wrapper.

Example:

~~~
from barchart_data import BarchartDataClient, PublicBarchartClient

client = BarchartDataClient()
corn = client.market.history("ZC*1", frequency="daily")
quotes = client.market.quote(["ZC*1", "AAPL"])
quarterly_balance_sheet = client.fundamentals.balance_sheets(
    "AAPL",
    frequency="Quarter",
)
~~~

The public-page client is the no-login path:

~~~
public = PublicBarchartClient()
corn_quote = public.quote("ZCU26")
corn_profile = public.profile("ZCU26")
~~~

It reads the quote and instrument JSON embedded in public Barchart overview
pages. Public data can be delayed or limited, and the page format can change.
The authenticated OnDemand client is required for official API coverage such
as quarterly company balance sheets.

Use client.call("getEndpointName", params={...}) for an OnDemand endpoint
that does not yet have a dedicated resource method.

The existing Barchart folder remains as a compatibility layer during the
migration. Its public-web historical adapter is useful for the current
commodity demonstration, but it is not the official Barchart OnDemand API.
Do not redistribute Barchart data without the appropriate data rights.
