import unittest

import pandas as pd

from barchart_data import (
    BarchartAPIError,
    BarchartAuthenticationError,
    BarchartDataClient,
)


class FakeResponse:
    def __init__(self, payload, *, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.text = str(payload)

    def raise_for_status(self):
        if self.status_code >= 400:
            raise __import__("requests").HTTPError(response=self)

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.headers = {}
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response

    def close(self):
        return None


class BarchartDataClientTests(unittest.TestCase):
    def test_quote_returns_dataframe_and_adds_api_key(self):
        session = FakeSession(
            FakeResponse(
                {
                    "status": {"code": 200, "message": "Success"},
                    "results": [{"symbol": "AAPL", "lastPrice": 100}],
                }
            )
        )
        client = BarchartDataClient(api_key="secret", session=session)

        result = client.market.quote(["AAPL"])

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.loc[0, "symbol"], "AAPL")
        self.assertEqual(session.calls[0][1]["params"]["apikey"], "secret")
        self.assertTrue(session.calls[0][0].endswith("/getQuote.json"))

    def test_history_maps_dates_and_supports_raw_json(self):
        session = FakeSession(
            FakeResponse(
                {
                    "status": {"code": 200},
                    "results": [
                        {
                            "symbol": "ZCU26",
                            "date": "2026-08-21",
                            "close": 483.75,
                        }
                    ],
                }
            )
        )
        client = BarchartDataClient(api_key="secret", session=session)

        frame = client.market.history(
            "ZCU26",
            start_date="2026-08-01",
            end_date="2026-08-21",
        )
        payload = client.market.history("ZCU26", output="json")

        self.assertEqual(frame.loc[0, "close"], 483.75)
        self.assertEqual(payload["status"]["code"], 200)
        self.assertEqual(session.calls[0][1]["params"]["startDate"], "2026-08-01")

    def test_fundamentals_and_metadata_use_endpoint_names(self):
        session = FakeSession(
            FakeResponse({"status": {"code": 200}, "results": []})
        )
        client = BarchartDataClient(api_key="secret", session=session)

        client.fundamentals.balance_sheets("AAPL")
        client.metadata.futures_specifications(symbols=["ZC"])

        self.assertTrue(session.calls[0][0].endswith("/getBalanceSheets.json"))
        self.assertTrue(
            session.calls[1][0].endswith("/getFuturesSpecifications.json")
        )

    def test_missing_key_is_rejected_before_network(self):
        session = FakeSession(FakeResponse({}))
        client = BarchartDataClient(api_key=None, session=session)
        client.api_key = None

        with self.assertRaises(BarchartAuthenticationError):
            client.market.quote("AAPL")
        self.assertEqual(session.calls, [])

    def test_api_status_errors_include_endpoint(self):
        session = FakeSession(
            FakeResponse(
                {"status": {"code": 401, "message": "Invalid API key"}}
            )
        )
        client = BarchartDataClient(api_key="secret", session=session)

        with self.assertRaises(BarchartAPIError) as context:
            client.market.quote("AAPL")

        self.assertEqual(context.exception.endpoint, "getQuote")


if __name__ == "__main__":
    unittest.main()
