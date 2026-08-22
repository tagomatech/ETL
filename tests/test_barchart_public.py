import json
import unittest

import pandas as pd

from barchart_data import (
    BarchartPublicClient,
    BarchartPublicPageError,
)


class FakeResponse:
    def __init__(self, text, *, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests

            raise requests.HTTPError(response=self)


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.headers = {}
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response


def page_html(payload):
    return (
        '<html><script id="barchart-www-inline-data" type="application/json">'
        + json.dumps(payload)
        + "</script></html>"
    )


class PublicBarchartClientTests(unittest.TestCase):
    def test_quote_decodes_public_page_without_api_key(self):
        session = FakeSession(
            FakeResponse(
                page_html(
                    {
                        "ZCU26": {
                            "instrument": {
                                "name": "Corn Futures",
                                "tickIncrement": 0.25,
                            },
                            "quote": {"close": 483.75, "volume": 12},
                        }
                    }
                )
            )
        )
        client = BarchartPublicClient(session=session)

        result = client.quote("ZCU26")

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.loc[0, "symbol"], "ZCU26")
        self.assertEqual(result.loc[0, "close"], 483.75)
        self.assertEqual(result.loc[0, "instrument_name"], "Corn Futures")
        self.assertNotIn("apikey", session.calls[0][1])
        self.assertEqual(
            session.calls[0][0],
            "https://www.barchart.com/futures/quotes/ZCU26/overview",
        )

    def test_profile_supports_multiple_symbols_and_json_output(self):
        session = FakeSession(
            FakeResponse(
                page_html(
                    {
                        "AAPL": {
                            "instrument": {"name": "Apple Inc."},
                            "quote": {"close": 100},
                        }
                    }
                )
            )
        )
        client = BarchartPublicClient(session=session)

        result = client.profile(["AAPL"], asset_class="stocks", output="json")

        self.assertEqual(result[0]["name"], "Apple Inc.")
        self.assertEqual(result[0]["asset_class"], "stocks")
        self.assertEqual(
            session.calls[0][0],
            "https://www.barchart.com/stocks/quotes/AAPL/overview",
        )

    def test_missing_inline_payload_is_typed_error(self):
        client = BarchartPublicClient(
            session=FakeSession(FakeResponse("<html>blocked</html>"))
        )

        with self.assertRaises(BarchartPublicPageError):
            client.page("ZCU26")

    def test_rejects_path_injection(self):
        client = BarchartPublicClient(session=FakeSession(FakeResponse("")))

        with self.assertRaises(ValueError):
            client.page("../secrets")
        with self.assertRaises(ValueError):
            client.page("ZCU26", asset_class="futures/../stocks")


if __name__ == "__main__":
    unittest.main()
