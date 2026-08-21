import unittest
from unittest.mock import Mock

import pandas as pd

from Barchart.barcharthistoricaldata import BarchartHistoricalData


class FakeResponse:
    def __init__(self, text, *, content_type="text/plain", status_code=200):
        self.text = text
        self.headers = {"content-type": content_type}
        self.status_code = status_code
        self.raise_for_status = Mock()


def client_for(response):
    client = BarchartHistoricalData(handshake=False)
    client.cookies.set("XSRF-TOKEN", "token%2Fvalue")
    client.get = Mock(return_value=response)
    return client


class BarchartHistoricalDataTests(unittest.TestCase):
    def test_history_decodes_eight_column_csv_and_builds_request(self):
        client = client_for(
            FakeResponse("KCZ25,2025-01-02,100,101,99,100.5,123,456\n")
        )

        frame = client.history(
            "KCZ25",
            startDate="2025-01-01",
            endDate="2025-01-31",
        )

        self.assertIsInstance(frame, pd.DataFrame)
        self.assertEqual(frame.loc[0, "symbol"], "KCZ25")
        self.assertEqual(frame.loc[0, "close"], 100.5)
        self.assertEqual(frame.loc[0, "openInterest"], 456)
        kwargs = client.get.call_args.kwargs
        self.assertNotIn("maxrecords", kwargs["params"])
        self.assertEqual(kwargs["headers"]["X-XSRF-TOKEN"], "token/value")

    def test_history_decodes_json_and_dict_output(self):
        client = client_for(
            FakeResponse(
                '[{"date":"2025-01-02","close":100.5}]',
                content_type="application/json; charset=utf-8",
            )
        )

        result = client.history("KCZ25", out="dict")

        self.assertEqual(result, [{"date": "2025-01-02", "close": 100.5}])

    def test_history_rejects_empty_and_error_responses(self):
        for body in ("", "Error: invalid symbol"):
            with self.subTest(body=body):
                client = client_for(FakeResponse(body))
                with self.assertRaises(ValueError):
                    client.history("KCZ25")

    def test_handshake_requires_xsrf_cookie(self):
        response = FakeResponse("ok")
        with unittest.mock.patch.object(
            BarchartHistoricalData,
            "get",
            return_value=response,
        ):
            with self.assertRaises(RuntimeError):
                BarchartHistoricalData()


if __name__ == "__main__":
    unittest.main()
