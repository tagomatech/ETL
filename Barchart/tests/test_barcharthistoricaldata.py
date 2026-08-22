import unittest
from unittest.mock import Mock

import pandas as pd

from Barchart.barcharthistoricaldata import BarchartClient, BarchartHistoricalData
from Barchart.exceptions import BarchartDecodeError


class FakeResponse:
    def __init__(self, text, *, content_type="text/plain", status_code=200):
        self.text = text
        self.headers = {"content-type": content_type}
        self.status_code = status_code
        self.raise_for_status = Mock()


def client_for(response):
    client = BarchartClient(handshake=False, max_retries=0)
    client.cookies.set("XSRF-TOKEN", "token%2Fvalue")
    client.get = Mock(return_value=response)
    return client


class BarchartHistoricalDataTests(unittest.TestCase):
    def test_legacy_name_is_compatible(self):
        self.assertIs(BarchartHistoricalData, BarchartClient)

    def test_history_decodes_eight_column_csv_and_builds_request(self):
        client = client_for(
            FakeResponse("KCZ25,2025-01-02,100,101,99,100.5,123,456" + chr(10))
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

    def test_history_decodes_seven_column_equity_csv(self):
        client = client_for(
            FakeResponse("AAPL,2025-01-02,100,101,99,100.5,123456" + chr(10))
        )

        frame = client.history("AAPL", start_date="2025-01-01", end_date="2025-01-31")

        self.assertEqual(frame.loc[0, "symbol"], "AAPL")
        self.assertEqual(frame.loc[0, "close"], 100.5)
        self.assertNotIn("openInterest", frame.columns)

    def test_snake_case_dates_and_nested_json_are_supported(self):
        client = client_for(
            FakeResponse(
                '{"data":[{"date":"2025-01-02","close":100.5}]}',
                content_type="application/json; charset=utf-8",
            )
        )

        result = client.history(
            "KCZ25",
            start_date="2025-01-01",
            end_date="2025-01-31",
            out="dict",
        )

        self.assertEqual(result, {"data": [{"date": "2025-01-02", "close": 100.5}]})

    def test_history_decodes_json_to_frame(self):
        client = client_for(
            FakeResponse(
                '[{"date":"2025-01-02","close":100.5}]',
                content_type="application/json; charset=utf-8",
            )
        )

        frame = client.history("KCZ25")
        self.assertEqual(frame.loc[0, "close"], 100.5)

    def test_history_clips_data_to_requested_date_window(self):
        client = client_for(
            FakeResponse(
                "KCZ25,2024-12-31,100,101,99,100,123,456"
                + chr(10)
                + "KCZ25,2025-01-02,101,102,100,101,123,456"
                + chr(10)
                + "KCZ25,2025-02-03,102,103,101,102,123,456"
                + chr(10)
            )
        )

        frame = client.history(
            "KCZ25",
            start_date="2025-01-01",
            end_date="2025-01-31",
        )

        self.assertEqual(
            frame["date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2025-01-02"],
        )

    def test_history_rejects_invalid_parameters(self):
        client = client_for(FakeResponse("unused"))
        for kwargs in ({"maxrecords": 0}, {"daystoexpiration": -1}):
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(ValueError):
                    client.history("KCZ25", **kwargs)

    def test_history_rejects_empty_and_error_responses(self):
        for body in ("", "Error: invalid symbol"):
            with self.subTest(body=body):
                client = client_for(FakeResponse(body))
                with self.assertRaises(BarchartDecodeError):
                    client.history("KCZ25")

    def test_handshake_requires_xsrf_cookie(self):
        response = FakeResponse("ok")
        with unittest.mock.patch.object(
            BarchartClient,
            "get",
            return_value=response,
        ):
            with self.assertRaises(RuntimeError):
                BarchartClient(max_retries=0)


if __name__ == "__main__":
    unittest.main()
