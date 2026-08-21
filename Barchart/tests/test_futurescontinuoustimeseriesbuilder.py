import logging
import unittest
from unittest.mock import Mock

import pandas as pd

from Barchart.futurescontinuoustimeseriesbuilder import (
    DEFAULT_ROOT_CYCLES,
    BarchartFetcher,
    ContractCycle,
    ContinuousFuturesBuilder,
    canonical_symbol,
    month_letters_to_nums,
    parse_symbol,
    step_symbol,
)


def contract_frame(dates, values, *, volume=None, column="last"):
    data = {"date": dates, column: values}
    if volume is not None:
        data["volume"] = volume
    return pd.DataFrame(data)


class FuturesBuilderTests(unittest.TestCase):
    def test_symbol_helpers_normalize_and_step_contracts(self):
        self.assertEqual(parse_symbol(" kcz25 "), ("KC", 12, 2025, "Z"))
        self.assertEqual(canonical_symbol("kcz25"), "KCZ25")
        self.assertEqual(step_symbol("KCZ25", 1, [3, 5, 7, 9, 12]), "KCH26")
        self.assertEqual(month_letters_to_nums(["H", "K", "N"]), [3, 5, 7])
        self.assertEqual(DEFAULT_ROOT_CYCLES["ZC"], [3, 5, 7, 9, 12])
        self.assertEqual(ContractCycle.for_root("zc").months, (3, 5, 7, 9, 12))
        with self.assertRaises(ValueError):
            month_letters_to_nums(["H", "H"])

    def test_ladder_includes_a_contract_month_when_start_is_mid_month(self):
        ladder = ContractCycle.for_root("KC", ["K", "N"]).symbol_ladder(
            pd.Timestamp("2025-05-15"),
            pd.Timestamp("2025-07-15"),
        )
        self.assertEqual(ladder, ["KCK25", "KCN25"])

    def test_fetcher_can_be_used_without_sleeping(self):
        client = Mock()
        client.history.return_value = contract_frame(["2025-01-02"], [100])
        sleep = Mock()
        fetcher = BarchartFetcher(
            client,
            min_delay_seconds=0,
            max_delay_seconds=0,
            sleep=sleep,
        )

        result = fetcher.fetch_one("KCZ25", "2025-01-01", "2025-01-31")

        self.assertEqual(result.loc[0, "last"], 100)
        sleep.assert_not_called()
        client.history.assert_called_once_with(
            symbol="KCZ25",
            data="daily",
            maxrecords=640,
            order="asc",
            out="df",
            startDate="2025-01-01",
            endDate="2025-01-31",
        )

    def test_last_is_retained_and_used_as_close(self):
        builder = ContinuousFuturesBuilder(verbose=False)
        result = builder.build(
            {"kcz25": contract_frame(["2025-01-02"], [100])},
            line_number=1,
        )

        self.assertEqual(result.loc[0, "source_symbol"], "KCZ25")
        self.assertEqual(result.loc[0, "close"], 100)
        self.assertEqual(result.loc[0, "last"], 100)

    def test_nearby_line_selection_and_segments(self):
        dates = ["2025-01-02", "2025-01-03"]
        data = {
            "KCZ25": contract_frame(dates, [100, 101]),
            "KCH26": contract_frame(dates, [110, 111]),
        }

        result, segments = ContinuousFuturesBuilder(verbose=False).build(
            data,
            line_number=2,
            return_segments=True,
        )

        self.assertEqual(result["source_symbol"].tolist(), ["KCH26", "KCH26"])
        self.assertEqual(segments.loc[0, "source_symbol"], "KCH26")
        self.assertEqual(segments.loc[0, "n_rows"], 2)

    def test_incomplete_days_are_removed_for_nearby_lines(self):
        data = {
            "KCZ25": contract_frame(["2025-01-02", "2025-01-03"], [100, 101]),
            "KCH26": contract_frame(["2025-01-02"], [110]),
        }

        result = ContinuousFuturesBuilder(verbose=False).build(data, line_number=2)

        self.assertEqual(
            result["date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2025-01-02"],
        )

    def test_timezone_aware_bounds_are_accepted(self):
        data = {
            "KCZ25": contract_frame(
                ["2025-01-01T00:00:00Z", "2025-01-02T00:00:00Z"],
                [100, 101],
            )
        }

        result = ContinuousFuturesBuilder(verbose=False).build(
            data,
            line_number=1,
            start="2025-01-02T00:00:00+00:00",
        )

        self.assertEqual(len(result), 1)
        self.assertEqual(result.loc[0, "close"], 101)

    def test_builder_logs_instead_of_printing(self):
        logger = logging.getLogger("test-builder")
        builder = ContinuousFuturesBuilder(
            fetcher=Mock(),
            verbose=True,
            logger=logger,
        )
        builder.fetcher.fetch_one.return_value = contract_frame(
            ["2025-01-02"], [100]
        )
        with self.assertLogs(logger, level="INFO") as captured:
            builder.build(["KCZ25"], line_number=1)
        self.assertTrue(any("Fetching KCZ25" in message for message in captured.output))


if __name__ == "__main__":
    unittest.main()
