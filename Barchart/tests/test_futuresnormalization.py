import unittest

import pandas as pd

from Barchart.futuresnormalization import rebase_frame, rebase_many, rebase_to_base


class FuturesNormalizationTests(unittest.TestCase):
    def test_rebase_to_base_uses_first_valid_observation(self):
        values = pd.Series([None, 50.0, 55.0], index=["a", "b", "c"])

        result = rebase_to_base(values)

        self.assertTrue(pd.isna(result.loc["a"]))
        self.assertEqual(result.loc["b"], 100.0)
        self.assertAlmostEqual(result.loc["c"], 110.0)

    def test_rebase_frame_preserves_columns_and_adds_index(self):
        frame = pd.DataFrame({"date": ["2025-01-01", "2025-01-02"], "close": [10, 12]})

        result = rebase_frame(frame)

        self.assertEqual(result["close"].tolist(), [10, 12])
        self.assertEqual(result["index_100"].tolist(), [100.0, 120.0])

    def test_rebase_many_returns_long_form(self):
        frames = {
            "ZC": pd.DataFrame({"date": ["2025-01-01"], "close": [5]}),
            "ZS": pd.DataFrame({"date": ["2025-01-01"], "close": [10]}),
        }

        result = rebase_many(frames)

        self.assertEqual(result.columns.tolist(), ["date", "series", "index_100"])
        self.assertEqual(set(result["series"]), {"ZC", "ZS"})

    def test_invalid_rebase_inputs_are_rejected(self):
        with self.assertRaises(ValueError):
            rebase_to_base(pd.Series([0, 1]))
        with self.assertRaises(ValueError):
            rebase_to_base(pd.Series([1]), base=0)
        with self.assertRaises(ValueError):
            rebase_to_base(pd.Series([None]))


if __name__ == "__main__":
    unittest.main()
