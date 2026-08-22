import unittest

import pandas as pd

from Barchart.commoditycatalog import (
    AGRICULTURAL_CATALOG,
    CommodityRoot,
    agricultural_catalog,
    barchart_nearby_symbol,
    catalog_frame,
)


class CommodityCatalogTests(unittest.TestCase):
    def test_catalog_has_unique_roots_and_expected_markets(self):
        roots = [item.root for item in AGRICULTURAL_CATALOG]

        self.assertEqual(len(roots), len(set(roots)))
        self.assertIn("ZC", roots)
        self.assertIn("ML", roots)
        self.assertIn("RS", roots)
        self.assertIn("CU", roots)
        self.assertIn("LE", roots)

    def test_front_shortcut_uses_barchart_rank(self):
        self.assertEqual(barchart_nearby_symbol("zc"), "ZC*1")
        self.assertEqual(barchart_nearby_symbol("zc", 0), "ZC*0")
        self.assertEqual(
            next(item for item in AGRICULTURAL_CATALOG if item.root == "ZC").barchart_symbol(),
            "ZC*1",
        )

    def test_catalog_frame_is_display_ready(self):
        frame = catalog_frame()

        self.assertIsInstance(frame, pd.DataFrame)
        self.assertIn("front_shortcut", frame.columns)
        self.assertEqual(frame.loc[frame["root"] == "ZC", "front_shortcut"].iat[0], "ZC*1")

    def test_comparison_catalog_excludes_secondary_products(self):
        roots = {item.root for item in agricultural_catalog(comparison_only=True)}

        self.assertIn("HE", roots)
        self.assertIn("M5", roots)
        self.assertNotIn("KM", roots)
        self.assertNotIn("L8", roots)

    def test_invalid_catalog_root_is_rejected(self):
        with self.assertRaises(ValueError):
            CommodityRoot(
                "Invalid",
                "",
                "grains",
                "test",
                "TEST",
                "units",
                ("H",),
            )


if __name__ == "__main__":
    unittest.main()
