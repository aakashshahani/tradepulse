"""Unit tests for producer.transform_match.

Run from the /producer directory (so `producer` is importable):

    python -m unittest discover -t . -s tests -p 'test_*.py'
"""

import unittest

from producer import transform_match

# A representative Coinbase `match` message (extra fields intentionally kept
# to prove they're ignored).
SAMPLE_MATCH = {
    "type": "match",
    "trade_id": 1047769595,
    "maker_order_id": "abc",
    "taker_order_id": "def",
    "side": "buy",
    "size": "0.01234567",
    "price": "58699.73",
    "product_id": "BTC-USD",
    "sequence": 123456789,
    "time": "2026-07-01T10:12:39.996920Z",
}


class TestTransformMatch(unittest.TestCase):
    def test_maps_all_fields(self):
        out = transform_match(SAMPLE_MATCH)
        self.assertEqual(
            out,
            {
                "symbol": "BTC-USD",
                "price": 58699.73,
                "size": 0.01234567,
                "side": "buy",
                "trade_id": 1047769595,
                "ts": "2026-07-01T10:12:39.996920Z",
            },
        )

    def test_price_and_size_are_floats(self):
        out = transform_match(SAMPLE_MATCH)
        self.assertIsInstance(out["price"], float)
        self.assertIsInstance(out["size"], float)

    def test_only_expected_keys(self):
        out = transform_match(SAMPLE_MATCH)
        self.assertEqual(
            set(out.keys()),
            {"symbol", "price", "size", "side", "trade_id", "ts"},
        )

    def test_tiny_size_scientific_notation(self):
        msg = {**SAMPLE_MATCH, "size": "8e-08"}
        self.assertEqual(transform_match(msg)["size"], 8e-08)

    def test_missing_field_raises_keyerror(self):
        msg = {k: v for k, v in SAMPLE_MATCH.items() if k != "price"}
        with self.assertRaises(KeyError):
            transform_match(msg)

    def test_non_numeric_price_raises_valueerror(self):
        msg = {**SAMPLE_MATCH, "price": "not-a-number"}
        with self.assertRaises(ValueError):
            transform_match(msg)


if __name__ == "__main__":
    unittest.main()
