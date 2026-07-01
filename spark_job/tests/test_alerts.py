"""Unit tests for the alert derivation logic (pct_change, build_alert).

Pure functions, no Spark or live stream needed.

    python -m pytest -q            # from the /spark_job directory
"""

from datetime import datetime

from streaming_job import build_alert, pct_change

WSTART = datetime(2026, 7, 1, 14, 32)
WEND = datetime(2026, 7, 1, 14, 33)


def test_pct_change_positive():
    assert round(pct_change(100.0, 100.42), 2) == 0.42


def test_pct_change_negative():
    assert round(pct_change(100.0, 99.5), 4) == -0.5


def test_pct_change_zero_open_is_safe():
    assert pct_change(0.0, 10.0) == 0.0


def test_no_alert_below_threshold():
    # 0.2% move, threshold 0.3
    assert build_alert("BTC-USD", WSTART, WEND, 100.0, 100.2, 0.3) is None


def test_no_alert_exactly_at_threshold():
    # exactly 0.3% does not "exceed" 0.3
    assert build_alert("BTC-USD", WSTART, WEND, 100.0, 100.3, 0.3) is None


def test_alert_above_threshold_fields():
    alert = build_alert("BTC-USD", WSTART, WEND, 100.0, 100.42, 0.3)
    assert alert is not None
    assert alert["symbol"] == "BTC-USD"
    assert alert["ts"] == WEND
    assert alert["price"] == 100.42
    assert round(alert["pct_change"], 2) == 0.42


def test_alert_message_format():
    alert = build_alert("BTC-USD", WSTART, WEND, 100.0, 100.42, 0.3)
    assert alert["message"] == "BTC-USD moved +0.42% in the 14:32-14:33 window"


def test_alert_negative_move_message_and_sign():
    alert = build_alert("ETH-USD", WSTART, WEND, 100.0, 99.5, 0.3)
    assert alert is not None
    assert alert["message"] == "ETH-USD moved -0.50% in the 14:32-14:33 window"
