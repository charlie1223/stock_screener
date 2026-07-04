"""
QuietAccumulationScreener 測試
規格: 外資「且」投信皆須符合連續買超條件才算通過 (原本是「或」，任一法人買超就放行太寬鬆)
"""
import pandas as pd
import pytest

from src.screeners.filters import QuietAccumulationScreener


class StubTracker:
    """回傳固定法人分析結果的假 InstitutionalTracker"""

    def __init__(self, analysis):
        self._analysis = analysis

    def analyze_institutional_behavior(self, stock_id, days=20):
        return self._analysis


def make_screener(analysis):
    screener = QuietAccumulationScreener(data_fetcher=None)
    screener._tracker = StubTracker(analysis)
    return screener


def make_row(stock_id="1101"):
    return pd.DataFrame([{"stock_id": stock_id}])


def test_passes_when_both_foreign_and_trust_accumulating():
    analysis = {
        "foreign_consecutive_buy": 3, "foreign_stability": 1.0, "foreign_20d_sum": 500,
        "trust_consecutive_buy": 3, "trust_stability": 1.0, "trust_20d_sum": 200,
    }
    screener = make_screener(analysis)
    result = screener.screen(make_row())
    assert len(result) == 1


def test_rejects_when_only_foreign_accumulating():
    analysis = {
        "foreign_consecutive_buy": 5, "foreign_stability": 1.0, "foreign_20d_sum": 1000,
        "trust_consecutive_buy": 0, "trust_stability": 99, "trust_20d_sum": -50,
    }
    screener = make_screener(analysis)
    result = screener.screen(make_row())
    assert len(result) == 0


def test_rejects_when_only_trust_accumulating():
    analysis = {
        "foreign_consecutive_buy": 0, "foreign_stability": 99, "foreign_20d_sum": -100,
        "trust_consecutive_buy": 4, "trust_stability": 1.0, "trust_20d_sum": 300,
    }
    screener = make_screener(analysis)
    result = screener.screen(make_row())
    assert len(result) == 0


def test_rejects_when_analysis_unavailable():
    screener = make_screener(None)
    result = screener.screen(make_row())
    assert len(result) == 0
