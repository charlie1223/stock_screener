"""
InstitutionalNotSellingScreener 測試
規格: 改用近 3 日三大法人合計買賣超「累計淨額」判斷，取代只看單日淨額 (單日雜訊過大)
"""
import pandas as pd
import pytest

from src.screeners.filters import InstitutionalNotSellingScreener


class StubFetcher:
    def __init__(self, inst_data, expected_days=None):
        self._inst_data = inst_data
        self.expected_days = expected_days
        self.received_days = None

    def get_institutional_investors(self, stock_id, days=1):
        self.received_days = days
        return self._inst_data


def make_row(stock_id="1101"):
    return pd.DataFrame([{"stock_id": stock_id}])


def test_queries_three_day_window_not_single_day():
    fetcher = StubFetcher({"total": {"today": -500, "sum_days": 200}, "foreign": {"today": -500}, "investment_trust": {"today": 0}})
    screener = InstitutionalNotSellingScreener(fetcher)

    screener.screen(make_row())

    assert fetcher.received_days == 3


def test_passes_when_3day_net_is_positive_despite_negative_today():
    # 今日單日賣超，但近3日累計仍是買超 -> 應通過 (排除單日雜訊誤判)
    fetcher = StubFetcher({
        "total": {"today": -500, "sum_days": 800},
        "foreign": {"today": -500}, "investment_trust": {"today": 0},
    })
    screener = InstitutionalNotSellingScreener(fetcher)

    result = screener.screen(make_row())

    assert len(result) == 1


def test_rejects_when_3day_net_is_negative():
    fetcher = StubFetcher({
        "total": {"today": 100, "sum_days": -300},
        "foreign": {"today": 100}, "investment_trust": {"today": 0},
    })
    screener = InstitutionalNotSellingScreener(fetcher)

    result = screener.screen(make_row())

    assert len(result) == 0


def test_keeps_when_data_unavailable():
    fetcher = StubFetcher({})
    screener = InstitutionalNotSellingScreener(fetcher)

    result = screener.screen(make_row())

    assert len(result) == 1
