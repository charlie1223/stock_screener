"""
VolumeShrinkScreener 測試
規格: 連續縮量天數 >= 門檻 且 當前量/均量 < 門檻比例 (兩條件皆須成立，非任一成立)
"""
import pandas as pd
import pytest

from src.screeners.filters import VolumeShrinkScreener


class StubFetcher:
    """回傳固定歷史資料的假 DataFetcher，避免呼叫外部 API"""

    def __init__(self, volumes):
        # volumes: list of 張 (仍會被 screener 除以 1000，因此這裡先乘 1000 存成股)
        self._hist = pd.DataFrame({
            "volume": [v * 1000 for v in volumes],
        })

    def get_historical_data(self, stock_id, days=25):
        return self._hist


def make_row(stock_id="1101", volume=100):
    return pd.DataFrame([{"stock_id": stock_id, "volume": volume}])


def test_passes_when_consecutive_shrink_and_low_volume():
    # 20 日均量 1000 張，最近 4 天持續遞減，當前量 100 張 (< 70% 均量)
    hist_volumes = [1000] * 16 + [400, 300, 200, 100]
    fetcher = StubFetcher(hist_volumes)
    screener = VolumeShrinkScreener(fetcher)

    df = make_row(volume=100)
    result = screener.screen(df)

    assert len(result) == 1


def test_rejects_when_volume_low_but_not_consecutively_shrinking():
    # 當前量遠低於均量 (符合 is_low_volume)，但前幾天量忽大忽小，並非連續縮量
    hist_volumes = [1000] * 16 + [200, 900, 150, 100]
    fetcher = StubFetcher(hist_volumes)
    screener = VolumeShrinkScreener(fetcher)

    df = make_row(volume=100)
    result = screener.screen(df)

    assert len(result) == 0


def test_rejects_when_consecutive_shrink_but_volume_not_low_enough():
    # 連續縮量天數足夠，但當前量仍高於均量門檻 (未真正量縮到位)
    hist_volumes = [500] * 16 + [1000, 950, 900, 850]
    fetcher = StubFetcher(hist_volumes)
    screener = VolumeShrinkScreener(fetcher)

    df = make_row(volume=850)
    result = screener.screen(df)

    assert len(result) == 0
