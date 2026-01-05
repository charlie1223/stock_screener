"""
八大篩選步驟實作
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional

from .base import BaseScreener
from config.settings import SCREENING_PARAMS


class PriceChangeScreener(BaseScreener):
    """步驟1: 漲幅 3%-5% 篩選"""

    def __init__(self):
        super().__init__(name="漲幅 3%-5%", step_number=1)
        self.min_change = SCREENING_PARAMS["price_change_min"]
        self.max_change = SCREENING_PARAMS["price_change_max"]

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        mask = (df["change_pct"] >= self.min_change) & (df["change_pct"] <= self.max_change)
        return df[mask].reset_index(drop=True)


class VolumeRatioScreener(BaseScreener):
    """步驟2: 量比 > 1 篩選"""

    def __init__(self, data_fetcher):
        super().__init__(name="量比 > 1", step_number=2)
        self.min_ratio = SCREENING_PARAMS["volume_ratio_min"]
        self.data_fetcher = data_fetcher

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        # 計算當前時間佔全天交易時間比例
        now = datetime.now()
        market_minutes = 270  # 09:00-13:30 = 4.5 hours

        if now.hour < 9:
            elapsed_minutes = 0
        elif now.hour >= 13 and now.minute >= 30:
            elapsed_minutes = market_minutes
        else:
            elapsed_minutes = (now.hour - 9) * 60 + now.minute

        time_ratio = max(elapsed_minutes / market_minutes, 0.1)

        volume_ratios = []
        for idx, row in df.iterrows():
            stock_id = row["stock_id"]
            current_volume = row["volume"]

            # 獲取過去5日平均成交量
            hist_data = self.data_fetcher.get_historical_data(stock_id, days=5)
            if hist_data.empty:
                volume_ratios.append(np.nan)
                continue

            avg_volume = hist_data["volume"].mean() / 1000  # 股 -> 張
            if avg_volume > 0:
                expected_volume = avg_volume * time_ratio
                volume_ratios.append(current_volume / expected_volume)
            else:
                volume_ratios.append(np.nan)

        df["volume_ratio"] = volume_ratios
        mask = df["volume_ratio"] > self.min_ratio
        return df[mask].dropna(subset=["volume_ratio"]).reset_index(drop=True)


class TurnoverRateScreener(BaseScreener):
    """步驟3: 換手率 5%-10% 篩選"""

    def __init__(self, data_fetcher):
        super().__init__(name="換手率 5%-10%", step_number=3)
        self.min_rate = SCREENING_PARAMS["turnover_rate_min"]
        self.max_rate = SCREENING_PARAMS["turnover_rate_max"]
        self.data_fetcher = data_fetcher
        self._shares_data = None

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        # 獲取流通股數資料 (只獲取一次)
        if self._shares_data is None:
            self._shares_data = self.data_fetcher.get_shares_outstanding()

        turnover_rates = []
        for idx, row in df.iterrows():
            stock_id = row["stock_id"]
            volume = row["volume"] * 1000  # 張 -> 股

            # 從快取找流通股數
            if not self._shares_data.empty:
                shares_row = self._shares_data[self._shares_data["stock_id"] == stock_id]
                if not shares_row.empty:
                    shares = shares_row.iloc[0]["NumberOfSharesIssued"]
                    if shares and shares > 0:
                        turnover_rate = (volume / shares) * 100
                        turnover_rates.append(turnover_rate)
                        continue

            # 備用: 用歷史成交量估算 (假設日均換手率約1%)
            hist_data = self.data_fetcher.get_historical_data(stock_id, days=20)
            if not hist_data.empty:
                avg_volume = hist_data["volume"].mean()
                if avg_volume > 0:
                    # 相對換手率
                    turnover_rate = (volume / avg_volume) * 1.0  # 假設平均換手率1%
                    turnover_rates.append(min(turnover_rate, 20))  # 上限20%
                    continue

            turnover_rates.append(np.nan)

        df["turnover_rate"] = turnover_rates

        mask = (df["turnover_rate"] >= self.min_rate) & (df["turnover_rate"] <= self.max_rate)
        return df[mask].dropna(subset=["turnover_rate"]).reset_index(drop=True)


class MarketCapScreener(BaseScreener):
    """步驟4: 市值 50億-200億 篩選"""

    def __init__(self, data_fetcher):
        super().__init__(name="市值 50-200億", step_number=4)
        self.min_cap = SCREENING_PARAMS["market_cap_min"]
        self.max_cap = SCREENING_PARAMS["market_cap_max"]
        self.data_fetcher = data_fetcher
        self._market_cap_data = None

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        # 獲取市值資料 (只獲取一次)
        if self._market_cap_data is None:
            self._market_cap_data = self.data_fetcher.get_market_cap_data()

        if self._market_cap_data.empty:
            # 無法獲取市值資料，跳過此篩選
            df["market_cap"] = np.nan
            return df

        # 合併市值資料
        df = df.merge(self._market_cap_data, on="stock_id", how="left")

        mask = (df["market_cap"] >= self.min_cap) & (df["market_cap"] <= self.max_cap)
        return df[mask].dropna(subset=["market_cap"]).reset_index(drop=True)


class VolumeTrendScreener(BaseScreener):
    """步驟5: 成交量持續放大篩選"""

    def __init__(self, data_fetcher):
        super().__init__(name="成交量放大", step_number=5)
        self.days = SCREENING_PARAMS["volume_increase_days"]
        self.data_fetcher = data_fetcher

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        volume_increasing = []
        for idx, row in df.iterrows():
            stock_id = row["stock_id"]

            hist_data = self.data_fetcher.get_historical_data(stock_id, days=self.days + 2)
            if hist_data.empty or len(hist_data) < self.days:
                volume_increasing.append(False)
                continue

            # 取最近 N 日成交量
            recent_volumes = hist_data["volume"].tail(self.days).values

            # 檢查是否持續放大 (每日成交量 > 前一日)
            is_increasing = all(
                recent_volumes[i] > recent_volumes[i-1] * 0.95  # 允許5%誤差
                for i in range(1, len(recent_volumes))
            )
            volume_increasing.append(is_increasing)

        df["volume_increasing"] = volume_increasing
        return df[df["volume_increasing"]].reset_index(drop=True)


class MovingAverageScreener(BaseScreener):
    """步驟6: 均線多頭排列篩選"""

    def __init__(self, data_fetcher):
        super().__init__(name="均線多頭排列", step_number=6)
        self.short_periods = SCREENING_PARAMS["short_ma_periods"]
        self.long_period = SCREENING_PARAMS["long_ma_period"]
        self.data_fetcher = data_fetcher

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        ma_valid = []
        for idx, row in df.iterrows():
            stock_id = row["stock_id"]
            current_price = row["price"]

            hist_data = self.data_fetcher.get_historical_data(stock_id, days=self.long_period + 10)
            if hist_data.empty or len(hist_data) < self.long_period:
                ma_valid.append(False)
                continue

            closes = hist_data["close"]

            # 計算各均線
            ma5 = closes.rolling(5).mean().iloc[-1]
            ma10 = closes.rolling(10).mean().iloc[-1]
            ma20 = closes.rolling(20).mean().iloc[-1]
            ma60 = closes.rolling(60).mean().iloc[-1]

            # 計算 60 日線斜率 (近5日 vs 10日前)
            ma60_series = closes.rolling(60).mean()
            if len(ma60_series) >= 10:
                ma60_recent = ma60_series.iloc[-5:].mean()
                ma60_before = ma60_series.iloc[-15:-10].mean() if len(ma60_series) >= 15 else ma60_series.iloc[-10:-5].mean()
                ma60_slope_up = ma60_recent > ma60_before
            else:
                ma60_slope_up = False

            # 多頭排列: 價格 > MA5 > MA10 > MA20 > MA60
            try:
                bullish = current_price > ma5 > ma10 > ma20 > ma60
                is_valid = bullish and ma60_slope_up
            except:
                is_valid = False

            ma_valid.append(is_valid)

        df["ma_bullish"] = ma_valid
        return df[df["ma_bullish"]].reset_index(drop=True)


class RelativeStrengthScreener(BaseScreener):
    """步驟7: 強於大盤篩選"""

    def __init__(self, data_fetcher):
        super().__init__(name="強於大盤", step_number=7)
        self.data_fetcher = data_fetcher
        self._benchmark_change = None

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        # 獲取大盤漲跌幅
        if self._benchmark_change is None:
            self._benchmark_change = self.data_fetcher.get_benchmark_change()

        if self._benchmark_change is None or self._benchmark_change == 0:
            # 無法獲取大盤數據，假設大盤漲幅為0
            self._benchmark_change = 0
            df["relative_strength"] = df["change_pct"]
            mask = df["change_pct"] > 0
        else:
            df["relative_strength"] = df["change_pct"] / abs(self._benchmark_change)
            mask = df["change_pct"] > self._benchmark_change

        return df[mask].reset_index(drop=True)


class IntradayHighScreener(BaseScreener):
    """步驟8: 尾盤創新高篩選"""

    def __init__(self):
        super().__init__(name="尾盤創新高", step_number=8)
        self.threshold = SCREENING_PARAMS["intraday_high_threshold"]

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        # 條件1: 現價接近當日最高價
        near_high = df["price"] >= df["high"] * self.threshold

        # 條件2: 現價高於開盤價
        above_open = df["price"] > df["open"]

        # 綜合條件
        mask = near_high & above_open
        df["intraday_strong"] = mask

        return df[mask].reset_index(drop=True)
