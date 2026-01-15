"""
八大篩選步驟實作
"""
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Optional, Dict

from .base import BaseScreener
from config.settings import SCREENING_PARAMS


class PriceChangeScreener(BaseScreener):
    """步驟1: 漲幅 >= 3% 篩選"""

    def __init__(self):
        super().__init__(name="漲幅 >= 3%", step_number=1)
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
        elif now.hour > 13 or (now.hour == 13 and now.minute >= 30):
            # 收盤後，使用全天交易時間
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
    """步驟5: 換手率篩選"""

    def __init__(self, data_fetcher):
        min_rate = SCREENING_PARAMS.get("turnover_rate_min", 0.5)
        max_rate = SCREENING_PARAMS.get("turnover_rate_max", 20.0)
        super().__init__(name=f"換手率 {min_rate}%-{max_rate}%", step_number=5)
        self.min_rate = min_rate
        self.max_rate = max_rate
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
    """步驟1: 市值篩選"""

    def __init__(self, data_fetcher):
        min_cap = SCREENING_PARAMS["market_cap_min"]
        super().__init__(name=f"市值 >= {min_cap}億", step_number=1)
        self.min_cap = min_cap
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

        if not self._market_cap_data.empty:
            # 有市值資料，使用市值篩選
            df = df.merge(self._market_cap_data, on="stock_id", how="left")
            mask = (df["market_cap"] >= self.min_cap) & (df["market_cap"] <= self.max_cap)
            return df[mask].dropna(subset=["market_cap"]).reset_index(drop=True)

        # 備援：使用成交金額估算（排除小型股）
        # 市值 50 億的股票，假設換手率 1%，日成交金額約 5000 萬
        # 用成交金額 > 1000 萬作為門檻（保守估計）
        if "volume" in df.columns and "price" in df.columns:
            # 成交金額（萬元）= 成交量（張）* 價格 * 1000 / 10000
            df["trade_value"] = df["volume"] * df["price"] * 0.1  # 萬元
            min_trade_value = self.min_cap * 0.1  # 市值50億 -> 成交額500萬
            mask = df["trade_value"] >= min_trade_value
            df["market_cap"] = np.nan  # 標記沒有真實市值資料
            return df[mask].reset_index(drop=True)

        # 無法篩選，返回原資料
        df["market_cap"] = np.nan
        return df


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


class MASupportScreener(BaseScreener):
    """步驟4: 均線支撐篩選 - 守住長期均線且斜率向上"""

    def __init__(self, data_fetcher):
        super().__init__(name="均線支撐", step_number=4)
        self.data_fetcher = data_fetcher
        self.support_ma_periods = SCREENING_PARAMS.get("ma_support_periods", [20, 60])
        self.tolerance = SCREENING_PARAMS.get("ma_support_tolerance", 0.02)
        self.slope_lookback = SCREENING_PARAMS.get("ma_slope_lookback_days", 5)

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        ma_support_valid = []
        support_ma_info = []
        support_distance_list = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]
            current_price = row["price"]
            low_price = row["low"]

            hist_data = self.data_fetcher.get_historical_data(stock_id, days=70)
            if hist_data.empty or len(hist_data) < 60:
                ma_support_valid.append(False)
                support_ma_info.append("")
                support_distance_list.append(np.nan)
                continue

            closes = hist_data["close"]

            # 計算各均線及斜率
            ma_values = {}
            ma_slopes = {}
            for period in self.support_ma_periods:
                if len(closes) >= period:
                    ma_series = closes.rolling(period).mean()
                    ma_values[period] = ma_series.iloc[-1]
                    # 計算斜率 (最近值 vs N日前)
                    if len(ma_series) >= self.slope_lookback:
                        ma_slopes[period] = ma_series.iloc[-1] > ma_series.iloc[-self.slope_lookback]
                    else:
                        ma_slopes[period] = False

            # 檢查是否守住均線支撐
            support_found = False
            support_info = ""
            support_distance = np.nan

            for period in self.support_ma_periods:
                if period in ma_values:
                    ma_val = ma_values[period]
                    slope_up = ma_slopes.get(period, False)

                    # 條件1: 最低價在均線上方 (允許跌破 tolerance)
                    above_support = low_price >= ma_val * (1 - self.tolerance)

                    # 條件2: 均線斜率向上
                    if above_support and slope_up:
                        support_found = True
                        support_distance = ((current_price - ma_val) / ma_val) * 100
                        support_info = f"MA{period}支撐 距離{support_distance:.1f}%"
                        break

            ma_support_valid.append(support_found)
            support_ma_info.append(support_info)
            support_distance_list.append(support_distance)

        df["ma_support"] = ma_support_valid
        df["support_info"] = support_ma_info
        df["support_distance"] = support_distance_list

        return df[df["ma_support"]].reset_index(drop=True)


class BullishPatternScreener(BaseScreener):
    """步驟10: 多方型態篩選 - 只留型態是多方的標的"""

    def __init__(self, data_fetcher):
        super().__init__(name="多方型態", step_number=10)
        self.data_fetcher = data_fetcher

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        bullish_pattern = []
        pattern_info = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]
            current_price = row["price"]

            hist_data = self.data_fetcher.get_historical_data(stock_id, days=60)
            if hist_data.empty or len(hist_data) < 20:
                bullish_pattern.append(False)
                pattern_info.append("")
                continue

            closes = hist_data["close"]
            highs = hist_data["high"]
            lows = hist_data["low"]

            # 計算均線
            ma5 = closes.rolling(5).mean()
            ma10 = closes.rolling(10).mean()
            ma20 = closes.rolling(20).mean()

            # 多方型態判斷條件
            conditions_met = []

            # 1. 短期均線向上
            if len(ma5) >= 5:
                ma5_slope = ma5.iloc[-1] > ma5.iloc[-5]
                if ma5_slope:
                    conditions_met.append("MA5向上")

            # 2. 價格在 MA20 上方
            if len(ma20) >= 1 and not pd.isna(ma20.iloc[-1]):
                if current_price > ma20.iloc[-1]:
                    conditions_met.append("站上MA20")

            # 3. 近期創新高 (20日內)
            if len(highs) >= 20:
                recent_high = highs.tail(20).max()
                if current_price >= recent_high * 0.97:  # 接近20日高點
                    conditions_met.append("近20日高")

            # 4. 底部墊高 (近期低點高於更早的低點)
            if len(lows) >= 20:
                recent_low = lows.tail(10).min()
                earlier_low = lows.tail(20).head(10).min()
                if recent_low > earlier_low:
                    conditions_met.append("底部墊高")

            # 5. 均線多頭排列
            if len(ma5) >= 1 and len(ma10) >= 1 and len(ma20) >= 1:
                try:
                    if ma5.iloc[-1] > ma10.iloc[-1] > ma20.iloc[-1]:
                        conditions_met.append("均線多頭")
                except:
                    pass

            # 判斷: 至少符合 3 個條件才算多方型態
            is_bullish = len(conditions_met) >= 3
            bullish_pattern.append(is_bullish)
            pattern_info.append(" | ".join(conditions_met) if conditions_met else "")

        df["bullish_pattern"] = bullish_pattern
        df["pattern_info"] = pattern_info

        return df[df["bullish_pattern"]].reset_index(drop=True)


class InstitutionalHoldingScreener(BaseScreener):
    """法人持股/散戶比例篩選 - 排除散戶過高的股票"""

    def __init__(self, data_fetcher):
        super().__init__(name="法人持股篩選", step_number=9)
        self.data_fetcher = data_fetcher
        self.min_institutional = SCREENING_PARAMS.get("min_institutional_holding", 30)
        self.max_retail = SCREENING_PARAMS.get("max_retail_holding", 50)

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        holding_info = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]

            # 獲取股權分散表資料
            holding_data = self.data_fetcher.get_shareholding_distribution(stock_id)

            if not holding_data:
                # 無資料時保留股票，但標記
                valid_stocks.append(True)
                holding_info.append("資料不足")
                continue

            institutional_pct = holding_data.get("institutional_pct", 0)
            retail_pct = holding_data.get("retail_pct", 100)

            # 條件: 法人持股 >= 門檻 且 散戶持股 <= 門檻
            is_valid = institutional_pct >= self.min_institutional and retail_pct <= self.max_retail
            valid_stocks.append(is_valid)
            holding_info.append(f"法人{institutional_pct:.0f}%/散戶{retail_pct:.0f}%")

        df["holding_valid"] = valid_stocks
        df["holding_info"] = holding_info

        return df[df["holding_valid"]].reset_index(drop=True)


class FundamentalScreener(BaseScreener):
    """基本面篩選 - EPS、營收成長"""

    def __init__(self, data_fetcher):
        super().__init__(name="基本面篩選", step_number=10)
        self.data_fetcher = data_fetcher
        self.min_eps = SCREENING_PARAMS.get("min_eps", 0)
        self.min_revenue_growth = SCREENING_PARAMS.get("min_revenue_growth", -10)

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        fundamental_info = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]

            # 獲取基本面資料
            fundamental_data = self.data_fetcher.get_fundamental_data(stock_id)

            if not fundamental_data:
                # 無資料時保留股票
                valid_stocks.append(True)
                fundamental_info.append("資料不足")
                continue

            eps = fundamental_data.get("eps", 0)
            revenue_growth = fundamental_data.get("revenue_growth", 0)

            # 條件: EPS > 0 (獲利) 且 營收成長 > 門檻
            is_valid = eps >= self.min_eps and revenue_growth >= self.min_revenue_growth
            valid_stocks.append(is_valid)
            fundamental_info.append(f"EPS:{eps:.2f}/營收YoY:{revenue_growth:.1f}%")

        df["fundamental_valid"] = valid_stocks
        df["fundamental_info"] = fundamental_info

        return df[df["fundamental_valid"]].reset_index(drop=True)


class InstitutionalBuyScreener(BaseScreener):
    """法人連續買超篩選"""

    def __init__(self, data_fetcher):
        super().__init__(name="法人買超", step_number=8)
        self.data_fetcher = data_fetcher
        self.buy_days = SCREENING_PARAMS.get("institutional_buy_days", 5)

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        buy_info = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]

            # 獲取法人買賣超資料
            inst_data = self.data_fetcher.get_institutional_investors(stock_id, days=self.buy_days)

            if not inst_data:
                valid_stocks.append(True)  # 無資料時保留
                buy_info.append("資料不足")
                continue

            # 外資 + 投信 合計買超
            foreign_sum = inst_data.get("foreign", {}).get("sum_days", 0)
            trust_sum = inst_data.get("investment_trust", {}).get("sum_days", 0)
            total_sum = foreign_sum + trust_sum

            # 條件: 外資+投信 近N日合計買超 > 0
            is_valid = total_sum > 0
            valid_stocks.append(is_valid)

            # 格式化顯示
            def fmt(x):
                return f"+{x:,}" if x > 0 else f"{x:,}"
            buy_info.append(f"外資{fmt(foreign_sum)}/投信{fmt(trust_sum)}")

        df["inst_buy_valid"] = valid_stocks
        df["inst_buy_info"] = buy_info

        return df[df["inst_buy_valid"]].reset_index(drop=True)


class ForeignConsecutiveBuyScreener(BaseScreener):
    """外資連續買超篩選 - 偵測外資連續 N 日買超訊號"""

    def __init__(self, data_fetcher, min_consecutive_days: int = None):
        super().__init__(name="外資連續買超", step_number=11)
        self.data_fetcher = data_fetcher
        self.min_consecutive_days = min_consecutive_days or SCREENING_PARAMS.get("foreign_consecutive_buy_days", 3)

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        consecutive_info = []
        consecutive_days_list = []
        total_buy_list = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]

            # 獲取外資連續買超資料
            foreign_data = self.data_fetcher.get_foreign_consecutive_buy(stock_id, days=10)

            consecutive_days = foreign_data.get("consecutive_buy_days", 0)
            total_buy = foreign_data.get("total_buy_amount", 0)
            is_consecutive = consecutive_days >= self.min_consecutive_days

            valid_stocks.append(is_consecutive)
            consecutive_days_list.append(consecutive_days)
            total_buy_list.append(total_buy)

            if is_consecutive:
                consecutive_info.append(f"連{consecutive_days}日買超 +{total_buy:,}張")
            else:
                consecutive_info.append(f"連{consecutive_days}日" if consecutive_days > 0 else "無連續買超")

        df["foreign_consecutive_valid"] = valid_stocks
        df["foreign_consecutive_info"] = consecutive_info
        df["foreign_consecutive_days"] = consecutive_days_list
        df["foreign_total_buy"] = total_buy_list

        return df[df["foreign_consecutive_valid"]].reset_index(drop=True)


class BelowForeignCostScreener(BaseScreener):
    """低於外資成本篩選 - 找出現價低於外資平均成本的「打折股」"""

    def __init__(self, data_fetcher, max_premium_pct: float = None, cost_days: int = None):
        """
        Args:
            data_fetcher: 資料獲取器
            max_premium_pct: 最大允許溢價幅度 (%)，預設 5%
                            例: 5 表示現價最多比外資成本高 5%
            cost_days: 計算成本的天數範圍
        """
        super().__init__(name="外資成本折價", step_number=12)
        self.data_fetcher = data_fetcher
        self.max_premium_pct = max_premium_pct if max_premium_pct is not None else SCREENING_PARAMS.get("foreign_cost_max_premium", 5.0)
        self.cost_days = cost_days or SCREENING_PARAMS.get("foreign_cost_calculation_days", 60)

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        cost_info = []
        avg_cost_list = []
        discount_pct_list = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]
            current_price = row["price"]

            # 獲取外資平均成本
            cost_data = self.data_fetcher.get_foreign_average_cost(stock_id, days=self.cost_days)

            if not cost_data or "avg_cost" not in cost_data:
                # 無法計算成本時保留股票，但不計算折價
                valid_stocks.append(True)
                cost_info.append("成本資料不足")
                avg_cost_list.append(np.nan)
                discount_pct_list.append(np.nan)
                continue

            avg_cost = cost_data["avg_cost"]

            # 計算現價相對成本的溢價/折價幅度
            # 負數 = 折價 (現價低於成本)，正數 = 溢價
            premium_pct = ((current_price - avg_cost) / avg_cost) * 100

            # 條件: 現價低於成本 (折價) 或 溢價不超過門檻
            is_valid = premium_pct <= self.max_premium_pct

            valid_stocks.append(is_valid)
            avg_cost_list.append(avg_cost)
            discount_pct_list.append(-premium_pct)  # 轉為折價幅度 (正數=打折)

            if premium_pct < 0:
                cost_info.append(f"成本{avg_cost:.1f} 折價{-premium_pct:.1f}%")
            elif premium_pct <= self.max_premium_pct:
                cost_info.append(f"成本{avg_cost:.1f} 溢價{premium_pct:.1f}%")
            else:
                cost_info.append(f"成本{avg_cost:.1f} 溢價過高{premium_pct:.1f}%")

        df["foreign_cost_valid"] = valid_stocks
        df["foreign_cost_info"] = cost_info
        df["foreign_avg_cost"] = avg_cost_list
        df["discount_pct"] = discount_pct_list

        return df[df["foreign_cost_valid"]].reset_index(drop=True)


# ========================================
# 回調縮量吸籌策略篩選器
# ========================================

class PullbackScreener(BaseScreener):
    """步驟2: 回調狀態篩選 - 跌破短期均線但守住長期均線"""

    def __init__(self, data_fetcher):
        super().__init__(name="回調狀態", step_number=2)
        self.data_fetcher = data_fetcher
        self.min_pullback = SCREENING_PARAMS.get("pullback_min_pct", 5.0)
        self.max_pullback = SCREENING_PARAMS.get("pullback_max_pct", 20.0)
        self.high_lookback = SCREENING_PARAMS.get("pullback_high_lookback_days", 20)
        self.short_ma = SCREENING_PARAMS.get("pullback_short_ma", [5, 10])
        self.long_ma = SCREENING_PARAMS.get("pullback_long_ma", [20, 60])

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        pullback_info = []
        pullback_pct_list = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]
            current_price = row["price"]

            hist_data = self.data_fetcher.get_historical_data(stock_id, days=70)
            if hist_data.empty or len(hist_data) < 60:
                valid_stocks.append(False)
                pullback_info.append("")
                pullback_pct_list.append(np.nan)
                continue

            closes = hist_data["close"]
            highs = hist_data["high"]

            # 計算均線
            ma_values = {}
            for period in self.short_ma + self.long_ma:
                if len(closes) >= period:
                    ma_values[period] = closes.rolling(period).mean().iloc[-1]

            # 條件1: 跌破短期均線 (MA5 或 MA10)
            below_short = False
            for period in self.short_ma:
                if period in ma_values and current_price < ma_values[period]:
                    below_short = True
                    break

            # 條件2: 守住長期均線 (MA20 或 MA60)
            above_long = False
            support_ma = None
            for period in self.long_ma:
                if period in ma_values and current_price > ma_values[period]:
                    above_long = True
                    support_ma = f"MA{period}"
                    break

            # 條件3: 從近期高點回落適當幅度
            recent_high = highs.tail(self.high_lookback).max()
            pullback_pct = ((recent_high - current_price) / recent_high) * 100
            proper_pullback = self.min_pullback <= pullback_pct <= self.max_pullback

            # 綜合判斷
            is_valid = below_short and above_long and proper_pullback

            valid_stocks.append(is_valid)
            pullback_pct_list.append(pullback_pct)

            if is_valid:
                pullback_info.append(f"回調{pullback_pct:.1f}% 守住{support_ma}")
            else:
                pullback_info.append("")

        df["pullback_valid"] = valid_stocks
        df["pullback_info"] = pullback_info
        df["pullback_pct"] = pullback_pct_list

        return df[df["pullback_valid"]].reset_index(drop=True)


class VolumeShrinkScreener(BaseScreener):
    """步驟3: 連續縮量篩選 - 成交量持續萎縮"""

    def __init__(self, data_fetcher):
        super().__init__(name="連續縮量", step_number=3)
        self.data_fetcher = data_fetcher
        self.shrink_days = SCREENING_PARAMS.get("volume_shrink_days", 3)
        self.shrink_threshold = SCREENING_PARAMS.get("volume_shrink_threshold", 0.7)
        self.avg_days = SCREENING_PARAMS.get("volume_avg_days", 20)

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        shrink_info = []
        volume_ratio_list = []
        shrink_days_list = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]
            current_volume = row["volume"]

            hist_data = self.data_fetcher.get_historical_data(stock_id, days=self.avg_days + 5)
            if hist_data.empty or len(hist_data) < self.avg_days:
                valid_stocks.append(False)
                shrink_info.append("")
                volume_ratio_list.append(np.nan)
                shrink_days_list.append(0)
                continue

            volumes = hist_data["volume"] / 1000  # 股 -> 張
            avg_volume = volumes.tail(self.avg_days).mean()

            # 計算連續縮量天數
            consecutive_shrink = 0
            recent_volumes = volumes.tail(self.shrink_days + 1).values
            for i in range(len(recent_volumes) - 1, 0, -1):
                if recent_volumes[i] < recent_volumes[i-1] * 1.05:  # 允許5%誤差
                    consecutive_shrink += 1
                else:
                    break

            # 當前量相對均量的比例
            volume_ratio = current_volume / avg_volume if avg_volume > 0 else 1.0

            # 條件1: 連續縮量天數 >= 門檻
            has_consecutive_shrink = consecutive_shrink >= self.shrink_days

            # 條件2: 當前量 < 均量的門檻比例
            is_low_volume = volume_ratio < self.shrink_threshold

            # 綜合判斷 (兩個條件至少符合一個)
            is_valid = has_consecutive_shrink or is_low_volume

            valid_stocks.append(is_valid)
            volume_ratio_list.append(volume_ratio)
            shrink_days_list.append(consecutive_shrink)

            if is_valid:
                shrink_info.append(f"量縮{consecutive_shrink}日 量比{volume_ratio:.1%}")
            else:
                shrink_info.append("")

        df["shrink_valid"] = valid_stocks
        df["shrink_info"] = shrink_info
        df["volume_ratio"] = volume_ratio_list
        df["shrink_days"] = shrink_days_list

        return df[df["shrink_valid"]].reset_index(drop=True)


class QuietAccumulationScreener(BaseScreener):
    """步驟6: 法人悄悄建倉篩選 - 回調中法人持續買超"""

    def __init__(self, data_fetcher):
        super().__init__(name="法人吸籌", step_number=6)
        self.data_fetcher = data_fetcher
        self.min_days = SCREENING_PARAMS.get("accumulation_min_days", 3)
        self.max_stability = SCREENING_PARAMS.get("accumulation_max_stability", 2.0)
        self._tracker = None

    def _get_tracker(self):
        """延遲初始化 InstitutionalTracker"""
        if self._tracker is None:
            from src.institutional_tracker import InstitutionalTracker
            self._tracker = InstitutionalTracker(self.data_fetcher)
        return self._tracker

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        tracker = self._get_tracker()

        valid_stocks = []
        accumulation_info = []
        foreign_consecutive_list = []
        trust_consecutive_list = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]

            # 使用 InstitutionalTracker 分析法人行為
            analysis = tracker.analyze_institutional_behavior(stock_id, days=20)

            if not analysis:
                valid_stocks.append(False)
                accumulation_info.append("")
                foreign_consecutive_list.append(0)
                trust_consecutive_list.append(0)
                continue

            foreign_consecutive = analysis.get("foreign_consecutive_buy", 0)
            trust_consecutive = analysis.get("trust_consecutive_buy", 0)
            foreign_stability = analysis.get("foreign_stability", 99)
            trust_stability = analysis.get("trust_stability", 99)
            foreign_sum = analysis.get("foreign_20d_sum", 0)
            trust_sum = analysis.get("trust_20d_sum", 0)

            # 條件: 外資或投信連續買超 >= 門檻天數
            foreign_valid = (
                foreign_consecutive >= self.min_days and
                foreign_stability < self.max_stability and
                foreign_sum > 0
            )
            trust_valid = (
                trust_consecutive >= self.min_days and
                trust_stability < self.max_stability and
                trust_sum > 0
            )

            is_valid = foreign_valid or trust_valid

            valid_stocks.append(is_valid)
            foreign_consecutive_list.append(foreign_consecutive)
            trust_consecutive_list.append(trust_consecutive)

            if foreign_valid and trust_valid:
                accumulation_info.append(f"外資連買{foreign_consecutive}日 投信連買{trust_consecutive}日")
            elif foreign_valid:
                accumulation_info.append(f"外資連買{foreign_consecutive}日 累計+{foreign_sum:,}張")
            elif trust_valid:
                accumulation_info.append(f"投信連買{trust_consecutive}日 累計+{trust_sum:,}張")
            else:
                accumulation_info.append("")

        df["accumulation_valid"] = valid_stocks
        df["accumulation_info"] = accumulation_info
        df["foreign_consecutive"] = foreign_consecutive_list
        df["trust_consecutive"] = trust_consecutive_list

        return df[df["accumulation_valid"]].reset_index(drop=True)


# ========================================
# 新增篩選器 - 基本面、技術面、籌碼面
# ========================================

class RevenueGrowthScreener(BaseScreener):
    """步驟2: 營收成長篩選 - 確保基本面健康"""

    def __init__(self, data_fetcher):
        super().__init__(name="營收成長", step_number=2)
        self.data_fetcher = data_fetcher
        self.min_growth = SCREENING_PARAMS.get("revenue_growth_min", 0)
        self.months_positive = SCREENING_PARAMS.get("revenue_months_positive", 2)

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        revenue_info = []
        growth_pct_list = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]

            # 獲取營收資料
            revenue_data = self._get_revenue_data(stock_id)

            if not revenue_data:
                # 無資料時保留股票 (避免誤殺)
                valid_stocks.append(True)
                revenue_info.append("資料不足")
                growth_pct_list.append(np.nan)
                continue

            latest_growth = revenue_data.get("latest_growth", 0)
            positive_months = revenue_data.get("positive_months", 0)

            # 條件: 最新營收年增率 >= 門檻 且 近N月有正成長
            is_valid = latest_growth >= self.min_growth and positive_months >= self.months_positive

            valid_stocks.append(is_valid)
            growth_pct_list.append(latest_growth)

            if is_valid:
                revenue_info.append(f"營收YoY {latest_growth:+.1f}% 連{positive_months}月正成長")
            else:
                revenue_info.append(f"營收YoY {latest_growth:+.1f}%")

        df["revenue_valid"] = valid_stocks
        df["revenue_info"] = revenue_info
        df["revenue_growth"] = growth_pct_list

        return df[df["revenue_valid"]].reset_index(drop=True)

    def _get_revenue_data(self, stock_id: str) -> Dict:
        """獲取營收資料"""
        try:
            import os
            import requests
            from datetime import datetime, timedelta

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

            url = "https://api.finmindtrade.com/api/v4/data"
            params = {
                "dataset": "TaiwanStockMonthRevenue",
                "data_id": stock_id,
                "start_date": start_date,
                "end_date": end_date
            }
            token = os.getenv("FINMIND_API_TOKEN", "")
            if token:
                params["token"] = token

            response = requests.get(url, params=params, timeout=10)
            result = response.json()

            if result.get("status") not in [200, "200"] or not result.get("data"):
                return {}

            df_rev = pd.DataFrame(result["data"])
            if df_rev.empty:
                return {}

            # 計算年增率
            if "revenue_year_growth_rate" in df_rev.columns:
                growth_rates = df_rev["revenue_year_growth_rate"].tail(6).tolist()
            elif "revenue" in df_rev.columns and len(df_rev) >= 13:
                # 手動計算
                growth_rates = []
                for i in range(-6, 0):
                    if len(df_rev) >= abs(i) + 12:
                        current = df_rev.iloc[i]["revenue"]
                        year_ago = df_rev.iloc[i - 12]["revenue"]
                        if year_ago > 0:
                            growth_rates.append((current - year_ago) / year_ago * 100)
            else:
                return {}

            if not growth_rates:
                return {}

            # 計算連續正成長月數
            positive_months = 0
            for g in reversed(growth_rates):
                if g > 0:
                    positive_months += 1
                else:
                    break

            return {
                "latest_growth": round(growth_rates[-1], 1) if growth_rates else 0,
                "positive_months": positive_months
            }

        except Exception as e:
            return {}


class PERatioScreener(BaseScreener):
    """步驟3: 本益比篩選 - 價值股篩選"""

    def __init__(self, data_fetcher):
        pe_max = SCREENING_PARAMS.get("pe_ratio_max", 20)
        super().__init__(name=f"本益比 < {pe_max}", step_number=3)
        self.data_fetcher = data_fetcher
        self.pe_min = SCREENING_PARAMS.get("pe_ratio_min", 0)
        self.pe_max = pe_max

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        pe_info = []
        pe_list = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]
            current_price = row["price"]

            # 獲取 EPS 資料
            pe_data = self._get_pe_data(stock_id, current_price)

            if not pe_data:
                # 無資料時保留股票
                valid_stocks.append(True)
                pe_info.append("資料不足")
                pe_list.append(np.nan)
                continue

            pe_ratio = pe_data.get("pe_ratio", 0)
            eps = pe_data.get("eps", 0)

            # 條件: PE > 0 (獲利) 且 PE <= 門檻
            is_valid = self.pe_min < pe_ratio <= self.pe_max

            valid_stocks.append(is_valid)
            pe_list.append(pe_ratio)

            if is_valid:
                pe_info.append(f"PE {pe_ratio:.1f} EPS {eps:.2f}")
            elif pe_ratio <= 0:
                pe_info.append(f"虧損股 EPS {eps:.2f}")
            else:
                pe_info.append(f"PE {pe_ratio:.1f} 過高")

        df["pe_valid"] = valid_stocks
        df["pe_info"] = pe_info
        df["pe_ratio"] = pe_list

        return df[df["pe_valid"]].reset_index(drop=True)

    def _get_pe_data(self, stock_id: str, current_price: float) -> Dict:
        """獲取本益比資料"""
        try:
            import os
            import requests
            from datetime import datetime, timedelta

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

            url = "https://api.finmindtrade.com/api/v4/data"
            params = {
                "dataset": "TaiwanStockFinancialStatements",
                "data_id": stock_id,
                "start_date": start_date,
                "end_date": end_date
            }
            token = os.getenv("FINMIND_API_TOKEN", "")
            if token:
                params["token"] = token

            response = requests.get(url, params=params, timeout=10)
            result = response.json()

            if result.get("status") not in [200, "200"] or not result.get("data"):
                return {}

            df_fin = pd.DataFrame(result["data"])
            if df_fin.empty:
                return {}

            # 找 EPS 欄位
            eps_df = df_fin[df_fin["type"] == "EPS"]
            if eps_df.empty:
                return {}

            # 取最近 4 季 EPS 加總
            eps = eps_df.tail(4)["value"].sum()

            if eps <= 0:
                return {"eps": round(eps, 2), "pe_ratio": 0}

            pe_ratio = current_price / eps

            return {
                "eps": round(eps, 2),
                "pe_ratio": round(pe_ratio, 1)
            }

        except Exception as e:
            return {}


class RSIOversoldScreener(BaseScreener):
    """步驟7: RSI 超賣篩選 - 技術面確認超賣且觸底回升 + 站回MA5"""

    def __init__(self, data_fetcher):
        rsi_threshold = SCREENING_PARAMS.get("rsi_oversold", 35)
        require_upturn = SCREENING_PARAMS.get("rsi_require_upturn", True)
        require_above_ma5 = SCREENING_PARAMS.get("rsi_require_above_ma5", True)

        # 組合篩選器名稱
        name_parts = [f"RSI < {rsi_threshold}"]
        if require_upturn:
            name_parts.append("回升")
        if require_above_ma5:
            name_parts.append("站MA5")
        name = " ".join(name_parts)

        super().__init__(name=name, step_number=7)
        self.data_fetcher = data_fetcher
        self.rsi_period = SCREENING_PARAMS.get("rsi_period", 14)
        self.rsi_oversold = rsi_threshold
        self.require_upturn = require_upturn
        self.require_above_ma5 = require_above_ma5

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        rsi_info = []
        rsi_list = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]
            current_price = row["price"]

            # 計算 RSI 和 MA5
            rsi_data = self._calculate_rsi_and_ma5(stock_id)

            if rsi_data is None:
                valid_stocks.append(True)  # 無資料時保留
                rsi_info.append("資料不足")
                rsi_list.append(np.nan)
                continue

            rsi_today = rsi_data["rsi_today"]
            rsi_yesterday = rsi_data["rsi_yesterday"]
            ma5 = rsi_data["ma5"]

            is_upturn = rsi_today > rsi_yesterday
            is_above_ma5 = current_price > ma5

            # 條件組合
            is_oversold = rsi_today <= self.rsi_oversold
            is_valid = is_oversold

            if self.require_upturn:
                is_valid = is_valid and is_upturn
            if self.require_above_ma5:
                is_valid = is_valid and is_above_ma5

            valid_stocks.append(is_valid)
            rsi_list.append(rsi_today)

            # 產生說明文字
            if is_valid:
                rsi_info.append(f"RSI {rsi_today:.1f} 觸底回升 站MA5")
            elif is_oversold and is_upturn and not is_above_ma5:
                rsi_info.append(f"RSI {rsi_today:.1f} 回升但未站MA5")
            elif is_oversold and not is_upturn:
                rsi_info.append(f"RSI {rsi_today:.1f} 仍在下探")
            else:
                rsi_info.append(f"RSI {rsi_today:.1f}")

        df["rsi_valid"] = valid_stocks
        df["rsi_info"] = rsi_info
        df["rsi"] = rsi_list

        return df[df["rsi_valid"]].reset_index(drop=True)

    def _calculate_rsi_and_ma5(self, stock_id: str) -> Optional[Dict]:
        """計算 RSI 和 MA5"""
        try:
            hist_data = self.data_fetcher.get_historical_data(stock_id, days=self.rsi_period + 15)
            if hist_data.empty or len(hist_data) < self.rsi_period + 2:
                return None

            closes = hist_data["close"]

            # 計算 MA5
            ma5 = closes.rolling(5).mean().iloc[-1]

            # 計算漲跌幅
            delta = closes.diff()

            # 分離漲跌
            gain = delta.where(delta > 0, 0)
            loss = (-delta).where(delta < 0, 0)

            # 計算平均漲跌 (使用 EMA)
            avg_gain = gain.ewm(span=self.rsi_period, adjust=False).mean()
            avg_loss = loss.ewm(span=self.rsi_period, adjust=False).mean()

            # 計算 RS 和 RSI
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

            return {
                "rsi_today": round(rsi.iloc[-1], 1),
                "rsi_yesterday": round(rsi.iloc[-2], 1),
                "ma5": ma5
            }

        except Exception as e:
            return None


class MajorHolderScreener(BaseScreener):
    """步驟9: 大戶持股篩選 - 千張大戶持股比例及變化"""

    def __init__(self, data_fetcher):
        min_pct = SCREENING_PARAMS.get("major_holder_min_pct", 30)
        super().__init__(name=f"大戶持股 >= {min_pct}%", step_number=9)
        self.data_fetcher = data_fetcher
        self.min_pct = min_pct
        self.increase_weeks = SCREENING_PARAMS.get("major_holder_increase_weeks", 1)

    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()

        valid_stocks = []
        holder_info = []
        holder_pct_list = []
        holder_change_list = []

        for idx, row in df.iterrows():
            stock_id = row["stock_id"]

            # 獲取大戶持股資料
            holder_data = self._get_major_holder_data(stock_id)

            if not holder_data:
                valid_stocks.append(True)  # 無資料時保留
                holder_info.append("資料不足")
                holder_pct_list.append(np.nan)
                holder_change_list.append(np.nan)
                continue

            current_pct = holder_data.get("current_pct", 0)
            pct_change = holder_data.get("pct_change", 0)
            increase_weeks = holder_data.get("increase_weeks", 0)

            # 條件: 大戶持股 >= 門檻 且 持股有增加
            is_valid = current_pct >= self.min_pct and increase_weeks >= self.increase_weeks

            valid_stocks.append(is_valid)
            holder_pct_list.append(current_pct)
            holder_change_list.append(pct_change)

            if is_valid:
                holder_info.append(f"大戶 {current_pct:.1f}% 連增{increase_weeks}週 {pct_change:+.1f}%")
            else:
                holder_info.append(f"大戶 {current_pct:.1f}% {pct_change:+.1f}%")

        df["holder_valid"] = valid_stocks
        df["holder_info"] = holder_info
        df["major_holder_pct"] = holder_pct_list
        df["holder_change"] = holder_change_list

        return df[df["holder_valid"]].reset_index(drop=True)

    def _get_major_holder_data(self, stock_id: str) -> Dict:
        """獲取大戶持股資料 (千張以上大戶)"""
        try:
            import os
            import requests
            from datetime import datetime, timedelta

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

            url = "https://api.finmindtrade.com/api/v4/data"
            params = {
                "dataset": "TaiwanStockHoldingSharesPer",
                "data_id": stock_id,
                "start_date": start_date,
                "end_date": end_date
            }
            token = os.getenv("FINMIND_API_TOKEN", "")
            if token:
                params["token"] = token

            response = requests.get(url, params=params, timeout=10)
            result = response.json()

            if result.get("status") not in [200, "200"] or not result.get("data"):
                return {}

            df_holder = pd.DataFrame(result["data"])
            if df_holder.empty:
                return {}

            # 篩選 1000 張以上的大戶 (HoldingSharesLevel >= 15 通常是大戶)
            # FinMind 的 HoldingSharesLevel: 1-15, 15 是 1000張以上
            major_df = df_holder[df_holder["HoldingSharesLevel"] == "15"]

            if major_df.empty:
                # 嘗試其他方式: 找最大持股級距
                major_df = df_holder[df_holder["HoldingSharesLevel"].astype(str).str.contains("1000|more", na=False)]

            if major_df.empty:
                return {}

            # 按日期排序
            major_df = major_df.sort_values("date")

            # 取最近幾週的資料
            recent_data = major_df.tail(4)  # 約4週

            if recent_data.empty:
                return {}

            current_pct = recent_data.iloc[-1]["percent"]

            # 計算持股變化
            if len(recent_data) >= 2:
                prev_pct = recent_data.iloc[-2]["percent"]
                pct_change = current_pct - prev_pct
            else:
                pct_change = 0

            # 計算連續增加週數
            increase_weeks = 0
            pcts = recent_data["percent"].tolist()
            for i in range(len(pcts) - 1, 0, -1):
                if pcts[i] > pcts[i - 1]:
                    increase_weeks += 1
                else:
                    break

            return {
                "current_pct": round(current_pct, 1),
                "pct_change": round(pct_change, 2),
                "increase_weeks": increase_weeks
            }

        except Exception as e:
            return {}
