"""
回調縮量吸籌策略篩選流水線
找出：回調縮量 + 守住支撐 + 法人悄悄建倉的股票
"""
import pandas as pd
from datetime import datetime
from typing import List, Dict
import logging

from src.data.fetcher import DataFetcher
from src.screeners.filters import (
    MarketCapScreener,
    TurnoverRateScreener,
    PullbackScreener,
    VolumeShrinkScreener,
    MASupportScreener,
    QuietAccumulationScreener,
)

logger = logging.getLogger(__name__)


class MarketMonitor:
    """大盤/OTC 均線監控器"""

    def __init__(self, data_fetcher: DataFetcher):
        self.data_fetcher = data_fetcher
        self.ma_periods = [5, 10, 20, 60]

    def check_market_status(self) -> Dict:
        """
        檢查大盤和 OTC 的均線狀態
        Returns: {
            "twse": {...},
            "otc": {...},
            "warnings": [...],
            "is_safe": bool
        }
        """
        logger.info("正在檢查大盤/OTC 均線狀態...")

        twse_status = self.data_fetcher.get_index_ma_status("TWSE", self.ma_periods)
        otc_status = self.data_fetcher.get_index_ma_status("OTC", self.ma_periods)

        warnings = []
        is_safe = True

        # 檢查加權指數
        if twse_status:
            if twse_status.get("broken_ma"):
                broken = twse_status["broken_ma"]
                warnings.append(f"⚠️  加權指數跌破 MA{broken} 均線！")
                is_safe = False
            if not twse_status.get("is_bullish"):
                warnings.append("⚠️  加權指數均線非多頭排列")

        # 檢查櫃買指數
        if otc_status:
            if otc_status.get("broken_ma"):
                broken = otc_status["broken_ma"]
                warnings.append(f"⚠️  櫃買指數跌破 MA{broken} 均線！")
                is_safe = False
            if not otc_status.get("is_bullish"):
                warnings.append("⚠️  櫃買指數均線非多頭排列")

        return {
            "twse": twse_status,
            "otc": otc_status,
            "warnings": warnings,
            "is_safe": is_safe
        }

    def print_market_status(self, status: Dict):
        """輸出大盤狀態"""
        print("\n" + "=" * 60)
        print("  大盤/OTC 均線狀態監控")
        print("=" * 60)

        # 加權指數
        twse = status.get("twse", {})
        if twse:
            print(f"\n【加權指數】 現價: {twse.get('current_price', 'N/A')}")
            ma_values = twse.get("ma_values", {})
            above_ma = twse.get("above_ma", {})
            for period in self.ma_periods:
                if period in ma_values:
                    status_icon = "✓" if above_ma.get(period, False) else "✗"
                    print(f"  MA{period}: {ma_values[period]:,.2f} [{status_icon}]")
            bullish = "多頭排列 ✓" if twse.get("is_bullish") else "非多頭排列 ✗"
            print(f"  均線排列: {bullish}")

        # 櫃買指數
        otc = status.get("otc", {})
        if otc:
            print(f"\n【櫃買指數】 現價: {otc.get('current_price', 'N/A')}")
            ma_values = otc.get("ma_values", {})
            above_ma = otc.get("above_ma", {})
            for period in self.ma_periods:
                if period in ma_values:
                    status_icon = "✓" if above_ma.get(period, False) else "✗"
                    print(f"  MA{period}: {ma_values[period]:,.2f} [{status_icon}]")
            bullish = "多頭排列 ✓" if otc.get("is_bullish") else "非多頭排列 ✗"
            print(f"  均線排列: {bullish}")

        # 警示
        warnings = status.get("warnings", [])
        if warnings:
            print("\n" + "!" * 60)
            print("  ⚠️  大盤警示  ⚠️")
            print("!" * 60)
            for w in warnings:
                print(f"  {w}")
            print("\n  建議: 大盤破均線時應減碼操作，優先砍破線股")
            print("        保留多方型態股票，或持有現金等待機會")
            print("!" * 60)
        else:
            print("\n  ✅ 大盤均線狀態正常")

        print("=" * 60 + "\n")


class ScreeningPipeline:
    """
    回調縮量吸籌策略篩選流水線
    目標：找出回調縮量 + 守住支撐 + 法人悄悄建倉的股票
    """

    def __init__(self):
        self.data_fetcher = DataFetcher()
        self.market_monitor = MarketMonitor(self.data_fetcher)
        self.screeners = self._init_screeners()
        self.stats = []
        self.market_status = None
        self.step_results = {}

    def _init_screeners(self) -> List:
        """
        初始化篩選器 - 回調縮量吸籌策略
        """
        screeners = [
            # 步驟1: 市值篩選 (快速排除小型股)
            MarketCapScreener(self.data_fetcher),

            # 步驟2: 回調狀態 (跌破短期均線、守住長期均線)
            PullbackScreener(self.data_fetcher),

            # 步驟3: 連續縮量 (成交量萎縮)
            VolumeShrinkScreener(self.data_fetcher),

            # 步驟4: 均線支撐 (守住 MA20/MA60 且斜率向上)
            MASupportScreener(self.data_fetcher),

            # 步驟5: 換手率篩選 (確保流動性)
            TurnoverRateScreener(self.data_fetcher),

            # 步驟6: 法人吸籌 (連續買超、穩定建倉)
            QuietAccumulationScreener(self.data_fetcher),
        ]

        return screeners

    def run(self, check_market: bool = True) -> pd.DataFrame:
        """
        執行完整篩選流程
        Args:
            check_market: 是否檢查大盤均線狀態
        """
        logger.info("=" * 60)
        logger.info(f"開始執行尾盤選股篩選 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        # 0. 檢查大盤均線狀態
        if check_market:
            self.market_status = self.market_monitor.check_market_status()
            self.market_monitor.print_market_status(self.market_status)

            # 如果大盤破均線，發出警告但仍繼續篩選
            if not self.market_status.get("is_safe", True):
                logger.warning("⚠️  大盤破均線警示！建議減碼操作")

        # 1. 獲取所有股票即時報價
        df = self.data_fetcher.get_all_stocks_realtime()

        if df.empty:
            logger.warning("無法獲取即時報價數據")
            return pd.DataFrame()

        logger.info(f"共獲取 {len(df)} 檔股票即時報價")

        # 1.5 加入產業分類
        industry_map = self.data_fetcher.get_industry_classification()
        if industry_map:
            df["industry"] = df["stock_id"].map(industry_map).fillna("未分類")

        # 2. 依序執行篩選步驟，並儲存每一步的結果
        self.stats = []
        self.step_results = {}

        for screener in self.screeners:
            if df.empty:
                logger.warning(f"在步驟 {screener.step_number} 前已無剩餘股票")
                break

            df = screener(df)
            self.stats.append(screener.get_stats())

            # 儲存這一步的結果
            self.step_results[screener.step_number] = {
                "name": screener.name,
                "data": df.copy() if not df.empty else pd.DataFrame()
            }

        # 3. 輸出結果摘要
        self._print_summary()

        return df

    def get_step_results(self) -> Dict:
        """取得每一步篩選的結果"""
        return self.step_results

    def _print_summary(self):
        """輸出篩選摘要"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("篩選結果摘要")
        logger.info("=" * 60)

        for stat in self.stats:
            logger.info(
                f"步驟{stat['step']:>2}: {stat['name']:<15} | "
                f"輸入: {stat['input']:>4} | 輸出: {stat['output']:>4} | "
                f"通過率: {stat['pass_rate']}"
            )

        if self.stats:
            final_count = self.stats[-1]["output"]
            initial_count = self.stats[0]["input"]
            overall_rate = f"{final_count/initial_count*100:.2f}%" if initial_count else "0%"
            logger.info("-" * 60)
            logger.info(f"最終篩選結果: {final_count} 檔 (總通過率: {overall_rate})")

    def get_market_status(self) -> Dict:
        """取得大盤狀態"""
        return self.market_status or {}
