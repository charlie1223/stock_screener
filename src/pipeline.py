"""
趨勢多頭股篩選流水線 (含大盤均線警示)
優先篩選：基本面優良 + 法人認養 + 趨勢多頭
"""
import pandas as pd
from datetime import datetime
from typing import List, Dict
import logging

from src.data.fetcher import DataFetcher
from src.screeners.filters import (
    PriceChangeScreener,
    VolumeRatioScreener,
    TurnoverRateScreener,
    MarketCapScreener,
    MovingAverageScreener,
    RelativeStrengthScreener,
    IntradayHighScreener,
    InstitutionalHoldingScreener,
    FundamentalScreener,
    InstitutionalBuyScreener,
    ForeignConsecutiveBuyScreener,
    BelowForeignCostScreener,
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
    篩選流水線
    目標：找出趨勢多頭 + 基本面優良 + 法人認養的股票
    """

    def __init__(
        self,
        enable_fundamental: bool = True,
        enable_institutional: bool = True,
        enable_foreign_signal: bool = True
    ):
        """
        Args:
            enable_fundamental: 是否啟用基本面篩選
            enable_institutional: 是否啟用法人持股/買超篩選
            enable_foreign_signal: 是否啟用外資連續買超訊號篩選
        """
        self.data_fetcher = DataFetcher()
        self.market_monitor = MarketMonitor(self.data_fetcher)
        self.enable_fundamental = enable_fundamental
        self.enable_institutional = enable_institutional
        self.enable_foreign_signal = enable_foreign_signal
        self.screeners = self._init_screeners()
        self.stats = []
        self.market_status = None
        self.step_results = {}

    def _init_screeners(self) -> List:
        """
        初始化篩選器
        篩選順序優化：先用快速條件淘汰，再用耗時條件精篩
        """
        screeners = [
            # === 第一階段：快速篩選 (當日數據) ===
            PriceChangeScreener(),                              # 1. 漲幅 >= 3%
            VolumeRatioScreener(self.data_fetcher),            # 2. 量比 > 1
            TurnoverRateScreener(self.data_fetcher),           # 3. 換手率 1%-20%
            MarketCapScreener(self.data_fetcher),              # 4. 市值 20億以上

            # === 第二階段：趨勢篩選 (歷史數據) ===
            MovingAverageScreener(self.data_fetcher),          # 5. 均線多頭排列
            RelativeStrengthScreener(self.data_fetcher),       # 6. 強於大盤
            IntradayHighScreener(),                             # 7. 尾盤創新高
        ]

        # === 第三階段：法人籌碼篩選 ===
        if self.enable_institutional:
            screeners.extend([
                InstitutionalBuyScreener(self.data_fetcher),    # 8. 法人近5日買超
                InstitutionalHoldingScreener(self.data_fetcher), # 9. 法人持股/散戶比例
            ])

        # === 第四階段：基本面篩選 ===
        if self.enable_fundamental:
            screeners.append(
                FundamentalScreener(self.data_fetcher),         # 10. EPS>0, 營收成長
            )

        # === 第五階段：外資連續買超訊號 (參考 stock-tw.aiinpocket.com 策略) ===
        if self.enable_foreign_signal:
            screeners.extend([
                ForeignConsecutiveBuyScreener(self.data_fetcher, min_consecutive_days=3),  # 11. 外資連續3日買超
                BelowForeignCostScreener(self.data_fetcher, max_premium_pct=5.0),         # 12. 現價不超過外資成本5%
            ])

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
