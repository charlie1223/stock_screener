"""
台股選股策略篩選流水線
支援兩種模式:
  左側 (left)  = 回調縮量吸籌 → 還沒漲就先買
  右側 (right) = 撒網抓強勢   → 已經在漲才追，留強砍弱
"""
import pandas as pd
from datetime import datetime
from typing import List, Dict
import logging

from src.data.fetcher import DataFetcher
from src.foreign_sentiment import ForeignSentimentAnalyzer
from src.screeners.filters import (
    # 共用
    MarketCapScreener,
    # 左側策略
    TurnoverRateScreener,
    PullbackScreener,
    VolumePriceHealthScreener,
    VolumeShrinkScreener,
    QuietAccumulationScreener,
    RevenueGrowthScreener,
    PERatioScreener,
    HigherLowsScreener,
    RSIOversoldScreener,
    MajorHolderScreener,
    # 右側策略
    PriceChangeScreener,
    VolumeRatioScreener,
    MovingAverageScreener,
    RelativeStrengthScreener,
    IntradayHighScreener,
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


STRATEGY_NAMES = {
    "left": "回調縮量吸籌策略 v4.0",
    "right": "撒網抓強勢策略 v1.0",
}


class ScreeningPipeline:
    """
    台股選股策略篩選流水線
    支援左側 (回調吸籌) 和右側 (撒網抓強勢) 兩種模式
    """

    def __init__(self, mode: str = "left"):
        self.mode = mode
        self.data_fetcher = DataFetcher()
        self.market_monitor = MarketMonitor(self.data_fetcher)
        self.foreign_sentiment = ForeignSentimentAnalyzer()
        self.screeners = self._init_screeners()
        self.stats = []
        self.market_status = None
        self.foreign_sentiment_result = None
        self.step_results = {}

    @property
    def strategy_name(self) -> str:
        return STRATEGY_NAMES.get(self.mode, STRATEGY_NAMES["left"])

    def _init_screeners(self) -> List:
        """根據模式初始化對應的篩選器鏈"""
        if self.mode == "right":
            return self._init_right_screeners()
        return self._init_left_screeners()

    def _init_left_screeners(self) -> List:
        """
        左側策略: 回調縮量吸籌 (10 步)
        邏輯: 基本面OK → 趨勢向上 → 正在回調 → 量價健康 → 量縮 → 籌碼面
        """
        screeners = [
            # ========== 快速排除 ==========
            # 步驟1: 市值篩選 (快速排除小型股)
            MarketCapScreener(self.data_fetcher),

            # ========== 基本面篩選 ==========
            # 步驟2: 營收成長 (確保不是爛股票)
            RevenueGrowthScreener(self.data_fetcher),

            # 步驟3: 本益比篩選 (價值股)
            PERatioScreener(self.data_fetcher),

            # ========== 趨勢確認 ==========
            # 步驟4: 底底高確認 (上升趨勢仍在，低點持續墊高)
            HigherLowsScreener(self.data_fetcher),

            # ========== 技術面篩選 ==========
            # 步驟5: 回調狀態 (跌破短期均線、守住長期均線+斜率向上)
            PullbackScreener(self.data_fetcher),

            # 步驟6: 量價健康度 (排除竭盡量，保留健康量/換手量)
            VolumePriceHealthScreener(self.data_fetcher),

            # 步驟7: 連續縮量 (成交量萎縮)
            VolumeShrinkScreener(self.data_fetcher),

            # 步驟8: RSI 超賣 (技術面確認超賣)
            RSIOversoldScreener(self.data_fetcher),

            # ========== 流動性篩選 ==========
            # 步驟9: 換手率篩選 (確保流動性)
            TurnoverRateScreener(self.data_fetcher),

            # ========== 籌碼面篩選 ==========
            # 步驟10: 大戶持股 (籌碼集中)
            MajorHolderScreener(self.data_fetcher),

            # 步驟11: 法人吸籌 (連續買超、穩定建倉)
            QuietAccumulationScreener(self.data_fetcher),
        ]

        return screeners

    def _init_right_screeners(self) -> List:
        """
        右側策略: 撒網抓強勢 (6 步)
        邏輯: 今天在噴 → 爆量確認 → 均線多頭 → 強於大盤 → 尾盤仍強
        操作: 結果按漲幅排名，留強砍弱
        """
        screeners = [
            # 步驟1: 市值篩選 (排除小型股)
            MarketCapScreener(self.data_fetcher),

            # 步驟2: 當日漲幅 >= 3% (已經在噴)
            PriceChangeScreener(),

            # 步驟3: 量比 > 1.5 (爆量確認，有人在買)
            VolumeRatioScreener(self.data_fetcher),

            # 步驟4: 均線多頭排列 (趨勢向上)
            MovingAverageScreener(self.data_fetcher),

            # 步驟5: 強於大盤 (個股漲幅 > 大盤漲幅)
            RelativeStrengthScreener(self.data_fetcher),

            # 步驟6: 尾盤創新高 (收盤前仍在高點，不是沖高回落)
            IntradayHighScreener(),
        ]

        # 動態設定 step_number (1-6)
        for i, screener in enumerate(screeners, 1):
            screener.step_number = i

        return screeners

    def run(self, check_market: bool = True) -> pd.DataFrame:
        """
        執行完整篩選流程
        Args:
            check_market: 是否檢查大盤均線狀態
        """
        mode_label = "左側-回調吸籌" if self.mode == "left" else "右側-撒網抓強勢"
        logger.info("=" * 60)
        logger.info(f"開始執行尾盤選股篩選 [{mode_label}] - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        # 0. 外資動向分析 (現貨+期貨交叉判讀)
        self.foreign_sentiment_result = self.foreign_sentiment.analyze()
        self._print_foreign_sentiment()

        # 0.5 檢查大盤均線狀態
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

        # 3. 右側策略: 按漲幅排名 (方便留強砍弱)
        if self.mode == "right" and not df.empty and "change_pct" in df.columns:
            df = df.sort_values("change_pct", ascending=False).reset_index(drop=True)
            df["rank"] = range(1, len(df) + 1)

        # 4. 輸出結果摘要
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

    def _print_foreign_sentiment(self):
        """輸出外資動向分析"""
        r = self.foreign_sentiment_result
        if not r:
            return

        print("\n" + "=" * 60)
        print("  外資動向分析 (現貨+期貨交叉判讀)")
        print("=" * 60)
        print(f"  日期: {r['date']}")
        print(f"  現貨: {r['spot_direction']} {abs(r['spot_net']):.1f} 億")
        print(f"  期貨: {r['futures_direction']} {abs(r['futures_oi_change']):,} 口")
        print(f"  ─────────────────────────────")
        print(f"  判讀: {r['icon']} {r['sentiment']}")
        print(f"  說明: {r['detail']}")

        # 操作建議
        sentiment = r["sentiment"]
        if sentiment == "絕對看多":
            print(f"  建議: 積極操作，選股結果可信度高")
        elif sentiment == "策略對沖":
            print(f"  建議: 正常操作，外資有避險但仍在買現貨")
        elif sentiment == "絕對看空":
            print(f"  建議: 保守操作，考慮減碼或觀望")
        elif sentiment == "底部佈局":
            print(f"  建議: 留意反轉訊號，外資可能在佈局底部")

        print("=" * 60 + "\n")

    def get_market_status(self) -> Dict:
        """取得大盤狀態"""
        return self.market_status or {}

    def get_foreign_sentiment(self) -> Dict:
        """取得外資動向分析結果"""
        return self.foreign_sentiment_result or {}
