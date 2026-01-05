"""
八大步驟篩選流水線
"""
import pandas as pd
from datetime import datetime
from typing import List
import logging

from src.data.fetcher import DataFetcher
from src.screeners.filters import (
    PriceChangeScreener,
    VolumeRatioScreener,
    TurnoverRateScreener,
    MarketCapScreener,
    VolumeTrendScreener,
    MovingAverageScreener,
    RelativeStrengthScreener,
    IntradayHighScreener,
)

logger = logging.getLogger(__name__)


class ScreeningPipeline:
    """篩選流水線"""

    def __init__(self):
        self.data_fetcher = DataFetcher()
        self.screeners = self._init_screeners()
        self.stats = []

    def _init_screeners(self) -> List:
        """初始化八大篩選器"""
        return [
            PriceChangeScreener(),                              # 步驟1
            VolumeRatioScreener(self.data_fetcher),            # 步驟2
            TurnoverRateScreener(self.data_fetcher),           # 步驟3
            MarketCapScreener(self.data_fetcher),              # 步驟4
            VolumeTrendScreener(self.data_fetcher),            # 步驟5
            MovingAverageScreener(self.data_fetcher),          # 步驟6
            RelativeStrengthScreener(self.data_fetcher),       # 步驟7
            IntradayHighScreener(),                             # 步驟8
        ]

    def run(self) -> pd.DataFrame:
        """執行完整篩選流程"""
        logger.info("=" * 60)
        logger.info(f"開始執行尾盤選股篩選 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)

        # 1. 獲取所有股票即時報價
        df = self.data_fetcher.get_all_stocks_realtime()

        if df.empty:
            logger.warning("無法獲取即時報價數據")
            return pd.DataFrame()

        logger.info(f"共獲取 {len(df)} 檔股票即時報價")

        # 2. 依序執行八大篩選步驟
        self.stats = []
        for screener in self.screeners:
            if df.empty:
                logger.warning(f"在步驟 {screener.step_number} 前已無剩餘股票")
                break

            df = screener(df)
            self.stats.append(screener.get_stats())

        # 3. 輸出結果摘要
        self._print_summary()

        return df

    def _print_summary(self):
        """輸出篩選摘要"""
        logger.info("")
        logger.info("=" * 60)
        logger.info("篩選結果摘要")
        logger.info("=" * 60)

        for stat in self.stats:
            logger.info(
                f"步驟{stat['step']}: {stat['name']:<15} | "
                f"輸入: {stat['input']:>4} | 輸出: {stat['output']:>4} | "
                f"通過率: {stat['pass_rate']}"
            )

        if self.stats:
            final_count = self.stats[-1]["output"]
            initial_count = self.stats[0]["input"]
            overall_rate = f"{final_count/initial_count*100:.2f}%" if initial_count else "0%"
            logger.info("-" * 60)
            logger.info(f"最終篩選結果: {final_count} 檔 (總通過率: {overall_rate})")
