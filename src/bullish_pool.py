"""
多頭股池追蹤系統
- 追蹤均線多頭排列的股票（體質追蹤）
- 記錄每日變化：新進/移出/持續天數
- 與每日訊號篩選分開
"""
import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging

from src.data.fetcher import DataFetcher
from config.settings import DATA_OUTPUT_DIR

logger = logging.getLogger(__name__)

# 多頭股池資料存放路徑
POOL_DATA_DIR = DATA_OUTPUT_DIR / "bullish_pool"


class BullishPoolTracker:
    """
    多頭股池追蹤器
    追蹤條件（體質條件，不含當日漲幅）：
    - 均線多頭排列 (MA5 > MA10 > MA20 > MA60)
    - 股價站上所有均線
    - 60日線向上
    """

    def __init__(self, data_fetcher: DataFetcher = None):
        self.data_fetcher = data_fetcher or DataFetcher()
        self.pool_dir = POOL_DATA_DIR
        self.pool_dir.mkdir(parents=True, exist_ok=True)
        self.ma_periods = [5, 10, 20, 60]

    def _get_pool_file(self, date: str = None) -> Path:
        """取得指定日期的股池檔案路徑"""
        if date is None:
            date = datetime.now().strftime("%Y%m%d")
        return self.pool_dir / f"pool_{date}.json"

    def _get_history_file(self) -> Path:
        """取得歷史追蹤檔案路徑"""
        return self.pool_dir / "history.json"

    def load_pool(self, date: str = None) -> Dict:
        """載入指定日期的股池資料"""
        pool_file = self._get_pool_file(date)
        if pool_file.exists():
            with open(pool_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def save_pool(self, pool_data: Dict, date: str = None):
        """儲存股池資料"""
        pool_file = self._get_pool_file(date)
        with open(pool_file, "w", encoding="utf-8") as f:
            json.dump(pool_data, f, ensure_ascii=False, indent=2)
        logger.info(f"多頭股池已儲存: {pool_file}")

    def load_history(self) -> Dict:
        """載入歷史追蹤資料"""
        history_file = self._get_history_file()
        if history_file.exists():
            with open(history_file, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"stocks": {}, "last_update": None}

    def save_history(self, history: Dict):
        """儲存歷史追蹤資料"""
        history_file = self._get_history_file()
        with open(history_file, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

    def check_bullish_condition(self, stock_id: str) -> Tuple[bool, Dict]:
        """
        檢查單一股票是否符合多頭條件
        Returns: (is_bullish, details)
        """
        hist = self.data_fetcher.get_historical_data(stock_id, days=70)
        if hist.empty or len(hist) < 60:
            return False, {}

        # 計算均線
        ma_values = {}
        for period in self.ma_periods:
            ma_values[period] = hist["close"].tail(period).mean()

        current_price = hist["close"].iloc[-1]

        # 檢查條件
        # 1. 股價站上所有均線
        above_all_ma = all(current_price > ma_values[p] for p in self.ma_periods)

        # 2. 均線多頭排列 (MA5 > MA10 > MA20 > MA60)
        ma_bullish = (
            ma_values[5] > ma_values[10] > ma_values[20] > ma_values[60]
        )

        # 3. 60日線向上 (比較最近5日的MA60)
        if len(hist) >= 65:
            ma60_5days_ago = hist["close"].iloc[-65:-5].tail(60).mean()
            ma60_trending_up = ma_values[60] > ma60_5days_ago
        else:
            ma60_trending_up = True  # 資料不足時預設為 True

        is_bullish = above_all_ma and ma_bullish and ma60_trending_up

        details = {
            "price": current_price,
            "ma_values": ma_values,
            "above_all_ma": above_all_ma,
            "ma_bullish": ma_bullish,
            "ma60_trending_up": ma60_trending_up
        }

        return is_bullish, details

    def scan_bullish_stocks(self, stock_df: pd.DataFrame = None) -> pd.DataFrame:
        """
        掃描所有股票，找出符合多頭條件的股票
        Args:
            stock_df: 股票清單 DataFrame (需包含 stock_id, stock_name)
                     若為 None 則自動獲取
        Returns:
            符合多頭條件的股票 DataFrame
        """
        if stock_df is None:
            stock_df = self.data_fetcher.get_all_stocks_realtime()

        if stock_df.empty:
            logger.warning("無法獲取股票清單")
            return pd.DataFrame()

        # 加入產業分類
        industry_map = self.data_fetcher.get_industry_classification()

        logger.info(f"開始掃描多頭股票，共 {len(stock_df)} 檔...")
        bullish_stocks = []
        total = len(stock_df)

        for idx, row in stock_df.iterrows():
            stock_id = row["stock_id"]

            # 進度顯示
            if (idx + 1) % 100 == 0:
                logger.info(f"掃描進度: {idx + 1}/{total}")

            is_bullish, details = self.check_bullish_condition(stock_id)

            if is_bullish:
                bullish_stocks.append({
                    "stock_id": stock_id,
                    "stock_name": row.get("stock_name", ""),
                    "industry": industry_map.get(stock_id, "未分類"),
                    "price": row.get("price", details.get("price", 0)),
                    "change_pct": row.get("change_pct", 0),
                    "ma5": round(details["ma_values"][5], 2),
                    "ma10": round(details["ma_values"][10], 2),
                    "ma20": round(details["ma_values"][20], 2),
                    "ma60": round(details["ma_values"][60], 2),
                })

        result_df = pd.DataFrame(bullish_stocks)
        logger.info(f"多頭股池掃描完成: 共 {len(result_df)} 檔符合條件")
        return result_df

    def update_pool(self, bullish_df: pd.DataFrame) -> Dict:
        """
        更新多頭股池，計算新進/移出/持續天數
        Args:
            bullish_df: 今日多頭股票 DataFrame
        Returns:
            更新結果 {new_entries, removed, continued, pool_data}
        """
        today = datetime.now().strftime("%Y%m%d")
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")

        # 載入昨日股池和歷史
        yesterday_pool = self.load_pool(yesterday)
        history = self.load_history()

        yesterday_ids = set(yesterday_pool.get("stocks", {}).keys())
        today_ids = set(bullish_df["stock_id"].tolist()) if not bullish_df.empty else set()

        # 計算變化
        new_entries = today_ids - yesterday_ids
        removed = yesterday_ids - today_ids
        continued = today_ids & yesterday_ids

        # 更新歷史追蹤 (連續天數)
        stocks_history = history.get("stocks", {})

        # 新進股票
        for sid in new_entries:
            stocks_history[sid] = {
                "first_date": today,
                "consecutive_days": 1,
                "last_date": today
            }

        # 持續股票
        for sid in continued:
            if sid in stocks_history:
                stocks_history[sid]["consecutive_days"] += 1
                stocks_history[sid]["last_date"] = today
            else:
                stocks_history[sid] = {
                    "first_date": today,
                    "consecutive_days": 1,
                    "last_date": today
                }

        # 移出股票 (保留記錄但標記)
        for sid in removed:
            if sid in stocks_history:
                stocks_history[sid]["removed_date"] = today

        history["stocks"] = stocks_history
        history["last_update"] = today
        self.save_history(history)

        # 建立今日股池資料
        pool_data = {"date": today, "stocks": {}}
        for _, row in bullish_df.iterrows():
            sid = row["stock_id"]
            pool_data["stocks"][sid] = {
                "stock_name": row.get("stock_name", ""),
                "industry": row.get("industry", "未分類"),
                "price": row.get("price", 0),
                "change_pct": row.get("change_pct", 0),
                "consecutive_days": stocks_history.get(sid, {}).get("consecutive_days", 1)
            }

        self.save_pool(pool_data, today)

        return {
            "new_entries": list(new_entries),
            "removed": list(removed),
            "continued": list(continued),
            "pool_data": pool_data
        }

    def get_pool_summary(self, pool_data: Dict = None) -> Dict:
        """
        取得股池摘要統計
        """
        if pool_data is None:
            pool_data = self.load_pool()

        stocks = pool_data.get("stocks", {})
        if not stocks:
            return {"total": 0, "by_industry": {}, "by_days": {}}

        # 產業分布
        industry_counts = {}
        for sid, info in stocks.items():
            ind = info.get("industry", "未分類")
            industry_counts[ind] = industry_counts.get(ind, 0) + 1

        # 連續天數分布
        days_counts = {}
        for sid, info in stocks.items():
            days = info.get("consecutive_days", 1)
            if days >= 10:
                key = "10天以上"
            elif days >= 5:
                key = "5-9天"
            else:
                key = f"{days}天"
            days_counts[key] = days_counts.get(key, 0) + 1

        return {
            "total": len(stocks),
            "by_industry": dict(sorted(industry_counts.items(), key=lambda x: -x[1])),
            "by_days": days_counts
        }

    def print_pool_report(self, update_result: Dict):
        """輸出多頭股池報告"""
        print("\n" + "=" * 80)
        print("  【多頭股池報告】- 體質追蹤")
        print("=" * 80)

        pool_data = update_result.get("pool_data", {})
        stocks = pool_data.get("stocks", {})

        print(f"\n  今日多頭股池: {len(stocks)} 檔")

        # 新進股票
        new_entries = update_result.get("new_entries", [])
        if new_entries:
            print(f"\n  🆕 新進多頭 ({len(new_entries)} 檔):")
            for sid in new_entries[:10]:
                info = stocks.get(sid, {})
                print(f"     {sid} {info.get('stock_name', '')} [{info.get('industry', '')}]")
            if len(new_entries) > 10:
                print(f"     ... 還有 {len(new_entries) - 10} 檔")

        # 移出股票
        removed = update_result.get("removed", [])
        if removed:
            print(f"\n  ⚠️  跌出多頭 ({len(removed)} 檔):")
            for sid in removed[:10]:
                print(f"     {sid}")
            if len(removed) > 10:
                print(f"     ... 還有 {len(removed) - 10} 檔")

        # 連續多頭排行 (前10名)
        if stocks:
            sorted_stocks = sorted(
                stocks.items(),
                key=lambda x: x[1].get("consecutive_days", 0),
                reverse=True
            )[:10]

            print(f"\n  🏆 連續多頭排行 (前10名):")
            for sid, info in sorted_stocks:
                days = info.get("consecutive_days", 1)
                print(f"     {sid} {info.get('stock_name', ''):<8} "
                      f"[{info.get('industry', '')}] - 連續 {days} 天")

        # 產業分布
        summary = self.get_pool_summary(pool_data)
        if summary["by_industry"]:
            print(f"\n  📊 產業分布:")
            for ind, cnt in list(summary["by_industry"].items())[:8]:
                print(f"     {ind}: {cnt} 檔")

        print("\n" + "=" * 80)
