"""
法人佈局追蹤系統
追蹤法人（外資/投信）的長期買賣行為，找出「偷偷佈局」的股票

追蹤指標：
1. 連續買超天數
2. 累積買超張數（5日/10日/20日）
3. 持股比例變化
4. 買超趨勢（是否穩定）
"""
import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List
import logging

from src.data.fetcher import DataFetcher
from config.settings import DATA_OUTPUT_DIR

logger = logging.getLogger(__name__)

# 法人追蹤資料存放路徑
TRACKER_DATA_DIR = DATA_OUTPUT_DIR / "institutional_tracker"


class InstitutionalTracker:
    """
    法人佈局追蹤器

    偵測「偷偷佈局」特徵：
    - 連續買超多天（>=5天）
    - 買超量穩定（不是暴量進出）
    - 股價沒有大漲（低調佈局）
    """

    def __init__(self, data_fetcher: DataFetcher = None):
        self.data_fetcher = data_fetcher or DataFetcher()
        self.tracker_dir = TRACKER_DATA_DIR
        self.tracker_dir.mkdir(parents=True, exist_ok=True)

    def _get_tracker_file(self, date: str = None) -> Path:
        """取得指定日期的追蹤檔案"""
        if date is None:
            date = datetime.now().strftime("%Y%m%d")
        return self.tracker_dir / f"tracker_{date}.json"

    def _get_history_file(self) -> Path:
        """取得歷史追蹤檔案"""
        return self.tracker_dir / "history.json"

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

    def get_institutional_data(self, stock_id: str, days: int = 20) -> pd.DataFrame:
        """
        獲取單一股票的法人買賣超資料
        Returns: DataFrame with columns [date, foreign_buy, trust_buy, dealer_buy, total]
        """
        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")

            url = "https://api.finmindtrade.com/api/v4/data"
            params = {
                "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
                "data_id": stock_id,
                "start_date": start_date,
                "end_date": end_date
            }

            import os
            import requests
            token = os.getenv("FINMIND_API_TOKEN", "")
            if token:
                params["token"] = token

            response = requests.get(url, params=params, timeout=15)
            result = response.json()

            if result.get("status") not in [200, "200"] or "data" not in result:
                return pd.DataFrame()

            df = pd.DataFrame(result["data"])
            if df.empty:
                return pd.DataFrame()

            # 計算買賣超 (buy - sell)
            df["buy_sell"] = df["buy"] - df["sell"]

            # 彙總每日各法人買賣超
            # name 欄位: Foreign_Investor(外資), Investment_Trust(投信), Dealer_self(自營商)
            pivot = df.pivot_table(
                index="date",
                columns="name",
                values="buy_sell",
                aggfunc="sum"
            ).reset_index()

            # 重新命名欄位
            rename_map = {
                "Foreign_Investor": "foreign",
                "Investment_Trust": "trust",
                "Dealer_self": "dealer",
                "Dealer_Self": "dealer",
                "Dealer_Hedging": "dealer_hedge",
                "Foreign_Dealer_Self": "foreign_dealer"
            }
            pivot = pivot.rename(columns=rename_map)

            # 合併外資相關欄位
            if "foreign_dealer" in pivot.columns and "foreign" not in pivot.columns:
                pivot["foreign"] = pivot.get("foreign_dealer", 0)

            # 確保欄位存在
            for col in ["foreign", "trust", "dealer"]:
                if col not in pivot.columns:
                    pivot[col] = 0

            # 計算合計
            pivot["total"] = pivot.get("foreign", 0) + pivot.get("trust", 0) + pivot.get("dealer", 0)

            # 轉換為張數（原始單位是股）
            for col in ["foreign", "trust", "dealer", "total"]:
                if col in pivot.columns:
                    pivot[col] = pivot[col] // 1000

            return pivot.tail(days).reset_index(drop=True)

        except Exception as e:
            logger.debug(f"獲取 {stock_id} 法人資料失敗: {e}")
            return pd.DataFrame()

    def analyze_institutional_behavior(self, stock_id: str, days: int = 20) -> Dict:
        """
        分析單一股票的法人行為
        Returns: {
            foreign_consecutive_buy: 外資連續買超天數,
            trust_consecutive_buy: 投信連續買超天數,
            foreign_5d_sum: 外資5日累計,
            foreign_10d_sum: 外資10日累計,
            foreign_20d_sum: 外資20日累計,
            trust_5d_sum: 投信5日累計,
            trust_10d_sum: 投信10日累計,
            trust_20d_sum: 投信20日累計,
            is_quietly_buying: 是否偷偷佈局,
            behavior_type: 行為類型描述
        }
        """
        df = self.get_institutional_data(stock_id, days)
        if df.empty or len(df) < 5:
            return {}

        result = {
            "stock_id": stock_id,
            "data_days": len(df)
        }

        # 計算外資連續買超天數
        foreign_consecutive = 0
        for val in df["foreign"].iloc[::-1]:  # 從最近一天往回看
            if val > 0:
                foreign_consecutive += 1
            else:
                break
        result["foreign_consecutive_buy"] = foreign_consecutive

        # 計算投信連續買超天數
        trust_consecutive = 0
        for val in df["trust"].iloc[::-1]:
            if val > 0:
                trust_consecutive += 1
            else:
                break
        result["trust_consecutive_buy"] = trust_consecutive

        # 計算累積買超量
        result["foreign_5d_sum"] = int(df["foreign"].tail(5).sum())
        result["foreign_10d_sum"] = int(df["foreign"].tail(10).sum())
        result["foreign_20d_sum"] = int(df["foreign"].tail(20).sum())

        result["trust_5d_sum"] = int(df["trust"].tail(5).sum())
        result["trust_10d_sum"] = int(df["trust"].tail(10).sum())
        result["trust_20d_sum"] = int(df["trust"].tail(20).sum())

        # 計算每日平均買超（用來判斷是否「小量穩定買」）
        result["foreign_daily_avg"] = int(df["foreign"].tail(10).mean())
        result["trust_daily_avg"] = int(df["trust"].tail(10).mean())

        # 計算買超穩定度（標準差/平均，越小越穩定）
        foreign_std = df["foreign"].tail(10).std()
        foreign_mean = abs(df["foreign"].tail(10).mean()) + 1  # 避免除以0
        result["foreign_stability"] = round(foreign_std / foreign_mean, 2)

        trust_std = df["trust"].tail(10).std()
        trust_mean = abs(df["trust"].tail(10).mean()) + 1
        result["trust_stability"] = round(trust_std / trust_mean, 2)

        # 判斷是否「偷偷佈局」
        # 條件：連續買超>=5天 且 買超量穩定（stability < 2）
        is_foreign_quietly_buying = (
            foreign_consecutive >= 5 and
            result["foreign_stability"] < 2.0 and
            result["foreign_20d_sum"] > 0
        )
        is_trust_quietly_buying = (
            trust_consecutive >= 5 and
            result["trust_stability"] < 2.0 and
            result["trust_20d_sum"] > 0
        )

        result["is_foreign_quietly_buying"] = is_foreign_quietly_buying
        result["is_trust_quietly_buying"] = is_trust_quietly_buying
        result["is_quietly_buying"] = is_foreign_quietly_buying or is_trust_quietly_buying

        # 行為類型判斷
        behaviors = []
        if is_foreign_quietly_buying:
            behaviors.append(f"外資悄悄佈局({foreign_consecutive}天)")
        if is_trust_quietly_buying:
            behaviors.append(f"投信悄悄佈局({trust_consecutive}天)")

        if result["foreign_20d_sum"] > 5000:
            behaviors.append("外資大量買超")
        elif result["foreign_20d_sum"] < -5000:
            behaviors.append("外資大量賣超")

        if result["trust_20d_sum"] > 2000:
            behaviors.append("投信大量買超")
        elif result["trust_20d_sum"] < -2000:
            behaviors.append("投信大量賣超")

        if not behaviors:
            if result["foreign_20d_sum"] > 0 and result["trust_20d_sum"] > 0:
                behaviors.append("法人小幅買超")
            elif result["foreign_20d_sum"] < 0 and result["trust_20d_sum"] < 0:
                behaviors.append("法人小幅賣超")
            else:
                behaviors.append("法人態度分歧")

        result["behavior_type"] = ", ".join(behaviors)

        return result

    def scan_quietly_buying_stocks(
        self,
        stock_ids: List[str],
        min_consecutive_days: int = 5
    ) -> pd.DataFrame:
        """
        掃描「偷偷佈局」的股票
        Args:
            stock_ids: 要掃描的股票列表
            min_consecutive_days: 最少連續買超天數
        Returns:
            符合條件的股票 DataFrame
        """
        logger.info(f"開始掃描法人佈局，共 {len(stock_ids)} 檔...")

        results = []
        for idx, stock_id in enumerate(stock_ids):
            if (idx + 1) % 50 == 0:
                logger.info(f"掃描進度: {idx + 1}/{len(stock_ids)}")

            analysis = self.analyze_institutional_behavior(stock_id)
            if not analysis:
                continue

            # 篩選：連續買超天數 >= min_consecutive_days
            if (analysis.get("foreign_consecutive_buy", 0) >= min_consecutive_days or
                analysis.get("trust_consecutive_buy", 0) >= min_consecutive_days):
                results.append(analysis)

        df = pd.DataFrame(results)
        if not df.empty:
            # 按連續買超天數排序
            df["max_consecutive"] = df[["foreign_consecutive_buy", "trust_consecutive_buy"]].max(axis=1)
            df = df.sort_values("max_consecutive", ascending=False)

        logger.info(f"法人佈局掃描完成: {len(df)} 檔符合條件")
        return df

    def update_tracking(self, analysis_results: List[Dict]) -> Dict:
        """
        更新追蹤歷史，計算變化趨勢
        """
        today = datetime.now().strftime("%Y%m%d")
        history = self.load_history()
        stocks_history = history.get("stocks", {})

        for result in analysis_results:
            stock_id = result.get("stock_id")
            if not stock_id:
                continue

            if stock_id not in stocks_history:
                stocks_history[stock_id] = {
                    "first_tracked": today,
                    "tracking_days": 0,
                    "history": []
                }

            stock_hist = stocks_history[stock_id]
            stock_hist["tracking_days"] += 1
            stock_hist["last_update"] = today

            # 記錄今日數據
            daily_record = {
                "date": today,
                "foreign_consecutive": result.get("foreign_consecutive_buy", 0),
                "trust_consecutive": result.get("trust_consecutive_buy", 0),
                "foreign_20d_sum": result.get("foreign_20d_sum", 0),
                "trust_20d_sum": result.get("trust_20d_sum", 0),
                "behavior": result.get("behavior_type", "")
            }
            stock_hist["history"].append(daily_record)

            # 只保留最近30天的記錄
            stock_hist["history"] = stock_hist["history"][-30:]

        history["stocks"] = stocks_history
        history["last_update"] = today
        self.save_history(history)

        return history

    def print_institutional_report(self, df: pd.DataFrame, stock_info: pd.DataFrame = None):
        """輸出法人佈局報告"""
        print("\n" + "=" * 80)
        print("  【法人佈局追蹤報告】")
        print("=" * 80)

        if df.empty:
            print("\n  今日無符合條件的法人佈局股票")
            print("=" * 80)
            return

        # 合併股票名稱
        if stock_info is not None and not stock_info.empty:
            df = df.merge(
                stock_info[["stock_id", "stock_name", "industry", "price", "change_pct"]],
                on="stock_id",
                how="left"
            )

        # 分類顯示
        # 1. 外資悄悄佈局
        foreign_quietly = df[df["is_foreign_quietly_buying"] == True].copy()
        if not foreign_quietly.empty:
            print(f"\n  🔍 外資悄悄佈局中 ({len(foreign_quietly)} 檔)")
            print("-" * 80)
            for _, row in foreign_quietly.head(15).iterrows():
                name = row.get("stock_name", "")[:6]
                industry = row.get("industry", "")[:8]
                consecutive = row.get("foreign_consecutive_buy", 0)
                sum_20d = row.get("foreign_20d_sum", 0)
                print(f"  {row['stock_id']} {name:<8} [{industry}] "
                      f"連買{consecutive}天 | 20日累計: {sum_20d:+,}張")

        # 2. 投信悄悄佈局
        trust_quietly = df[df["is_trust_quietly_buying"] == True].copy()
        if not trust_quietly.empty:
            print(f"\n  🔍 投信悄悄佈局中 ({len(trust_quietly)} 檔)")
            print("-" * 80)
            for _, row in trust_quietly.head(15).iterrows():
                name = row.get("stock_name", "")[:6]
                industry = row.get("industry", "")[:8]
                consecutive = row.get("trust_consecutive_buy", 0)
                sum_20d = row.get("trust_20d_sum", 0)
                print(f"  {row['stock_id']} {name:<8} [{industry}] "
                      f"連買{consecutive}天 | 20日累計: {sum_20d:+,}張")

        # 3. 連續買超排行（不限是否「悄悄」）
        print(f"\n  📊 法人連續買超排行 (前15名)")
        print("-" * 80)
        print(f"  {'代號':<6} {'名稱':<8} {'產業':<10} "
              f"{'外資連買':>8} {'投信連買':>8} {'外資20日':>10} {'投信20日':>10}")
        print("-" * 80)

        for _, row in df.head(15).iterrows():
            name = row.get("stock_name", "")[:6]
            industry = row.get("industry", "")[:8]
            print(f"  {row['stock_id']:<6} {name:<8} {industry:<10} "
                  f"{row.get('foreign_consecutive_buy', 0):>6}天 "
                  f"{row.get('trust_consecutive_buy', 0):>6}天 "
                  f"{row.get('foreign_20d_sum', 0):>+10,} "
                  f"{row.get('trust_20d_sum', 0):>+10,}")

        print("\n" + "=" * 80)
        print("  說明: 「悄悄佈局」= 連續買超>=5天 + 買超量穩定 + 累計為正")
        print("=" * 80)
