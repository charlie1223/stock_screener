"""
台股數據獲取模組
整合 TWSE/TPEx API 和 FinMind
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
import logging
import time as time_module

logger = logging.getLogger(__name__)


class DataFetcher:
    """統一數據獲取類"""

    def __init__(self):
        self._stock_info_cache = {}
        self._hist_data_cache = {}

    def get_all_stocks_realtime(self) -> pd.DataFrame:
        """
        獲取所有上市櫃股票即時報價
        Returns: DataFrame with columns:
            [stock_id, stock_name, price, open, high, low, volume,
             prev_close, change_pct, market]
        """
        logger.info("正在獲取上市股票即時報價...")
        twse_df = self._fetch_twse_realtime()
        time_module.sleep(0.5)

        logger.info("正在獲取上櫃股票即時報價...")
        tpex_df = self._fetch_tpex_realtime()

        if twse_df.empty and tpex_df.empty:
            logger.error("無法獲取任何即時報價數據")
            return pd.DataFrame()

        df = pd.concat([twse_df, tpex_df], ignore_index=True)
        logger.info(f"共獲取 {len(df)} 檔股票即時報價")
        return df

    def _fetch_twse_realtime(self) -> pd.DataFrame:
        """從證交所獲取上市股票即時報價"""
        url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
        params = {
            "response": "json",
            "date": datetime.now().strftime("%Y%m%d"),
            "type": "ALLBUT0999"
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            data = response.json()

            # 新版 API 使用 tables 格式，股票資料在 tables[8]
            stock_data = None
            if "tables" in data and len(data["tables"]) > 8:
                stock_data = data["tables"][8].get("data", [])
            elif "data9" in data:
                # 舊版 API 格式 (備用)
                stock_data = data["data9"]

            if not stock_data:
                logger.warning("TWSE API 無資料 (可能非交易時段)")
                return pd.DataFrame()

            rows = []
            for item in stock_data:
                try:
                    stock_id = item[0].strip()
                    # 只取一般股票 (4碼數字)
                    if not stock_id.isdigit() or len(stock_id) != 4:
                        continue

                    # 新版 API 欄位順序:
                    # 0:代號, 1:名稱, 2:成交股數, 3:成交筆數, 4:成交金額,
                    # 5:開盤價, 6:最高價, 7:最低價, 8:收盤價, 9:漲跌符號, 10:漲跌價差

                    # 處理收盤價 (可能有 "--" 或空值)
                    price_str = item[8].replace(",", "").strip()
                    if price_str == "--" or not price_str:
                        continue
                    price = float(price_str)

                    # 計算昨收價 (收盤價 - 漲跌價差)
                    change_val_str = item[10].replace(",", "").strip() if len(item) > 10 else "0"
                    change_val = float(change_val_str) if change_val_str and change_val_str != "--" else 0
                    # 判斷漲跌符號
                    change_sign = item[9].strip() if len(item) > 9 else ""
                    if "-" in change_sign or "green" in change_sign:
                        change_val = -abs(change_val)
                    prev_close = price - change_val

                    open_str = item[5].replace(",", "").strip()
                    open_price = float(open_str) if open_str and open_str != "--" else price

                    high_str = item[6].replace(",", "").strip()
                    high = float(high_str) if high_str and high_str != "--" else price

                    low_str = item[7].replace(",", "").strip()
                    low = float(low_str) if low_str and low_str != "--" else price

                    volume_str = item[2].replace(",", "").strip()
                    volume = int(volume_str) if volume_str else 0
                    volume = volume // 1000  # 股 -> 張

                    change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0

                    rows.append({
                        "stock_id": stock_id,
                        "stock_name": item[1].strip(),
                        "price": price,
                        "open": open_price,
                        "high": high,
                        "low": low,
                        "volume": volume,
                        "prev_close": prev_close,
                        "change_pct": round(change_pct, 2),
                        "market": "TWSE"
                    })
                except (ValueError, IndexError) as e:
                    continue

            return pd.DataFrame(rows)

        except Exception as e:
            logger.error(f"獲取上市股票即時報價失敗: {e}")
            return pd.DataFrame()

    def _fetch_tpex_realtime(self) -> pd.DataFrame:
        """從櫃買中心獲取上櫃股票即時報價"""
        # 民國年格式 (例: 115/01/05)
        now = datetime.now()
        roc_year = now.year - 1911
        date_str = f"{roc_year}/{now.strftime('%m/%d')}"
        url = f"https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
        params = {
            "l": "zh-tw",
            "d": date_str,
            "se": "EW",
            "_": int(datetime.now().timestamp() * 1000)
        }

        try:
            response = requests.get(url, params=params, timeout=30)
            data = response.json()

            # 新版 API 使用 tables 格式
            stock_data = None
            if "tables" in data and len(data["tables"]) > 0:
                stock_data = data["tables"][0].get("data", [])
            elif "aaData" in data:
                # 舊版 API 格式 (備用)
                stock_data = data["aaData"]

            if not stock_data:
                logger.warning("TPEx API 無資料")
                return pd.DataFrame()

            rows = []
            for item in stock_data:
                try:
                    stock_id = item[0].strip()
                    # 只取一般股票 (4碼數字)
                    if not stock_id.isdigit() or len(stock_id) != 4:
                        continue

                    # 新版 API 欄位順序:
                    # 0:代號, 1:名稱, 2:收盤, 3:漲跌, 4:開盤, 5:最高, 6:最低, 7:成交股數

                    price_str = str(item[2]).replace(",", "").strip()
                    if price_str == "--" or not price_str:
                        continue
                    price = float(price_str)

                    # 漲跌價差 (第3欄)
                    change_val_str = str(item[3]).replace(",", "").strip()
                    change_val = float(change_val_str) if change_val_str and change_val_str != "--" else 0
                    prev_close = price - change_val

                    open_str = str(item[4]).replace(",", "").strip()
                    open_price = float(open_str) if open_str and open_str != "--" else price

                    high_str = str(item[5]).replace(",", "").strip()
                    high = float(high_str) if high_str and high_str != "--" else price

                    low_str = str(item[6]).replace(",", "").strip()
                    low = float(low_str) if low_str and low_str != "--" else price

                    volume_str = str(item[7]).replace(",", "").strip()
                    volume = int(float(volume_str)) // 1000 if volume_str else 0  # 股 -> 張

                    change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0

                    rows.append({
                        "stock_id": stock_id,
                        "stock_name": item[1].strip(),
                        "price": price,
                        "open": open_price,
                        "high": high,
                        "low": low,
                        "volume": volume,
                        "prev_close": prev_close,
                        "change_pct": round(change_pct, 2),
                        "market": "TPEx"
                    })
                except (ValueError, IndexError) as e:
                    continue

            return pd.DataFrame(rows)

        except Exception as e:
            logger.error(f"獲取上櫃股票即時報價失敗: {e}")
            return pd.DataFrame()

    def get_historical_data(self, stock_id: str, days: int = 60) -> pd.DataFrame:
        """
        獲取歷史日K數據 (使用 FinMind)
        Returns: DataFrame with columns [date, open, high, low, close, volume]
        """
        cache_key = f"{stock_id}_{days}"
        if cache_key in self._hist_data_cache:
            return self._hist_data_cache[cache_key]

        try:
            from FinMind.data import DataLoader
            loader = DataLoader()

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")

            df = loader.taiwan_stock_daily(
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                return pd.DataFrame()

            df = df.rename(columns={
                "Trading_Volume": "volume",
                "open": "open",
                "max": "high",
                "min": "low",
                "close": "close"
            })

            df = df[["date", "open", "high", "low", "close", "volume"]].tail(days)
            self._hist_data_cache[cache_key] = df
            return df

        except Exception as e:
            logger.debug(f"獲取 {stock_id} 歷史數據失敗: {e}")
            return pd.DataFrame()

    def get_stock_info(self, stock_id: str) -> Dict:
        """
        獲取股票基本資訊 (市值、流通股數)
        使用 FinMind TaiwanStockInfo
        """
        if stock_id in self._stock_info_cache:
            return self._stock_info_cache[stock_id]

        try:
            from FinMind.data import DataLoader
            loader = DataLoader()

            # 獲取股票資訊
            df = loader.taiwan_stock_info()
            if df.empty:
                return {}

            row = df[df["stock_id"] == stock_id]
            if row.empty:
                return {}

            info = {
                "stock_id": stock_id,
                "stock_name": row.iloc[0].get("stock_name", ""),
                "industry": row.iloc[0].get("industry_category", ""),
            }
            self._stock_info_cache[stock_id] = info
            return info

        except Exception as e:
            logger.debug(f"獲取 {stock_id} 基本資訊失敗: {e}")
            return {}

    def get_market_cap_data(self) -> pd.DataFrame:
        """
        獲取所有股票市值資料
        使用 FinMind TaiwanStockMarketValue
        """
        try:
            from FinMind.data import DataLoader
            loader = DataLoader()

            # 獲取最近交易日的市值
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

            df = loader.taiwan_stock_market_value(
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                return pd.DataFrame()

            # 取最新日期的資料
            latest_date = df["date"].max()
            df = df[df["date"] == latest_date]

            # 市值轉為億元
            df["market_cap"] = df["market_value"] / 100_000_000
            return df[["stock_id", "market_cap"]]

        except Exception as e:
            logger.error(f"獲取市值資料失敗: {e}")
            return pd.DataFrame()

    def get_shares_outstanding(self) -> pd.DataFrame:
        """
        獲取所有股票流通股數
        使用 FinMind TaiwanStockSharesOutstanding
        """
        try:
            from FinMind.data import DataLoader
            loader = DataLoader()

            # 獲取最近的流通股數資料
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

            df = loader.taiwan_stock_shareholding(
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                # 備用方案: 使用股票資訊
                return pd.DataFrame()

            # 取最新日期
            latest_date = df["date"].max()
            df = df[df["date"] == latest_date]
            return df[["stock_id", "NumberOfSharesIssued"]]

        except Exception as e:
            logger.debug(f"獲取流通股數失敗: {e}")
            return pd.DataFrame()

    def get_benchmark_change(self) -> Optional[float]:
        """獲取大盤 (加權指數) 當日漲跌幅"""
        url = "https://www.twse.com.tw/exchangeReport/MI_INDEX"
        params = {
            "response": "json",
            "date": datetime.now().strftime("%Y%m%d"),
            "type": "IND"
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            if "data1" not in data:
                return None

            for item in data["data1"]:
                if "加權股價指數" in item[0]:
                    change_str = item[3].replace(",", "").strip()
                    prev_str = item[1].replace(",", "").strip()

                    if change_str and prev_str:
                        current = float(change_str)
                        # 計算漲跌幅需要昨收,這裡簡化處理
                        return float(item[2].replace(",", "")) if item[2] else 0

            return None

        except Exception as e:
            logger.error(f"獲取大盤漲跌幅失敗: {e}")
            return None

    def get_index_historical_data(self, index_type: str = "TWSE", days: int = 60) -> pd.DataFrame:
        """
        獲取大盤/OTC 指數歷史資料
        index_type: "TWSE" (加權指數) 或 "OTC" (櫃買指數)
        Returns: DataFrame with columns [date, close]
        使用 0050/0051 ETF 作為大盤/OTC 指數的代理
        """
        try:
            from FinMind.data import DataLoader
            loader = DataLoader()

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")

            # 使用 ETF 作為指數代理
            # 0050 元大台灣50 追蹤加權指數
            # 006201 元大富櫃50 追蹤櫃買指數
            if index_type == "TWSE":
                stock_id = "0050"
            else:
                stock_id = "006201"

            df = loader.taiwan_stock_daily(
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                return pd.DataFrame()

            df = df.rename(columns={"close": "close"})
            df = df[["date", "close"]].tail(days)
            return df

        except Exception as e:
            logger.debug(f"獲取 {index_type} 指數歷史數據失敗: {e}")
            return pd.DataFrame()

    def get_index_ma_status(self, index_type: str = "TWSE", ma_periods: List[int] = [5, 10, 20, 60]) -> Dict:
        """
        獲取大盤/OTC 指數均線狀態
        Returns: {
            "current_price": float,
            "ma_values": {5: float, 10: float, ...},
            "above_ma": {5: bool, 10: bool, ...},
            "is_bullish": bool,  # 是否多頭排列
            "broken_ma": List[int]  # 跌破的均線
        }
        """
        hist_data = self.get_index_historical_data(index_type, days=max(ma_periods) + 10)

        if hist_data.empty or len(hist_data) < max(ma_periods):
            return {}

        current_price = hist_data["close"].iloc[-1]
        ma_values = {}
        above_ma = {}
        broken_ma = []

        for period in ma_periods:
            if len(hist_data) >= period:
                ma_val = hist_data["close"].tail(period).mean()
                ma_values[period] = round(ma_val, 2)
                above_ma[period] = current_price >= ma_val
                if not above_ma[period]:
                    broken_ma.append(period)

        # 判斷多頭排列: 短均線 > 長均線
        is_bullish = True
        sorted_periods = sorted(ma_periods)
        for i in range(len(sorted_periods) - 1):
            short_ma = ma_values.get(sorted_periods[i], 0)
            long_ma = ma_values.get(sorted_periods[i + 1], 0)
            if short_ma < long_ma:
                is_bullish = False
                break

        return {
            "index_type": index_type,
            "current_price": round(current_price, 2),
            "ma_values": ma_values,
            "above_ma": above_ma,
            "is_bullish": is_bullish,
            "broken_ma": broken_ma
        }
