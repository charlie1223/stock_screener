"""
台股數據獲取模組
整合多資料來源: TWSE/TPEx API, FinMind
支援自動備援切換 (FinMind -> TWSE/TPEx 官方 API)
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
import logging
import time as time_module
import os

logger = logging.getLogger(__name__)


class DataFetcher:
    """
    統一數據獲取類
    支援多資料來源與自動備援:
    - 即時報價: TWSE/TPEx 官方 API (免費)
    - 歷史股價: FinMind -> TWSE/TPEx 官方 API (備援)
    - 法人買賣超: FinMind
    - 財報/股權: FinMind
    """

    def __init__(self):
        self._stock_info_cache = {}
        self._hist_data_cache = {}
        self._industry_cache = {}       # 產業分類快取
        self._finmind_available = True  # FinMind API 是否可用
        self._finmind_fail_count = 0    # 連續失敗次數
        self._max_fail_count = 3        # 超過此次數切換備援

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
        """從證交所獲取上市股票即時報價 (優先盤中API，備援盤後API)"""
        # 先嘗試盤中即時報價 API
        df = self._fetch_twse_intraday()
        if not df.empty:
            return df

        # 備援：盤後資料 API
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

    def _fetch_twse_intraday(self) -> pd.DataFrame:
        """從證交所獲取盤中即時報價 (mis.twse.com.tw)"""
        try:
            # 先獲取所有上市股票代碼
            stock_list_url = "https://www.twse.com.tw/rwd/zh/afterTrading/MI_INDEX?response=json&type=ALLBUT0999"
            resp = requests.get(stock_list_url, timeout=15)
            data = resp.json()

            # 取得股票代碼列表
            stock_ids = []
            if "tables" in data and len(data["tables"]) > 8:
                stock_data = data["tables"][8].get("data", [])
                for item in stock_data:
                    sid = item[0].strip()
                    if sid.isdigit() and len(sid) == 4:
                        stock_ids.append(sid)

            if not stock_ids:
                logger.debug("無法取得上市股票列表")
                return pd.DataFrame()

            # 分批查詢即時報價 (每次最多 50 檔)
            batch_size = 50
            all_rows = []

            for i in range(0, len(stock_ids), batch_size):
                batch = stock_ids[i:i + batch_size]
                ex_ch = "|".join([f"tse_{sid}.tw" for sid in batch])

                url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}"
                resp = requests.get(url, timeout=15)
                result = resp.json()

                if "msgArray" not in result:
                    continue

                for item in result["msgArray"]:
                    try:
                        stock_id = item.get("c", "")
                        if not stock_id or not stock_id.isdigit() or len(stock_id) != 4:
                            continue

                        # z: 成交價, o: 開盤價, h: 最高價, l: 最低價, y: 昨收
                        # v: 成交量(張), n: 股票名稱
                        price_str = item.get("z", "-")
                        if price_str == "-" or not price_str:
                            # 若無成交價，使用最佳買價
                            bid_str = item.get("b", "").split("_")[0]
                            price_str = bid_str if bid_str else item.get("y", "0")

                        price = float(price_str) if price_str and price_str != "-" else 0
                        if price <= 0:
                            continue

                        prev_close = float(item.get("y", "0") or "0")
                        open_price = float(item.get("o", "0") or "0") or price
                        high = float(item.get("h", "0") or "0") or price
                        low = float(item.get("l", "0") or "0") or price
                        volume = int(item.get("v", "0") or "0")

                        change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0

                        all_rows.append({
                            "stock_id": stock_id,
                            "stock_name": item.get("n", ""),
                            "price": price,
                            "open": open_price,
                            "high": high,
                            "low": low,
                            "volume": volume,
                            "prev_close": prev_close,
                            "change_pct": round(change_pct, 2),
                            "market": "TWSE"
                        })
                    except (ValueError, KeyError) as e:
                        continue

                time_module.sleep(0.2)  # 避免請求過快

            if all_rows:
                logger.info(f"盤中即時報價: 取得 {len(all_rows)} 檔上市股票")
                return pd.DataFrame(all_rows)

            return pd.DataFrame()

        except Exception as e:
            logger.debug(f"盤中即時報價 API 失敗: {e}")
            return pd.DataFrame()

    def _fetch_tpex_realtime(self) -> pd.DataFrame:
        """從櫃買中心獲取上櫃股票即時報價 (優先盤中API，備援盤後API)"""
        # 先嘗試盤中即時報價 API
        df = self._fetch_tpex_intraday()
        if not df.empty:
            return df

        # 備援：盤後資料 API
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

    def _fetch_tpex_intraday(self) -> pd.DataFrame:
        """從櫃買中心獲取盤中即時報價 (mis.twse.com.tw)"""
        try:
            # 先獲取所有上櫃股票代碼
            now = datetime.now()
            roc_year = now.year - 1911
            date_str = f"{roc_year}/{now.strftime('%m/%d')}"

            list_url = "https://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_result.php"
            params = {"l": "zh-tw", "d": date_str, "se": "EW"}
            resp = requests.get(list_url, params=params, timeout=15)
            data = resp.json()

            stock_ids = []
            if "aaData" in data:
                for item in data["aaData"]:
                    sid = item[0].strip()
                    if sid.isdigit() and len(sid) == 4:
                        stock_ids.append(sid)

            if not stock_ids:
                logger.debug("無法取得上櫃股票列表")
                return pd.DataFrame()

            # 分批查詢即時報價 (每次最多 50 檔)
            batch_size = 50
            all_rows = []

            for i in range(0, len(stock_ids), batch_size):
                batch = stock_ids[i:i + batch_size]
                ex_ch = "|".join([f"otc_{sid}.tw" for sid in batch])

                url = f"https://mis.twse.com.tw/stock/api/getStockInfo.jsp?ex_ch={ex_ch}"
                resp = requests.get(url, timeout=15)
                result = resp.json()

                if "msgArray" not in result:
                    continue

                for item in result["msgArray"]:
                    try:
                        stock_id = item.get("c", "")
                        if not stock_id or not stock_id.isdigit() or len(stock_id) != 4:
                            continue

                        price_str = item.get("z", "-")
                        if price_str == "-" or not price_str:
                            bid_str = item.get("b", "").split("_")[0]
                            price_str = bid_str if bid_str else item.get("y", "0")

                        price = float(price_str) if price_str and price_str != "-" else 0
                        if price <= 0:
                            continue

                        prev_close = float(item.get("y", "0") or "0")
                        open_price = float(item.get("o", "0") or "0") or price
                        high = float(item.get("h", "0") or "0") or price
                        low = float(item.get("l", "0") or "0") or price
                        volume = int(item.get("v", "0") or "0")

                        change_pct = ((price - prev_close) / prev_close * 100) if prev_close > 0 else 0

                        all_rows.append({
                            "stock_id": stock_id,
                            "stock_name": item.get("n", ""),
                            "price": price,
                            "open": open_price,
                            "high": high,
                            "low": low,
                            "volume": volume,
                            "prev_close": prev_close,
                            "change_pct": round(change_pct, 2),
                            "market": "TPEx"
                        })
                    except (ValueError, KeyError) as e:
                        continue

                time_module.sleep(0.2)

            if all_rows:
                logger.info(f"盤中即時報價: 取得 {len(all_rows)} 檔上櫃股票")
                return pd.DataFrame(all_rows)

            return pd.DataFrame()

        except Exception as e:
            logger.debug(f"上櫃盤中即時報價 API 失敗: {e}")
            return pd.DataFrame()

    def get_industry_classification(self) -> Dict[str, str]:
        """
        獲取所有股票的產業分類
        Returns: Dict[stock_id, industry_name]
        """
        if self._industry_cache:
            return self._industry_cache

        try:
            from bs4 import BeautifulSoup
            import re

            industry_map = {}

            # 上市股票產業分類
            logger.info("正在獲取產業分類資料...")
            url = 'https://isin.twse.com.tw/isin/C_public.jsp'
            for mode in [2, 4]:  # 2=上市, 4=上櫃
                params = {'strMode': mode}
                resp = requests.get(url, params=params, timeout=30)
                resp.encoding = 'MS950'

                soup = BeautifulSoup(resp.text, 'html.parser')
                rows = soup.find_all('tr')

                for row in rows:
                    cells = row.find_all('td')
                    if len(cells) >= 5:
                        code_name = cells[0].text.strip()
                        industry = cells[4].text.strip()

                        # 解析代碼 (格式: '1101　台泥')
                        match = re.match(r'(\d{4})\s+', code_name)
                        if match and industry:
                            stock_id = match.group(1)
                            industry_map[stock_id] = industry

                time_module.sleep(0.3)

            self._industry_cache = industry_map
            logger.info(f"產業分類: 共 {len(industry_map)} 檔股票")
            return industry_map

        except Exception as e:
            logger.error(f"獲取產業分類失敗: {e}")
            return {}

    def get_stock_industry(self, stock_id: str) -> str:
        """獲取單一股票的產業分類"""
        if not self._industry_cache:
            self.get_industry_classification()
        return self._industry_cache.get(stock_id, "未分類")

    def get_historical_data(self, stock_id: str, days: int = 60) -> pd.DataFrame:
        """
        獲取歷史日K數據
        優先使用 FinMind，失敗時自動切換到 TWSE/TPEx 官方 API
        Returns: DataFrame with columns [date, open, high, low, close, volume]
        """
        cache_key = f"{stock_id}_{days}"
        if cache_key in self._hist_data_cache:
            return self._hist_data_cache[cache_key]

        df = pd.DataFrame()

        # 優先嘗試 FinMind (如果可用)
        if self._finmind_available:
            df = self._get_historical_from_finmind(stock_id, days)

        # FinMind 失敗，嘗試 TWSE/TPEx 官方 API (備援)
        if df.empty:
            df = self._get_historical_from_twse(stock_id, days)

        if not df.empty:
            self._hist_data_cache[cache_key] = df

        return df

    def _get_historical_from_finmind(self, stock_id: str, days: int) -> pd.DataFrame:
        """從 FinMind 獲取歷史數據"""
        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")

            url = "https://api.finmindtrade.com/api/v4/data"
            params = {
                "dataset": "TaiwanStockPrice",
                "data_id": stock_id,
                "start_date": start_date,
                "end_date": end_date
            }
            token = os.getenv("FINMIND_API_TOKEN", "")
            if token:
                params["token"] = token

            response = requests.get(url, params=params, timeout=10)
            result = response.json()

            # 檢查 API 限制 (402 = 額度用完)
            if result.get("status") in [402, "402"]:
                logger.warning("FinMind API 額度已用完，切換至 TWSE/TPEx 官方 API 備援")
                self._finmind_fail_count += 1
                if self._finmind_fail_count >= self._max_fail_count:
                    self._finmind_available = False
                return pd.DataFrame()

            if result.get("status") not in [200, "200"] or "data" not in result:
                self._finmind_fail_count += 1
                return pd.DataFrame()

            df = pd.DataFrame(result["data"])
            if df.empty:
                return pd.DataFrame()

            # 成功，重置失敗計數
            self._finmind_fail_count = 0

            df = df.rename(columns={
                "Trading_Volume": "volume",
                "open": "open",
                "max": "high",
                "min": "low",
                "close": "close"
            })

            return df[["date", "open", "high", "low", "close", "volume"]].tail(days)

        except Exception as e:
            logger.debug(f"FinMind 獲取 {stock_id} 歷史數據失敗: {e}")
            self._finmind_fail_count += 1
            return pd.DataFrame()

    def _get_historical_from_twse(self, stock_id: str, days: int) -> pd.DataFrame:
        """從證交所/櫃買中心獲取歷史數據 (備援)"""
        try:
            # 計算需要獲取幾個月的資料
            months_needed = max(2, days // 20 + 1)
            all_data = []

            for i in range(months_needed):
                target_date = datetime.now() - timedelta(days=i * 30)
                year = target_date.year
                month = target_date.month

                # 嘗試上市 (TWSE)
                df = self._fetch_twse_monthly(stock_id, year, month)
                if df.empty:
                    # 嘗試上櫃 (TPEx)
                    df = self._fetch_tpex_monthly(stock_id, year, month)

                if not df.empty:
                    all_data.append(df)

                time_module.sleep(0.3)  # 避免請求過快

            if not all_data:
                return pd.DataFrame()

            result = pd.concat(all_data, ignore_index=True)
            result = result.drop_duplicates(subset=["date"]).sort_values("date")
            return result.tail(days)

        except Exception as e:
            logger.debug(f"TWSE/TPEx 獲取 {stock_id} 歷史數據失敗: {e}")
            return pd.DataFrame()

    def _fetch_twse_monthly(self, stock_id: str, year: int, month: int) -> pd.DataFrame:
        """從證交所獲取單月歷史資料"""
        try:
            date_str = f"{year}{month:02d}01"
            url = f"https://www.twse.com.tw/exchangeReport/STOCK_DAY"
            params = {
                "response": "json",
                "date": date_str,
                "stockNo": stock_id
            }

            response = requests.get(url, params=params, timeout=10)
            data = response.json()

            if "data" not in data or not data["data"]:
                return pd.DataFrame()

            rows = []
            for item in data["data"]:
                try:
                    # 民國年轉西元年
                    date_parts = item[0].split("/")
                    year_ce = int(date_parts[0]) + 1911
                    date_str = f"{year_ce}-{date_parts[1]}-{date_parts[2]}"

                    rows.append({
                        "date": date_str,
                        "open": float(item[3].replace(",", "")),
                        "high": float(item[4].replace(",", "")),
                        "low": float(item[5].replace(",", "")),
                        "close": float(item[6].replace(",", "")),
                        "volume": int(item[1].replace(",", ""))
                    })
                except (ValueError, IndexError):
                    continue

            return pd.DataFrame(rows)

        except Exception as e:
            logger.debug(f"TWSE 獲取 {stock_id} {year}/{month} 失敗: {e}")
            return pd.DataFrame()

    def _fetch_tpex_monthly(self, stock_id: str, year: int, month: int) -> pd.DataFrame:
        """從櫃買中心獲取單月歷史資料"""
        try:
            roc_year = year - 1911

            # 使用個股日成交資訊 API
            url = "https://www.tpex.org.tw/web/stock/aftertrading/daily_trading_info/st43_result.php"
            params = {
                "l": "zh-tw",
                "d": f"{roc_year}/{month:02d}/01",
                "stkno": stock_id,
                "_": int(datetime.now().timestamp() * 1000)
            }
            headers = {"User-Agent": "Mozilla/5.0"}

            response = requests.get(url, params=params, headers=headers, timeout=10)
            data = response.json()

            # 新版 API 使用 tables 格式
            stock_data = None
            if "tables" in data and len(data["tables"]) > 0:
                stock_data = data["tables"][0].get("data", [])
            elif "aaData" in data:
                stock_data = data["aaData"]

            if not stock_data:
                return pd.DataFrame()

            rows = []
            for item in stock_data:
                try:
                    # 民國年轉西元年
                    date_parts = str(item[0]).split("/")
                    year_ce = int(date_parts[0]) + 1911
                    date_str = f"{year_ce}-{date_parts[1]}-{date_parts[2]}"

                    # 欄位: 日期, 成交股數, 成交金額, 開盤, 最高, 最低, 收盤, 漲跌, 成交筆數
                    rows.append({
                        "date": date_str,
                        "open": float(str(item[3]).replace(",", "")),
                        "high": float(str(item[4]).replace(",", "")),
                        "low": float(str(item[5]).replace(",", "")),
                        "close": float(str(item[6]).replace(",", "")),
                        "volume": int(float(str(item[1]).replace(",", "")))
                    })
                except (ValueError, IndexError):
                    continue

            return pd.DataFrame(rows)

        except Exception as e:
            logger.debug(f"TPEx 獲取 {stock_id} {year}/{month} 失敗: {e}")
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

    def get_institutional_investors(self, stock_id: str, days: int = 5) -> Dict:
        """
        獲取三大法人買賣超資料
        Returns: {
            "foreign": {"today": int, "sum_days": int},  # 外資
            "investment_trust": {"today": int, "sum_days": int},  # 投信
            "dealer": {"today": int, "sum_days": int},  # 自營商
            "total": {"today": int, "sum_days": int}  # 合計
        }
        單位: 張
        """
        try:
            from FinMind.data import DataLoader
            loader = DataLoader()

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")

            df = loader.taiwan_stock_institutional_investors(
                stock_id=stock_id,
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                return {}

            # 取最近 N 天的資料
            df = df.tail(days * 3)  # 每天有多筆資料 (外資/投信/自營商)

            result = {
                "foreign": {"today": 0, "sum_days": 0},
                "investment_trust": {"today": 0, "sum_days": 0},
                "dealer": {"today": 0, "sum_days": 0},
                "total": {"today": 0, "sum_days": 0}
            }

            # 取得最新日期
            latest_date = df["date"].max()

            # 外資 (Foreign_Investor)
            foreign_df = df[df["name"].str.contains("外資", na=False)]
            if not foreign_df.empty:
                today_foreign = foreign_df[foreign_df["date"] == latest_date]["buy"].sum() - \
                               foreign_df[foreign_df["date"] == latest_date]["sell"].sum()
                result["foreign"]["today"] = int(today_foreign / 1000)  # 股 -> 張
                result["foreign"]["sum_days"] = int((foreign_df["buy"].sum() - foreign_df["sell"].sum()) / 1000)

            # 投信 (Investment_Trust)
            trust_df = df[df["name"].str.contains("投信", na=False)]
            if not trust_df.empty:
                today_trust = trust_df[trust_df["date"] == latest_date]["buy"].sum() - \
                             trust_df[trust_df["date"] == latest_date]["sell"].sum()
                result["investment_trust"]["today"] = int(today_trust / 1000)
                result["investment_trust"]["sum_days"] = int((trust_df["buy"].sum() - trust_df["sell"].sum()) / 1000)

            # 自營商 (Dealer)
            dealer_df = df[df["name"].str.contains("自營商", na=False)]
            if not dealer_df.empty:
                today_dealer = dealer_df[dealer_df["date"] == latest_date]["buy"].sum() - \
                              dealer_df[dealer_df["date"] == latest_date]["sell"].sum()
                result["dealer"]["today"] = int(today_dealer / 1000)
                result["dealer"]["sum_days"] = int((dealer_df["buy"].sum() - dealer_df["sell"].sum()) / 1000)

            # 合計
            result["total"]["today"] = result["foreign"]["today"] + result["investment_trust"]["today"] + result["dealer"]["today"]
            result["total"]["sum_days"] = result["foreign"]["sum_days"] + result["investment_trust"]["sum_days"] + result["dealer"]["sum_days"]

            return result

        except Exception as e:
            logger.debug(f"獲取 {stock_id} 三大法人買賣超失敗: {e}")
            return {}

    def get_institutional_investors_batch(self, stock_ids: List[str], days: int = 5) -> pd.DataFrame:
        """
        批次獲取多檔股票的三大法人買賣超資料
        Returns: DataFrame with columns [stock_id, foreign_today, foreign_sum, trust_today, trust_sum, total_today, total_sum]
        """
        rows = []
        for stock_id in stock_ids:
            data = self.get_institutional_investors(stock_id, days)
            if data:
                rows.append({
                    "stock_id": stock_id,
                    "foreign_today": data["foreign"]["today"],
                    "foreign_sum": data["foreign"]["sum_days"],
                    "trust_today": data["investment_trust"]["today"],
                    "trust_sum": data["investment_trust"]["sum_days"],
                    "dealer_today": data["dealer"]["today"],
                    "dealer_sum": data["dealer"]["sum_days"],
                    "total_today": data["total"]["today"],
                    "total_sum": data["total"]["sum_days"]
                })
            else:
                rows.append({
                    "stock_id": stock_id,
                    "foreign_today": 0, "foreign_sum": 0,
                    "trust_today": 0, "trust_sum": 0,
                    "dealer_today": 0, "dealer_sum": 0,
                    "total_today": 0, "total_sum": 0
                })

        return pd.DataFrame(rows)

    def get_shareholding_distribution(self, stock_id: str) -> Dict:
        """
        獲取股權分散表 - 法人持股比例 vs 散戶持股比例
        使用 FinMind TaiwanStockShareholding
        Returns: {
            "institutional_pct": float,  # 法人持股比例 (外資+投信+自營)
            "retail_pct": float,         # 散戶持股比例 (1-1000股持有人)
            "major_shareholders_pct": float  # 大股東持股比例
        }
        """
        try:
            import os

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")

            url = "https://api.finmindtrade.com/api/v4/data"
            params = {
                "dataset": "TaiwanStockShareholding",
                "data_id": stock_id,
                "start_date": start_date,
                "end_date": end_date
            }
            token = os.getenv("FINMIND_API_TOKEN", "")
            if token:
                params["token"] = token

            response = requests.get(url, params=params, timeout=10)
            result = response.json()

            if result.get("status") not in [200, "200"] or "data" not in result or not result["data"]:
                return {}

            df = pd.DataFrame(result["data"])
            if df.empty:
                return {}

            # 取最新日期的資料
            latest_date = df["date"].max()
            df = df[df["date"] == latest_date]

            # 計算散戶比例 (持股 1-1000 股的投資人)
            # FinMind 的 TaiwanStockShareholding 有 HoldingSharesLevel 欄位
            retail_levels = ["1-999", "1,000-5,000"]  # 小散戶
            retail_df = df[df["HoldingSharesLevel"].isin(retail_levels)]
            retail_pct = retail_df["percent"].sum() if not retail_df.empty else 0

            # 大股東比例 (持股 > 1000張)
            major_levels = ["400,001-600,000", "600,001-800,000", "800,001-1,000,000", "more than 1,000,001"]
            major_df = df[df["HoldingSharesLevel"].isin(major_levels)]
            major_pct = major_df["percent"].sum() if not major_df.empty else 0

            # 法人持股 = 100 - 散戶 - 中間層 (粗估)
            institutional_pct = max(0, 100 - retail_pct - (100 - retail_pct - major_pct) * 0.3)

            return {
                "institutional_pct": round(institutional_pct, 1),
                "retail_pct": round(retail_pct, 1),
                "major_shareholders_pct": round(major_pct, 1)
            }

        except Exception as e:
            logger.debug(f"獲取 {stock_id} 股權分散表失敗: {e}")
            return {}

    def get_fundamental_data(self, stock_id: str) -> Dict:
        """
        獲取基本面資料 - EPS、營收成長率
        Returns: {
            "eps": float,           # 近四季 EPS
            "revenue_growth": float # 營收年增率 %
        }
        """
        try:
            import os

            # 獲取財報資料 (EPS)
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=400)).strftime("%Y-%m-%d")

            url = "https://api.finmindtrade.com/api/v4/data"
            token = os.getenv("FINMIND_API_TOKEN", "")

            # 獲取 EPS (財務報表)
            params = {
                "dataset": "TaiwanStockFinancialStatements",
                "data_id": stock_id,
                "start_date": start_date,
                "end_date": end_date
            }
            if token:
                params["token"] = token

            response = requests.get(url, params=params, timeout=10)
            result = response.json()

            eps = 0
            if result.get("status") in [200, "200"] and result.get("data"):
                df = pd.DataFrame(result["data"])
                # 找 EPS 欄位
                eps_df = df[df["type"] == "EPS"]
                if not eps_df.empty:
                    # 取最近 4 季加總
                    eps = eps_df.tail(4)["value"].sum()

            # 獲取營收資料
            params["dataset"] = "TaiwanStockMonthRevenue"
            response = requests.get(url, params=params, timeout=10)
            result = response.json()

            revenue_growth = 0
            if result.get("status") in [200, "200"] and result.get("data"):
                df = pd.DataFrame(result["data"])
                if not df.empty and "revenue_year_growth_rate" in df.columns:
                    # 取最新一個月的年增率
                    revenue_growth = df.iloc[-1]["revenue_year_growth_rate"]
                elif not df.empty and "revenue" in df.columns:
                    # 手動計算年增率
                    if len(df) >= 13:
                        current = df.iloc[-1]["revenue"]
                        year_ago = df.iloc[-13]["revenue"]
                        if year_ago > 0:
                            revenue_growth = (current - year_ago) / year_ago * 100

            return {
                "eps": round(eps, 2),
                "revenue_growth": round(revenue_growth, 1)
            }

        except Exception as e:
            logger.debug(f"獲取 {stock_id} 基本面資料失敗: {e}")
            return {}

    def get_foreign_consecutive_buy(self, stock_id: str, days: int = 10) -> Dict:
        """
        獲取外資連續買超資訊
        Returns: {
            "consecutive_buy_days": int,  # 連續買超天數
            "total_buy_amount": int,      # 連續買超期間總買超張數
            "is_consecutive": bool,       # 是否連續買超 >= 3 天
            "daily_data": List[Dict]      # 每日買賣超資料
        }
        """
        try:
            import os

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")

            url = "https://api.finmindtrade.com/api/v4/data"
            params = {
                "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
                "data_id": stock_id,
                "start_date": start_date,
                "end_date": end_date
            }
            token = os.getenv("FINMIND_API_TOKEN", "")
            if token:
                params["token"] = token

            response = requests.get(url, params=params, timeout=10)
            result = response.json()

            if result.get("status") not in [200, "200"] or "data" not in result or not result["data"]:
                return {"consecutive_buy_days": 0, "total_buy_amount": 0, "is_consecutive": False, "daily_data": []}

            df = pd.DataFrame(result["data"])
            if df.empty:
                return {"consecutive_buy_days": 0, "total_buy_amount": 0, "is_consecutive": False, "daily_data": []}

            # 篩選外資資料 (FinMind API 使用英文 Foreign_Investor)
            foreign_df = df[df["name"].str.contains("Foreign_Investor|外資", na=False, regex=True)]
            if foreign_df.empty:
                return {"consecutive_buy_days": 0, "total_buy_amount": 0, "is_consecutive": False, "daily_data": []}

            # 按日期分組計算每日淨買超
            daily_net = foreign_df.groupby("date")[["buy", "sell"]].sum()
            daily_net = ((daily_net["buy"] - daily_net["sell"]) / 1000).sort_index()  # 股 -> 張

            # 取最近 N 天
            daily_net = daily_net.tail(days)
            daily_data = [{"date": d, "net_buy": int(v)} for d, v in daily_net.items()]

            # 計算連續買超天數 (從最新日期往前算)
            consecutive_days = 0
            total_buy = 0
            for net in reversed(daily_net.values):
                if net > 0:
                    consecutive_days += 1
                    total_buy += net
                else:
                    break

            return {
                "consecutive_buy_days": consecutive_days,
                "total_buy_amount": int(total_buy),
                "is_consecutive": consecutive_days >= 3,
                "daily_data": daily_data
            }

        except Exception as e:
            logger.debug(f"獲取 {stock_id} 外資連續買超資料失敗: {e}")
            return {"consecutive_buy_days": 0, "total_buy_amount": 0, "is_consecutive": False, "daily_data": []}

    def get_foreign_average_cost(self, stock_id: str, days: int = 60) -> Dict:
        """
        計算外資平均成本
        策略: 根據外資買超期間的成交均價估算成本
        Returns: {
            "avg_cost": float,           # 外資平均成本
            "current_price": float,      # 現價 (需外部傳入)
            "cost_ratio": float,         # 現價 / 平均成本
            "is_below_cost": bool,       # 是否低於成本 (打折股)
            "discount_pct": float        # 折價幅度 %
        }
        """
        try:
            import os

            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=days * 2)).strftime("%Y-%m-%d")

            # 獲取外資買賣超資料
            url = "https://api.finmindtrade.com/api/v4/data"
            token = os.getenv("FINMIND_API_TOKEN", "")

            params = {
                "dataset": "TaiwanStockInstitutionalInvestorsBuySell",
                "data_id": stock_id,
                "start_date": start_date,
                "end_date": end_date
            }
            if token:
                params["token"] = token

            response = requests.get(url, params=params, timeout=10)
            result = response.json()

            if result.get("status") not in [200, "200"] or "data" not in result:
                return {}

            inst_df = pd.DataFrame(result["data"])
            if inst_df.empty:
                return {}

            # 篩選外資 (FinMind API 使用英文 Foreign_Investor)
            foreign_df = inst_df[inst_df["name"].str.contains("Foreign_Investor|外資", na=False, regex=True)]
            if foreign_df.empty:
                return {}

            # 獲取股價資料
            hist_data = self.get_historical_data(stock_id, days=days)
            if hist_data.empty:
                return {}

            # 計算外資加權平均成本
            # 方法: 外資買超日 * 當日成交均價 的加權平均
            foreign_daily = foreign_df.groupby("date")[["buy", "sell"]].sum()
            foreign_daily = (foreign_daily["buy"] - foreign_daily["sell"]) / 1000  # 股 -> 張

            total_cost = 0
            total_shares = 0

            for date_str, net_buy in foreign_daily.items():
                if net_buy > 0:  # 只計算買超的日期
                    # 找對應日期的股價
                    price_row = hist_data[hist_data["date"] == date_str]
                    if not price_row.empty:
                        # 使用 (最高+最低)/2 作為當日成交均價
                        avg_price = (price_row.iloc[0]["high"] + price_row.iloc[0]["low"]) / 2
                        total_cost += avg_price * net_buy
                        total_shares += net_buy

            if total_shares <= 0:
                return {}

            avg_cost = total_cost / total_shares

            return {
                "avg_cost": round(avg_cost, 2),
                "total_shares_bought": int(total_shares),
                "calculation_days": days
            }

        except Exception as e:
            logger.debug(f"獲取 {stock_id} 外資平均成本失敗: {e}")
            return {}
