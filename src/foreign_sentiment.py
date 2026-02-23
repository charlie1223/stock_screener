"""
外資動向分析模組 - 現貨 + 期貨交叉判讀大盤氛圍

判讀邏輯:
  現貨買超 + 期貨多單增加 = 絕對看多
  現貨買超 + 期貨空單增加 = 策略對沖
  現貨賣超 + 期貨空單增加 = 絕對看空
  現貨賣超 + 期貨多單增加 = 底部佈局
"""
import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)


class ForeignSentimentAnalyzer:
    """外資現貨+期貨動向分析器"""

    # 四種氛圍狀態
    SENTIMENT_BULLISH = "絕對看多"
    SENTIMENT_HEDGE = "策略對沖"
    SENTIMENT_BEARISH = "絕對看空"
    SENTIMENT_BOTTOM = "底部佈局"
    SENTIMENT_UNKNOWN = "資料不足"

    SENTIMENT_ICONS = {
        "絕對看多": "🟢",
        "策略對沖": "🟡",
        "絕對看空": "🔴",
        "底部佈局": "🔵",
        "資料不足": "⚪",
    }

    def __init__(self):
        self._token = os.getenv("FINMIND_API_TOKEN", "")

    def analyze(self) -> Dict:
        """
        執行外資動向分析
        Returns: {
            "sentiment": str,          # 氛圍判讀 (絕對看多/策略對沖/絕對看空/底部佈局)
            "icon": str,               # 氛圍圖示
            "spot_net": float,         # 現貨淨買超 (億元)
            "spot_direction": str,     # 現貨方向 (買超/賣超)
            "futures_oi_change": int,  # 期貨未平倉口數變化
            "futures_direction": str,  # 期貨方向 (多單增/空單增)
            "detail": str,             # 詳細說明
            "date": str,               # 資料日期
        }
        """
        logger.info("正在分析外資動向 (現貨+期貨)...")

        spot_data = self._fetch_spot_data()
        futures_data = self._fetch_futures_data()

        result = {
            "sentiment": self.SENTIMENT_UNKNOWN,
            "icon": self.SENTIMENT_ICONS[self.SENTIMENT_UNKNOWN],
            "spot_net": 0,
            "spot_direction": "N/A",
            "futures_oi_change": 0,
            "futures_direction": "N/A",
            "detail": "無法取得資料",
            "date": datetime.now().strftime("%Y-%m-%d"),
        }

        if not spot_data and not futures_data:
            logger.warning("無法取得外資現貨/期貨資料")
            return result

        # 現貨
        if spot_data:
            result["spot_net"] = spot_data["net_buy_billion"]
            result["spot_direction"] = "買超" if spot_data["net_buy_billion"] > 0 else "賣超"
            result["date"] = spot_data.get("date", result["date"])

        # 期貨
        if futures_data:
            result["futures_oi_change"] = futures_data["oi_change"]
            result["futures_direction"] = "多單增" if futures_data["oi_change"] > 0 else "空單增"

        # 交叉判讀
        if spot_data and futures_data:
            spot_buy = spot_data["net_buy_billion"] > 0
            futures_long = futures_data["oi_change"] > 0

            if spot_buy and futures_long:
                result["sentiment"] = self.SENTIMENT_BULLISH
            elif spot_buy and not futures_long:
                result["sentiment"] = self.SENTIMENT_HEDGE
            elif not spot_buy and not futures_long:
                result["sentiment"] = self.SENTIMENT_BEARISH
            else:
                result["sentiment"] = self.SENTIMENT_BOTTOM

            result["icon"] = self.SENTIMENT_ICONS[result["sentiment"]]

            spot_label = f"{'買超' if spot_buy else '賣超'} {abs(spot_data['net_buy_billion']):.1f}億"
            futures_label = f"{'多單增' if futures_long else '空單增'} {abs(futures_data['oi_change']):,}口"
            result["detail"] = f"現貨{spot_label} / 期貨{futures_label}"

        elif spot_data:
            result["detail"] = f"現貨{'買超' if spot_data['net_buy_billion'] > 0 else '賣超'} " \
                               f"{abs(spot_data['net_buy_billion']):.1f}億 (期貨資料不足)"
        elif futures_data:
            result["detail"] = f"期貨{'多單增' if futures_data['oi_change'] > 0 else '空單增'} " \
                               f"{abs(futures_data['oi_change']):,}口 (現貨資料不足)"

        logger.info(f"外資動向: {result['icon']} {result['sentiment']} - {result['detail']}")
        return result

    def _fetch_spot_data(self) -> Optional[Dict]:
        """
        抓取外資現貨買賣超 (整體市場)
        來源: TWSE 三大法人買賣金額統計表
        不帶日期參數，TWSE 會自動回傳最近交易日的資料
        Returns: {"net_buy_billion": float, "date": str}
        """
        try:
            url = "https://www.twse.com.tw/rwd/zh/fund/BFI82U"
            params = {"response": "json"}
            headers = {"User-Agent": "Mozilla/5.0"}

            response = requests.get(url, params=params, headers=headers, timeout=15)
            data = response.json()

            if data.get("stat") != "OK" or "data" not in data or not data["data"]:
                return None

            # 取得資料日期
            api_date = data.get("date", "")
            if api_date and len(api_date) == 8:
                formatted_date = f"{api_date[:4]}-{api_date[4:6]}-{api_date[6:8]}"
            else:
                formatted_date = datetime.now().strftime("%Y-%m-%d")

            # 找外資那一行
            # TWSE 格式: "外資及陸資(不含外資自營商)" → 要抓這行
            #            "外資自營商" → 不要這行
            for row in data["data"]:
                name = str(row[0]).strip()
                if "外資及陸資" in name or name == "外資(不含外資自營商)":
                    buy = int(str(row[1]).replace(",", ""))
                    sell = int(str(row[2]).replace(",", ""))
                    net = buy - sell  # 單位: 元

                    return {
                        "net_buy_billion": round(net / 1_0000_0000, 2),  # 轉億元
                        "buy_billion": round(buy / 1_0000_0000, 2),
                        "sell_billion": round(sell / 1_0000_0000, 2),
                        "date": formatted_date,
                    }

            return None

        except Exception as e:
            logger.debug(f"取得外資現貨資料失敗: {e}")
            return None

    def _fetch_futures_data(self) -> Optional[Dict]:
        """
        抓取外資期貨未平倉變化
        來源: FinMind TaiwanFuturesInstitutionalInvestors
              或 期交所官方 API
        Returns: {"oi_change": int, "oi_long": int, "oi_short": int, "date": str}
        """
        # 優先 FinMind
        result = self._fetch_futures_from_finmind()
        if result:
            return result

        # 備援: 期交所
        return self._fetch_futures_from_taifex()

    def _fetch_futures_from_finmind(self) -> Optional[Dict]:
        """從 FinMind 取得外資期貨未平倉"""
        try:
            end_date = datetime.now().strftime("%Y-%m-%d")
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

            url = "https://api.finmindtrade.com/api/v4/data"
            params = {
                "dataset": "TaiwanFuturesInstitutionalInvestors",
                "start_date": start_date,
                "end_date": end_date,
            }
            if self._token:
                params["token"] = self._token

            response = requests.get(url, params=params, timeout=15)
            result = response.json()

            if result.get("status") not in [200, "200"] or not result.get("data"):
                return None

            df = pd.DataFrame(result["data"])
            if df.empty:
                return None

            # 篩選台指期 (TX) + 外資
            tx_df = df[
                (df["name"].str.contains("外資", na=False)) &
                (df["contract_id"].str.contains("TX", na=False))
            ]

            if tx_df.empty:
                return None

            # 取最新兩天計算 OI 變化
            dates = sorted(tx_df["date"].unique())
            if len(dates) < 2:
                # 只有一天資料，看當天未平倉淨額
                latest = tx_df[tx_df["date"] == dates[-1]]
                oi_long = latest["open_interest_long"].sum()
                oi_short = latest["open_interest_short"].sum()
                return {
                    "oi_change": int(oi_long - oi_short),
                    "oi_long": int(oi_long),
                    "oi_short": int(oi_short),
                    "date": dates[-1],
                }

            latest = tx_df[tx_df["date"] == dates[-1]]
            prev = tx_df[tx_df["date"] == dates[-2]]

            latest_net = latest["open_interest_long"].sum() - latest["open_interest_short"].sum()
            prev_net = prev["open_interest_long"].sum() - prev["open_interest_short"].sum()

            return {
                "oi_change": int(latest_net - prev_net),
                "oi_long": int(latest["open_interest_long"].sum()),
                "oi_short": int(latest["open_interest_short"].sum()),
                "oi_net": int(latest_net),
                "date": dates[-1],
            }

        except Exception as e:
            logger.debug(f"FinMind 期貨資料失敗: {e}")
            return None

    def _fetch_futures_from_taifex(self) -> Optional[Dict]:
        """
        從期交所取得外資期貨未平倉 (備援)
        來源: 期交所「三大法人-區分各期貨契約」
        抓最近兩個交易日的外資台指期未平倉，計算淨額變化
        """
        try:
            from io import StringIO

            headers = {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
                "Referer": "https://www.taifex.com.tw/cht/3/futContractsDate",
            }

            # 抓最近 5 天 (跳過週末/假日，需要至少 2 個交易日的資料)
            oi_records = []
            for days_ago in range(5):
                target_date = datetime.now() - timedelta(days=days_ago)
                date_str = target_date.strftime("%Y/%m/%d")

                url = "https://www.taifex.com.tw/cht/3/futContractsDate"
                params = {
                    "queryType": "1",
                    "doQuery": "1",
                    "queryDate": date_str,
                    "commodityId": "TXF",
                }

                response = requests.get(url, params=params, headers=headers, timeout=15)
                if response.status_code != 200:
                    continue

                try:
                    dfs = pd.read_html(StringIO(response.text))
                except ValueError:
                    continue

                if not dfs:
                    continue

                df = dfs[0]
                # 找外資 + 臺股期貨那一行
                oi_net = self._parse_taifex_foreign_oi(df)
                if oi_net is not None:
                    oi_records.append({
                        "date": target_date.strftime("%Y-%m-%d"),
                        "oi_net": oi_net["oi_net"],
                        "oi_long": oi_net["oi_long"],
                        "oi_short": oi_net["oi_short"],
                    })

                # 有兩天資料就夠了
                if len(oi_records) >= 2:
                    break

            if not oi_records:
                return None

            latest = oi_records[0]

            if len(oi_records) >= 2:
                prev = oi_records[1]
                oi_change = latest["oi_net"] - prev["oi_net"]
            else:
                # 只有一天，用當天淨額
                oi_change = latest["oi_net"]

            return {
                "oi_change": oi_change,
                "oi_long": latest["oi_long"],
                "oi_short": latest["oi_short"],
                "oi_net": latest["oi_net"],
                "date": latest["date"],
            }

        except Exception as e:
            logger.debug(f"期交所期貨資料失敗: {e}")
            return None

    def _parse_taifex_foreign_oi(self, df: pd.DataFrame) -> Optional[Dict]:
        """
        解析期交所表格，找出外資台指期未平倉口數
        表格結構 (MultiIndex columns):
          [0] 序號 [1] 商品名稱 [2] 身份別
          [3-8] 交易口數: 多方口數/金額, 空方口數/金額, 淨額口數/金額
          [9-14] 未平倉: 多方口數/金額, 空方口數/金額, 淨額口數/金額
        """
        try:
            for idx, row in df.iterrows():
                vals = [str(v).strip() for v in row.values]
                # 找 "臺股期貨" + "外資" 的那一行
                if "臺股期貨" in vals[1] and "外資" in vals[2]:
                    oi_long = int(str(row.iloc[9]).replace(",", ""))
                    oi_short = int(str(row.iloc[11]).replace(",", ""))
                    oi_net = int(str(row.iloc[13]).replace(",", ""))
                    return {
                        "oi_long": oi_long,
                        "oi_short": oi_short,
                        "oi_net": oi_net,
                    }
        except (ValueError, IndexError) as e:
            logger.debug(f"解析期交所表格失敗: {e}")

        return None
