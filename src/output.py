"""
輸出模組 - 終端機顯示和 CSV 輸出
"""
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import logging
import shutil

from config.settings import DATA_OUTPUT_DIR

logger = logging.getLogger(__name__)

# 資料保留天數
DATA_RETENTION_DAYS = 30


class TerminalDisplay:
    """終端機顯示器"""

    @staticmethod
    def display_step_results(step_results: dict, max_stocks_per_step: int = 20):
        """
        顯示每一步篩選的結果
        Args:
            step_results: 每一步篩選的結果字典 {step_number: {"name": str, "data": DataFrame}}
            max_stocks_per_step: 每步最多顯示的股票數量
        """
        print("\n" + "=" * 80)
        print(f"  逐步篩選結果 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        if not step_results:
            print("\n  無篩選結果")
            print("=" * 80)
            return

        for step_num in sorted(step_results.keys()):
            step_info = step_results[step_num]
            step_name = step_info["name"]
            df = step_info["data"]

            print(f"\n{'─' * 80}")
            print(f"  【步驟 {step_num}】{step_name}")
            print(f"{'─' * 80}")

            if df.empty:
                print("  無符合條件的股票")
                continue

            stock_count = len(df)
            print(f"  符合條件: {stock_count} 檔")

            # 準備顯示的資料
            display_df = df.head(max_stocks_per_step).copy()

            # 選擇顯示欄位 (加入產業)
            display_columns = ["stock_id", "stock_name", "industry", "price", "change_pct"]
            available_cols = [c for c in display_columns if c in display_df.columns]

            if not available_cols:
                # 如果沒有標準欄位，顯示前幾個欄位
                available_cols = list(display_df.columns)[:4]

            display_df = display_df[available_cols].copy()

            # 格式化數值
            if "change_pct" in display_df.columns:
                display_df["change_pct"] = display_df["change_pct"].apply(
                    lambda x: f"+{x:.2f}%" if pd.notna(x) and x >= 0 else (f"{x:.2f}%" if pd.notna(x) else "-")
                )
            if "price" in display_df.columns:
                display_df["price"] = display_df["price"].apply(
                    lambda x: f"{x:.2f}" if pd.notna(x) else "-"
                )

            # 重新命名欄位
            column_names = {
                "stock_id": "代號",
                "stock_name": "名稱",
                "industry": "產業",
                "price": "現價",
                "change_pct": "漲幅"
            }
            display_df = display_df.rename(columns=column_names)

            # 顯示產業分布統計
            if "industry" in df.columns:
                industry_counts = df["industry"].value_counts().head(5)
                if not industry_counts.empty:
                    print(f"  產業分布: {', '.join([f'{ind}({cnt})' for ind, cnt in industry_counts.items()])}")

            # 顯示表格
            try:
                from tabulate import tabulate
                print(tabulate(
                    display_df,
                    headers="keys",
                    tablefmt="simple",
                    showindex=False,
                    numalign="right",
                    stralign="left"
                ))
            except ImportError:
                print(display_df.to_string(index=False))

            if stock_count > max_stocks_per_step:
                print(f"  ... 還有 {stock_count - max_stocks_per_step} 檔未顯示")

        print("\n" + "=" * 80)

    @staticmethod
    def display_results(df: pd.DataFrame, institutional_data: pd.DataFrame = None):
        """
        在終端機顯示篩選結果
        Args:
            df: 篩選結果 DataFrame
            institutional_data: 三大法人買賣超資料 DataFrame (可選)
        """
        print("\n" + "=" * 80)
        print(f"  台股尾盤選股結果 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        if df.empty:
            print("\n  今日無符合條件的股票")
            print("=" * 80)
            return

        # 合併法人資料
        if institutional_data is not None and not institutional_data.empty:
            df = df.merge(institutional_data, on="stock_id", how="left")

        # 準備顯示欄位 (加入產業)
        display_columns = [
            "stock_id", "stock_name", "industry", "price", "change_pct",
            "volume_ratio", "turnover_rate", "market_cap"
        ]

        available_cols = [c for c in display_columns if c in df.columns]
        display_df = df[available_cols].copy()

        # 格式化數值
        if "change_pct" in display_df.columns:
            display_df["change_pct"] = display_df["change_pct"].apply(
                lambda x: f"+{x:.2f}%" if pd.notna(x) else "-"
            )
        if "volume_ratio" in display_df.columns:
            display_df["volume_ratio"] = display_df["volume_ratio"].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else "-"
            )
        if "turnover_rate" in display_df.columns:
            display_df["turnover_rate"] = display_df["turnover_rate"].apply(
                lambda x: f"{x:.2f}%" if pd.notna(x) else "-"
            )
        if "market_cap" in display_df.columns:
            display_df["market_cap"] = display_df["market_cap"].apply(
                lambda x: f"{x:.1f}億" if pd.notna(x) else "-"
            )
        if "price" in display_df.columns:
            display_df["price"] = display_df["price"].apply(
                lambda x: f"{x:.2f}" if pd.notna(x) else "-"
            )

        # 重新命名欄位為中文
        column_names = {
            "stock_id": "代號",
            "stock_name": "名稱",
            "industry": "產業",
            "price": "現價",
            "change_pct": "漲幅",
            "volume_ratio": "量比",
            "turnover_rate": "換手率",
            "market_cap": "市值"
        }
        display_df = display_df.rename(columns=column_names)

        # 顯示產業族群統計
        if "industry" in df.columns:
            print("\n  【產業族群分布】")
            industry_counts = df["industry"].value_counts()
            for ind in industry_counts.index:
                stocks_in_ind = df[df["industry"] == ind]["stock_name"].tolist()
                stock_names = ", ".join(stocks_in_ind[:5])
                if len(stocks_in_ind) > 5:
                    stock_names += f"... 等{len(stocks_in_ind)}檔"
                print(f"  {ind}: {stock_names}")
            print()

        # 嘗試使用 tabulate，如果沒有安裝則使用簡單格式
        try:
            from tabulate import tabulate
            print(tabulate(
                display_df,
                headers="keys",
                tablefmt="simple",
                showindex=False,
                numalign="right",
                stralign="left"
            ))
        except ImportError:
            # 簡單表格格式
            print("\n" + display_df.to_string(index=False))

        print(f"\n  共篩選出 {len(df)} 檔股票符合條件")
        print("=" * 80)

        # 顯示三大法人資訊
        if institutional_data is not None and not institutional_data.empty:
            TerminalDisplay._display_institutional_info(df)

    @staticmethod
    def _display_institutional_info(df: pd.DataFrame):
        """顯示三大法人買賣超資訊"""
        # 檢查是否有法人資料欄位
        if "foreign_today" not in df.columns:
            return

        print("\n" + "-" * 80)
        print("  【三大法人買賣超參考】 (單位: 張)")
        print("-" * 80)
        print(f"  {'代號':<8} {'名稱':<12} {'外資今日':>10} {'外資5日':>10} {'投信今日':>10} {'投信5日':>10} {'合計':>10}")
        print("-" * 80)

        for _, row in df.iterrows():
            stock_id = row.get("stock_id", "")
            stock_name = row.get("stock_name", "")[:6]  # 截斷名稱

            foreign_today = row.get("foreign_today", 0)
            foreign_sum = row.get("foreign_sum", 0)
            trust_today = row.get("trust_today", 0)
            trust_sum = row.get("trust_sum", 0)
            total_sum = row.get("total_sum", 0)

            # 格式化數字，正數加 + 號
            def fmt(x):
                if pd.isna(x) or x == 0:
                    return "0"
                return f"+{int(x):,}" if x > 0 else f"{int(x):,}"

            print(f"  {stock_id:<8} {stock_name:<12} {fmt(foreign_today):>10} {fmt(foreign_sum):>10} "
                  f"{fmt(trust_today):>10} {fmt(trust_sum):>10} {fmt(total_sum):>10}")

        print("-" * 80)
        print("  說明: 外資/投信連續買超通常代表法人看好")
        print("=" * 80)


class CSVExporter:
    """CSV 輸出器 - 按日期歸檔，自動清理超過30天的資料"""

    def __init__(self):
        self.output_dir = DATA_OUTPUT_DIR
        # 執行時自動清理舊資料
        self.cleanup_old_data()

    def _get_date_dir(self) -> Path:
        """取得今日日期資料夾路徑"""
        today = datetime.now().strftime("%Y%m%d")
        date_dir = self.output_dir / today
        date_dir.mkdir(parents=True, exist_ok=True)
        return date_dir

    def cleanup_old_data(self):
        """清理超過保留天數的資料"""
        if not self.output_dir.exists():
            return

        cutoff_date = datetime.now() - timedelta(days=DATA_RETENTION_DAYS)
        deleted_count = 0

        for item in self.output_dir.iterdir():
            # 只處理日期格式的資料夾 (YYYYMMDD)
            if item.is_dir() and len(item.name) == 8 and item.name.isdigit():
                try:
                    folder_date = datetime.strptime(item.name, "%Y%m%d")
                    if folder_date < cutoff_date:
                        shutil.rmtree(item)
                        deleted_count += 1
                        logger.debug(f"已刪除過期資料夾: {item.name}")
                except ValueError:
                    continue

        if deleted_count > 0:
            logger.info(f"已清理 {deleted_count} 個超過 {DATA_RETENTION_DAYS} 天的資料夾")

    def export(self, df: pd.DataFrame, filename: str = None, mode: str = "left") -> str:
        """
        將篩選結果輸出為 CSV (存放在日期資料夾)
        Args:
            df: 篩選結果 DataFrame
            filename: 自訂檔名 (可選)
            mode: 策略模式 ("left" 或 "right")
        """
        if df.empty:
            logger.warning("無資料可輸出")
            return None

        date_dir = self._get_date_dir()

        if filename is None:
            timestamp = datetime.now().strftime("%H%M%S")
            mode_label = "left" if mode == "left" else "right"
            filename = f"screener_{mode_label}_{timestamp}.csv"

        filepath = date_dir / filename

        # 選擇要輸出的欄位 (加入量價狀態欄位)
        output_columns = [
            "stock_id", "stock_name", "industry", "price", "change_pct",
            "volume", "volume_ratio", "turnover_rate", "market_cap",
            "open", "high", "low", "prev_close", "market",
            # 量價健康度欄位
            "vp_status", "vp_info", "vp_volume_ratio",
            # 回調狀態欄位
            "pullback_info", "pullback_pct", "support_distance",
            # RSI 欄位
            "rsi", "rsi_info",
            # 籌碼面欄位
            "holder_info", "major_holder_pct", "accumulation_info",
            # 底底高欄位
            "higher_lows_info", "higher_lows_confirms",
            # 右側策略欄位
            "rank", "relative_strength", "intraday_strong",
            # 三大法人欄位
            "foreign_today", "foreign_sum", "trust_today", "trust_sum",
            "dealer_today", "dealer_sum", "total_today", "total_sum"
        ]
        available_cols = [c for c in output_columns if c in df.columns]
        output_df = df[available_cols].copy()

        # 輸出 CSV (UTF-8-BOM 確保 Excel 正確顯示中文)
        output_df.to_csv(filepath, index=False, encoding="utf-8-sig")

        logger.info(f"結果已儲存至: {filepath}")
        return str(filepath)

    def export_step_results(self, step_results: dict, mode: str = "left") -> str:
        """
        將每一步篩選結果輸出為 CSV (存放在日期資料夾下的 steps 子資料夾)
        Args:
            step_results: 每一步篩選的結果字典 {step_number: {"name": str, "data": DataFrame}}
            mode: 策略模式 ("left" 或 "right")
        Returns:
            輸出的資料夾路徑
        """
        if not step_results:
            logger.warning("無篩選結果可輸出")
            return None

        date_dir = self._get_date_dir()
        timestamp = datetime.now().strftime("%H%M%S")

        # 建立 steps 子資料夾，加入策略模式標記
        mode_label = "left" if mode == "left" else "right"
        step_dir = date_dir / f"steps_{mode_label}_{timestamp}"
        step_dir.mkdir(parents=True, exist_ok=True)

        # 選擇要輸出的欄位 (加入量價狀態欄位)
        output_columns = [
            "stock_id", "stock_name", "industry", "price", "change_pct",
            "volume", "volume_ratio", "turnover_rate", "market_cap",
            "open", "high", "low", "prev_close", "market",
            # 量價健康度欄位
            "vp_status", "vp_info", "vp_volume_ratio",
            # 回調狀態欄位
            "pullback_info", "pullback_pct", "support_distance",
            # RSI 欄位
            "rsi", "rsi_info",
            # 籌碼面欄位
            "holder_info", "major_holder_pct", "accumulation_info",
            # 底底高欄位
            "higher_lows_info", "higher_lows_confirms",
        ]

        exported_files = []

        for step_num in sorted(step_results.keys()):
            step_info = step_results[step_num]
            step_name = step_info["name"]
            df = step_info["data"]

            if df.empty:
                continue

            # 檔名格式: step_01_漲幅3%-5%.csv
            safe_name = step_name.replace("/", "-").replace(" ", "_").replace("<", "").replace(">", "")
            filename = f"step_{step_num:02d}_{safe_name}.csv"
            filepath = step_dir / filename

            available_cols = [c for c in output_columns if c in df.columns]
            output_df = df[available_cols].copy()

            # 輸出 CSV (UTF-8-BOM 確保 Excel 正確顯示中文)
            output_df.to_csv(filepath, index=False, encoding="utf-8-sig")
            exported_files.append(filename)

        if exported_files:
            logger.info(f"逐步篩選結果已儲存至: {step_dir}")
            logger.info(f"共輸出 {len(exported_files)} 個檔案")
            return str(step_dir)

        return None
