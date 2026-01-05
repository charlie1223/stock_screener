"""
輸出模組 - 終端機顯示和 CSV 輸出
"""
import pandas as pd
from datetime import datetime
from pathlib import Path
import logging

from config.settings import DATA_OUTPUT_DIR

logger = logging.getLogger(__name__)


class TerminalDisplay:
    """終端機顯示器"""

    @staticmethod
    def display_results(df: pd.DataFrame):
        """在終端機顯示篩選結果"""
        print("\n" + "=" * 80)
        print(f"  台股尾盤選股結果 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)

        if df.empty:
            print("\n  今日無符合條件的股票")
            print("=" * 80)
            return

        # 準備顯示欄位
        display_columns = [
            "stock_id", "stock_name", "price", "change_pct",
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
            "price": "現價",
            "change_pct": "漲幅",
            "volume_ratio": "量比",
            "turnover_rate": "換手率",
            "market_cap": "市值"
        }
        display_df = display_df.rename(columns=column_names)

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

        print(f"\n  共篩選出 {len(df)} 檔股票符合八大條件")
        print("=" * 80)


class CSVExporter:
    """CSV 輸出器"""

    def __init__(self):
        self.output_dir = DATA_OUTPUT_DIR

    def export(self, df: pd.DataFrame, filename: str = None) -> str:
        """將篩選結果輸出為 CSV"""
        if df.empty:
            logger.warning("無資料可輸出")
            return None

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"screener_result_{timestamp}.csv"

        filepath = self.output_dir / filename

        # 選擇要輸出的欄位
        output_columns = [
            "stock_id", "stock_name", "price", "change_pct",
            "volume", "volume_ratio", "turnover_rate", "market_cap",
            "open", "high", "low", "prev_close", "market"
        ]
        available_cols = [c for c in output_columns if c in df.columns]
        output_df = df[available_cols].copy()

        # 輸出 CSV (UTF-8-BOM 確保 Excel 正確顯示中文)
        output_df.to_csv(filepath, index=False, encoding="utf-8-sig")

        logger.info(f"結果已儲存至: {filepath}")
        return str(filepath)
