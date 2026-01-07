"""
台股尾盤選股主程式
"""
import sys
import logging
from datetime import datetime
import argparse

# 設置 Python path
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

# 載入 .env 環境變數
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

from config.settings import SCREENING_START, MARKET_CLOSE
from src.pipeline import ScreeningPipeline
from src.output import TerminalDisplay, CSVExporter
from src.bullish_pool import BullishPoolTracker


def setup_logging(verbose: bool = False):
    """設置日誌"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%H:%M:%S"
    )


def is_trading_time() -> bool:
    """檢查是否在交易時段"""
    now = datetime.now().time()
    return SCREENING_START <= now <= MARKET_CLOSE


def is_weekday() -> bool:
    """檢查是否為工作日"""
    return datetime.now().weekday() < 5


def run_screener(force: bool = False, scan_pool: bool = False):
    """執行選股程式"""
    # 檢查時間
    if not force:
        if not is_weekday():
            logging.warning("今天不是交易日 (週末)")
            logging.info("使用 --force 參數可強制執行")
            return

        if not is_trading_time():
            logging.warning(
                f"當前時間 {datetime.now().strftime('%H:%M')} 不在尾盤篩選時段 "
                f"({SCREENING_START.strftime('%H:%M')}-{MARKET_CLOSE.strftime('%H:%M')})"
            )
            logging.info("使用 --force 參數可強制執行")
            return

    # 執行篩選
    pipeline = ScreeningPipeline()
    results = pipeline.run()

    # 顯示逐步篩選結果
    step_results = pipeline.get_step_results()
    if step_results:
        TerminalDisplay.display_step_results(step_results)

    # 獲取三大法人買賣超資料 (作為參考資訊)
    institutional_data = None
    if not results.empty:
        logging.info("正在獲取三大法人買賣超資料...")
        stock_ids = results["stock_id"].tolist()
        institutional_data = pipeline.data_fetcher.get_institutional_investors_batch(stock_ids, days=5)

        # 合併法人資料到結果
        if not institutional_data.empty:
            results = results.merge(institutional_data, on="stock_id", how="left")

    # 終端機顯示最終結果
    TerminalDisplay.display_results(results, institutional_data)

    # 輸出 CSV - 每一步的結果
    exporter = CSVExporter()
    if step_results:
        step_dir = exporter.export_step_results(step_results)
        if step_dir:
            print(f"\n逐步篩選結果已儲存至: {step_dir}")

    # 輸出最終結果 CSV
    if not results.empty:
        filepath = exporter.export(results)
        if filepath:
            print(f"最終結果已儲存至: {filepath}")

    # === 多頭股池追蹤 ===
    if scan_pool:
        run_bullish_pool_scan(pipeline.data_fetcher)


def run_bullish_pool_scan(data_fetcher=None):
    """執行多頭股池掃描"""
    print("\n" + "=" * 60)
    print("  開始掃描多頭股池...")
    print("=" * 60)

    tracker = BullishPoolTracker(data_fetcher)

    # 獲取所有股票
    stock_df = tracker.data_fetcher.get_all_stocks_realtime()
    if stock_df.empty:
        logging.warning("無法獲取股票清單")
        return

    # 掃描多頭股票
    bullish_df = tracker.scan_bullish_stocks(stock_df)

    # 更新股池並輸出報告
    update_result = tracker.update_pool(bullish_df)
    tracker.print_pool_report(update_result)

    # 輸出 CSV
    if not bullish_df.empty:
        from src.output import CSVExporter
        exporter = CSVExporter()
        date_dir = exporter._get_date_dir()
        filepath = date_dir / "bullish_pool.csv"
        bullish_df.to_csv(filepath, index=False, encoding="utf-8-sig")
        print(f"\n多頭股池已儲存至: {filepath}")


def main():
    """主程式入口"""
    parser = argparse.ArgumentParser(
        description="台股尾盤選股程式 - 八大步驟篩選 + 多頭股池追蹤",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
兩份報告:
  1. 【今日訊號】當日漲幅>=3% + 量比 + 均線多頭 + 法人買超
  2. 【多頭股池】體質追蹤 (均線多頭排列，不含當日漲幅條件)

範例:
  python -m src.main              # 正常執行今日訊號篩選
  python -m src.main --force      # 強制執行 (忽略時間檢查)
  python -m src.main -f --pool    # 執行今日訊號 + 多頭股池掃描
  python -m src.main --pool-only  # 只執行多頭股池掃描
        """
    )
    parser.add_argument(
        "-f", "--force",
        action="store_true",
        help="強制執行 (忽略時間檢查)"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="顯示詳細日誌"
    )
    parser.add_argument(
        "--pool",
        action="store_true",
        help="同時執行多頭股池掃描"
    )
    parser.add_argument(
        "--pool-only",
        action="store_true",
        help="只執行多頭股池掃描 (不執行今日訊號篩選)"
    )

    args = parser.parse_args()

    # 設置日誌
    setup_logging(verbose=args.verbose)

    print("\n" + "=" * 60)
    print("  台股選股程式 v2.0")
    print("  今日訊號 + 多頭股池追蹤")
    print("=" * 60 + "\n")

    # 執行
    try:
        if args.pool_only:
            # 只執行多頭股池掃描
            run_bullish_pool_scan()
        else:
            # 執行今日訊號篩選 (可選擇同時掃描股池)
            run_screener(force=args.force, scan_pool=args.pool)
    except KeyboardInterrupt:
        print("\n\n程式已中斷")
        sys.exit(0)
    except Exception as e:
        logging.error(f"執行錯誤: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
