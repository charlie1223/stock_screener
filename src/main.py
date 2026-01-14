"""
台股回調縮量吸籌選股主程式
策略：回調縮量 + 守住支撐 + 法人悄悄建倉
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
from src.institutional_tracker import InstitutionalTracker


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


def run_institutional_scan(data_fetcher=None, stock_ids: list = None):
    """執行法人佈局掃描"""
    print("\n" + "=" * 60)
    print("  開始掃描法人佈局...")
    print("=" * 60)

    tracker = InstitutionalTracker(data_fetcher)

    # 如果沒有指定股票，從即時報價取得
    if stock_ids is None:
        stock_df = tracker.data_fetcher.get_all_stocks_realtime()
        if stock_df.empty:
            logging.warning("無法獲取股票清單")
            return
        stock_ids = stock_df["stock_id"].tolist()
    else:
        stock_df = tracker.data_fetcher.get_all_stocks_realtime()

    # 加入產業分類
    industry_map = tracker.data_fetcher.get_industry_classification()
    if industry_map:
        stock_df["industry"] = stock_df["stock_id"].map(industry_map).fillna("未分類")

    # 掃描法人連續買超的股票
    result_df = tracker.scan_quietly_buying_stocks(stock_ids, min_consecutive_days=3)

    # 輸出報告
    tracker.print_institutional_report(result_df, stock_df)

    # 更新追蹤歷史
    if not result_df.empty:
        tracker.update_tracking(result_df.to_dict("records"))

        # 輸出 CSV
        from src.output import CSVExporter
        exporter = CSVExporter()
        date_dir = exporter._get_date_dir()
        filepath = date_dir / "institutional_tracking.csv"

        # 合併股票資訊
        if not stock_df.empty:
            merge_cols = ["stock_id", "stock_name", "price", "change_pct"]
            if "industry" in stock_df.columns:
                merge_cols.insert(2, "industry")
            available_cols = [c for c in merge_cols if c in stock_df.columns]
            result_df = result_df.merge(
                stock_df[available_cols],
                on="stock_id",
                how="left"
            )

        # 調整欄位順序：stock_id, stock_name, industry 放最前面
        priority_cols = ["stock_id", "stock_name", "industry", "price", "change_pct"]
        other_cols = [c for c in result_df.columns if c not in priority_cols]
        final_cols = [c for c in priority_cols if c in result_df.columns] + other_cols
        result_df = result_df[final_cols]

        result_df.to_csv(filepath, index=False, encoding="utf-8-sig")
        print(f"\n法人佈局追蹤已儲存至: {filepath}")


def main():
    """主程式入口"""
    parser = argparse.ArgumentParser(
        description="台股回調縮量吸籌選股程式",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
策略說明:
  回調縮量吸籌 = 找出「回調但不破支撐 + 成交量萎縮 + 法人悄悄建倉」的股票

篩選流程:
  1. 市值 >= 50億 (排除小型股)
  2. 回調狀態 (跌破短期均線、守住長期均線、從高點回落5-20%)
  3. 連續縮量 (成交量萎縮)
  4. 均線支撐 (守住 MA20/MA60 且斜率向上)
  5. 換手率篩選 (確保流動性)
  6. 法人吸籌 (連續買超、穩定建倉)

範例:
  python -m src.main              # 正常執行
  python -m src.main --force      # 強制執行 (忽略時間檢查)
  python -m src.main -f --inst    # 執行篩選 + 法人佈局追蹤
  python -m src.main --inst-only  # 只執行法人佈局追蹤
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
        help="只執行多頭股池掃描"
    )
    parser.add_argument(
        "--inst",
        action="store_true",
        help="同時執行法人佈局追蹤"
    )
    parser.add_argument(
        "--inst-only",
        action="store_true",
        help="只執行法人佈局追蹤"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="執行所有報告 (今日訊號 + 多頭股池 + 法人佈局)"
    )

    args = parser.parse_args()

    # 設置日誌
    setup_logging(verbose=args.verbose)

    print("\n" + "=" * 60)
    print("  台股回調縮量吸籌選股程式 v3.0")
    print("  回調縮量 + 守住支撐 + 法人悄悄建倉")
    print("=" * 60 + "\n")

    # 執行
    try:
        if args.pool_only:
            # 只執行多頭股池掃描
            run_bullish_pool_scan()
        elif args.inst_only:
            # 只執行法人佈局追蹤
            run_institutional_scan()
        else:
            # 判斷是否執行額外報告
            scan_pool = args.pool or args.all
            scan_inst = args.inst or args.all

            # 執行今日訊號篩選
            run_screener(force=args.force, scan_pool=scan_pool)

            # 執行法人佈局追蹤
            if scan_inst:
                run_institutional_scan()

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
