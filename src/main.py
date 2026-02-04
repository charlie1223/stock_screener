"""
台股選股主程式
支援兩種策略模式:
  左側 (left)  = 回調縮量吸籌 → 還沒漲就先買
  右側 (right) = 撒網抓強勢   → 已經在漲才追，留強砍弱
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
from src.notifier import get_notifier


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


def run_screener(force: bool = False, scan_pool: bool = False, mode: str = "left"):
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
    pipeline = ScreeningPipeline(mode=mode)
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

    # 右側策略: 額外顯示漲幅排名
    if mode == "right" and not results.empty and "rank" in results.columns:
        print("\n" + "=" * 60)
        print("  漲幅排名 (留強砍弱參考)")
        print("=" * 60)
        for _, row in results.iterrows():
            name = row.get("stock_name", "")
            sid = row.get("stock_id", "")
            chg = row.get("change_pct", 0)
            rank = row.get("rank", 0)
            print(f"  #{rank:<3} {sid} {name:<8} 漲幅 {chg:+.1f}%")
        print("=" * 60)

    # 輸出 CSV - 每一步的結果 (加入策略模式標記)
    exporter = CSVExporter()
    if step_results:
        step_dir = exporter.export_step_results(step_results, mode=mode)
        if step_dir:
            print(f"\n逐步篩選結果已儲存至: {step_dir}")

    # 輸出最終結果 CSV (加入策略模式標記)
    if not results.empty:
        filepath = exporter.export(results, mode=mode)
        if filepath:
            print(f"最終結果已儲存至: {filepath}")

    # === Discord 通知 ===
    notifier = get_notifier()
    if notifier.enabled:
        logging.info("正在發送 Discord 通知...")
        # 發送逐步篩選摘要
        if step_results:
            notifier.send_step_summary(step_results)
        # 發送最終選股結果
        notifier.send_screening_results(results, pipeline.strategy_name)

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
        description="台股選股程式 - 支援左側/右側兩種策略",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
策略模式:
  --mode left  (預設) 左側: 回調縮量吸籌 = 還沒漲就先買
    1. 市值篩選  2. 營收成長  3. 本益比  4. 底底高確認
    5. 回調狀態  6. 量價健康  7. 連續縮量  8. RSI超賣回升
    9. 換手率  10. 大戶持股  11. 法人吸籌
    (量價健康度: 排除竭盡量，保留健康量/換手量)

  --mode right 右側: 撒網抓強勢 = 已經在漲才追，留強砍弱
    1. 市值篩選  2. 漲幅>=3%  3. 量比>1.5
    4. 均線多頭  5. 強於大盤  6. 尾盤創新高
    結果按漲幅排名，方便決定留誰砍誰

範例:
  python -m src.main -f              # 左側策略 (預設)
  python -m src.main -f --mode right # 右側策略
  python -m src.main -f --inst       # 左側 + 法人佈局追蹤
  python -m src.main --inst-only     # 只執行法人佈局追蹤
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
        "--mode",
        choices=["left", "right"],
        default="left",
        help="策略模式: left=回調吸籌(預設), right=撒網抓強勢"
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

    # 顯示標題
    from src.pipeline import STRATEGY_NAMES
    mode_name = STRATEGY_NAMES.get(args.mode, STRATEGY_NAMES["left"])
    print("\n" + "=" * 60)
    print(f"  台股選股程式 - {mode_name}")
    if args.mode == "left":
        print("  基本面 + 技術面 + 籌碼面 多維度篩選")
    else:
        print("  爆量突破 + 均線多頭 + 強於大盤 → 留強砍弱")
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
            run_screener(force=args.force, scan_pool=scan_pool, mode=args.mode)

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
