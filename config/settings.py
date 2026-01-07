"""
台股尾盤選股 - 全域設定檔
"""
import os
from datetime import time
from pathlib import Path

# 專案根目錄
PROJECT_ROOT = Path(__file__).parent.parent
DATA_OUTPUT_DIR = PROJECT_ROOT / "data" / "output"
LOG_DIR = PROJECT_ROOT / "logs"

# 確保目錄存在
DATA_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

# API 設定
FINMIND_API_TOKEN = os.getenv("FINMIND_API_TOKEN", "")

# 交易時間設定
MARKET_OPEN = time(9, 0)
MARKET_CLOSE = time(13, 30)
SCREENING_START = time(13, 0)   # 尾盤篩選開始時間

# 篩選參數
SCREENING_PARAMS = {
    # 步驟1: 漲幅範圍
    "price_change_min": 3.0,    # 最小漲幅 %
    "price_change_max": 100.0,  # 最大漲幅 % (無上限)

    # 步驟2: 量比門檻
    "volume_ratio_min": 1.0,    # 量比 > 1

    # 步驟3: 換手率範圍 (放寬)
    "turnover_rate_min": 1.0,   # 換手率 % (放寬，大型股換手率本來就低)
    "turnover_rate_max": 20.0,  # 上限放寬

    # 步驟4: 市值範圍 (億元) - 放寬，不排除大型股和小型成長股
    "market_cap_min": 20,       # 20億 (排除過小的)
    "market_cap_max": 50000,    # 5兆 (幾乎無上限)

    # 步驟5: 成交量放大判斷
    "volume_increase_days": 3,  # 連續 N 日成交量放大

    # 步驟6: 均線設定
    "short_ma_periods": [5, 10, 20],
    "long_ma_period": 60,

    # 步驟8: 尾盤創新高
    "intraday_high_threshold": 0.995,  # 接近當日最高價 99.5%

    # 新增: 法人持股/買超篩選
    "institutional_buy_days": 5,        # 法人連續買超天數
    "min_institutional_holding": 30,    # 最低法人持股比例 %
    "max_retail_holding": 50,           # 最高散戶持股比例 %

    # 新增: 基本面篩選
    "min_eps": 0,                       # 最低 EPS (排除虧損股)
    "min_revenue_growth": -10,          # 最低營收年增率 % (允許小幅衰退)

    # 新增: 外資連續買超訊號 (參考 stock-tw.aiinpocket.com 策略)
    "foreign_consecutive_buy_days": 3,  # 外資連續買超天數門檻
    "foreign_cost_max_premium": 5.0,    # 最大允許溢價幅度 (%)，超過視為過貴
    "foreign_cost_calculation_days": 60, # 計算外資平均成本的天數範圍
}
