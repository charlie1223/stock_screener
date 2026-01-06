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

# 篩選參數 (八大步驟)
SCREENING_PARAMS = {
    # 步驟1: 漲幅範圍
    "price_change_min": 3.0,    # 最小漲幅 %
    "price_change_max": 100.0,  # 最大漲幅 % (改為無上限)

    # 步驟2: 量比門檻
    "volume_ratio_min": 1.0,    # 量比 > 1

    # 步驟3: 換手率範圍
    "turnover_rate_min": 5.0,   # 換手率 %
    "turnover_rate_max": 10.0,

    # 步驟4: 市值範圍 (億元)
    "market_cap_min": 50,       # 50億
    "market_cap_max": 200,      # 200億

    # 步驟5: 成交量放大判斷
    "volume_increase_days": 3,  # 連續 N 日成交量放大

    # 步驟6: 均線設定
    "short_ma_periods": [5, 10, 20],
    "long_ma_period": 60,

    # 步驟8: 尾盤創新高
    "intraday_high_threshold": 0.995,  # 接近當日最高價 99.5%
}
