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

# 篩選參數 - 回調縮量吸籌策略 (增強版)
SCREENING_PARAMS = {
    # ========================================
    # 步驟1: 市值篩選 (快速排除)
    # ========================================
    "market_cap_min": 50,       # 50億 (排除小型股，流動性較差)
    "market_cap_max": 50000,    # 5兆 (幾乎無上限)

    # ========================================
    # 步驟2: 營收成長篩選 (基本面)
    # ========================================
    "revenue_growth_min": 0,              # 最低營收年增率 % (0 = 正成長)
    "revenue_months_positive": 2,         # 近N個月營收需正成長

    # ========================================
    # 步驟3: 本益比篩選 (估值面)
    # ========================================
    "pe_ratio_min": 0,                    # 最低本益比 (排除虧損股)
    "pe_ratio_max": 20,                   # 最高本益比 (價值股篩選)

    # ========================================
    # 步驟4: 回調狀態偵測 (技術面)
    # ========================================
    "pullback_min_pct": 5.0,              # 從高點回落最小幅度 %
    "pullback_max_pct": 20.0,             # 從高點回落最大幅度 %
    "pullback_high_lookback_days": 20,    # 尋找近期高點的天數
    "pullback_short_ma": [5, 10],         # 短期均線 (需跌破其中之一)
    "pullback_long_ma": [20, 60],         # 長期均線 (需守住其中之一)

    # ========================================
    # 步驟5: 連續縮量偵測 (技術面)
    # ========================================
    "volume_shrink_days": 3,              # 連續縮量天數
    "volume_shrink_threshold": 0.7,       # 當前量需低於均量的比例 (70%)
    "volume_avg_days": 20,                # 計算均量的天數

    # ========================================
    # 步驟6: 均線支撐偵測 (技術面)
    # ========================================
    "ma_support_periods": [20, 60],       # 支撐均線
    "ma_support_tolerance": 0.02,         # 允許跌破支撐的比例 (2%)
    "ma_slope_lookback_days": 5,          # 計算斜率的回看天數

    # ========================================
    # 步驟7: RSI 超賣篩選 (技術面)
    # ========================================
    "rsi_period": 14,                     # RSI 計算週期
    "rsi_oversold": 35,                   # RSI 超賣門檻 (低於此值為超賣)

    # ========================================
    # 步驟8: 換手率篩選 (流動性)
    # ========================================
    "turnover_rate_min": 0.5,             # 換手率 % (放寬，回調時換手率本來就低)
    "turnover_rate_max": 20.0,            # 上限

    # ========================================
    # 步驟9: 大戶持股篩選 (籌碼面)
    # ========================================
    "major_holder_min_pct": 30,           # 千張大戶最低持股比例 %
    "major_holder_increase_weeks": 1,     # 大戶持股需增加的週數

    # ========================================
    # 步驟10: 法人吸籌偵測 (籌碼面)
    # ========================================
    "accumulation_min_days": 3,           # 法人連續買超最少天數
    "accumulation_max_stability": 2.0,    # 最大穩定度 (越小越穩定)
}
