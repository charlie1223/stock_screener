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
    "revenue_months_positive": 1,         # 近N個月營收需正成長 (放寬: 2→1)

    # ========================================
    # 步驟3: 本益比篩選 (估值面)
    # ========================================
    "pe_ratio_min": 0,                    # 最低本益比 (排除虧損股)
    "pe_ratio_max": 35,                   # 最高本益比 (放寬至35，適合科技/半導體股)

    # ========================================
    # 步驟4: 底底高確認 (趨勢確認)
    # ========================================
    "higher_lows_lookback_days": 60,          # 回看天數 (找波段低點的範圍)
    "higher_lows_window": 5,                  # local low 判定窗口 (前後各5根K棒)
    "higher_lows_min_confirms": 1,            # 最少需要幾次底底高確認 (放寬: 2→1)
    "higher_lows_tolerance_pct": 1.0,         # 容許誤差 % (允許低點差距在1%內仍算持平)

    # ========================================
    # 步驟5: 回調狀態偵測 (技術面) - 已合併均線支撐邏輯
    # ========================================
    "pullback_min_pct": 3.0,              # 從高點回落最小幅度 % (放寬: 5→3)
    "pullback_max_pct": 20.0,             # 從高點回落最大幅度 %
    "pullback_high_lookback_days": 20,    # 尋找近期高點的天數
    "pullback_short_ma": [5, 10],         # 短期均線 (需跌破其中之一)
    "pullback_long_ma": [20, 60],         # 長期均線 (需守住其中之一)
    "ma_support_tolerance": 0.03,         # 允許跌破支撐的比例 (3%)
    "ma_slope_lookback_days": 5,          # 計算斜率的回看天數

    # ========================================
    # 步驟6: 量價健康度篩選 (技術面)
    # ========================================
    # 量價狀態分類：
    #   健康量 = 創新高 + 小量/持平量，回調時縮量 → 趨勢延續訊號
    #   換手量 = 創新高 + 2-4倍量，看收盤位置 → 中性
    #   竭盡量 = 暴漲 + 區間最大量 → 可能見頂訊號，排除
    "healthy_volume_ratio_max": 1.5,      # 健康量: 量不超過均量的 1.5 倍
    "turnover_volume_ratio_min": 2.0,     # 換手量: 量是均量的 2 倍以上
    "turnover_volume_ratio_max": 4.0,     # 換手量: 量不超過均量的 4 倍
    "exhaustion_lookback_days": 20,       # 竭盡量判定: 回看天數
    "exhaustion_price_change_min": 5.0,   # 竭盡量判定: 當日漲幅門檻 %
    "volume_avg_days": 20,                # 計算均量的天數

    # ========================================
    # 步驟7: 連續縮量偵測 (技術面)
    # ========================================
    "volume_shrink_days": 3,              # 連續縮量天數
    "volume_shrink_threshold": 0.7,       # 當前量需低於均量的比例 (70%)

    # ========================================
    # 步驟8: RSI 超賣篩選 (技術面)
    # ========================================
    "rsi_period": 14,                     # RSI 計算週期
    "rsi_oversold": 45,                   # RSI 超賣門檻 (放寬: 35→45，偏弱勢即可)
    "rsi_require_upturn": True,           # 是否要求 RSI 觸底回升 (今日 > 昨日)
    "rsi_require_above_ma5": False,       # 是否要求收盤價站回 MA5 (放寬: 取消，只留回升確認)

    # ========================================
    # 步驟9: 換手率篩選 (流動性)
    # ========================================
    "turnover_rate_min": 0.5,             # 換手率 % (放寬，回調時換手率本來就低)
    "turnover_rate_max": 20.0,            # 上限

    # ========================================
    # 步驟10: 大戶持股篩選 (籌碼面)
    # ========================================
    "major_holder_min_pct": 20,           # 千張大戶最低持股比例 % (放寬: 30→20)
    "major_holder_increase_weeks": 1,     # 大戶持股需增加的週數

    # ========================================
    # 步驟11: 法人吸籌偵測 (籌碼面)
    # ========================================
    "accumulation_min_days": 2,           # 法人連續買超最少天數 (放寬: 3→2)
    "accumulation_max_stability": 2.0,    # 最大穩定度 (越小越穩定)

    # ========================================
    # 右側策略: 撒網抓強勢 (追漲模式)
    # 用法: python -m src.main --force --mode right
    # ========================================
    "price_change_min": 3.0,              # 當日最低漲幅 % (確認已在漲)
    "price_change_max": 10.0,             # 當日最高漲幅 % (排除漲停鎖死)
    "volume_ratio_min": 1.5,              # 量比門檻 (爆量確認)
    "short_ma_periods": [5, 10],          # 短期均線 (多頭排列用)
    "long_ma_period": 60,                 # 長期均線 (多頭排列用)
    "volume_increase_days": 3,            # 成交量持續放大天數
    "intraday_high_threshold": 0.97,      # 現價接近日高的比例 (97%)
}
