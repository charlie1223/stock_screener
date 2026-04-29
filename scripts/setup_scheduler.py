#!/usr/bin/env python3
"""
設置定時執行任務 (macOS launchd)

排程項目：
1. 盤中篩選-左側 (13:00, 13:20) - 回調縮量吸籌
2. 盤中篩選-右側 (13:05, 13:25) - 撒網抓強勢 (含散戶警示)
3. 盤後追蹤 (14:30) - 法人佈局追蹤
"""
import os
import sys
from pathlib import Path
import subprocess

PROJECT_ROOT = Path(__file__).parent.parent

# 三個排程任務
PLIST_SCREENER_LEFT = "com.stockscreener.left.plist"
PLIST_SCREENER_RIGHT = "com.stockscreener.right.plist"
PLIST_INSTITUTIONAL = "com.stockscreener.institutional.plist"

# 舊版單一篩選任務 (向下相容用，install 時會清掉)
PLIST_SCREENER_LEGACY = "com.stockscreener.plist"

PLIST_DIR = Path.home() / "Library" / "LaunchAgents"


def get_python_path() -> str:
    """獲取 Python 路徑"""
    venv_python = PROJECT_ROOT / "venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def _build_screener_plist(label: str, mode: str, hour_minutes: list, log_basename: str) -> str:
    """
    生成盤中篩選 plist 內容
    Args:
        label: launchd Label
        mode: 'left' 或 'right'
        hour_minutes: [(hour, minute), ...] 觸發時間
        log_basename: 日誌檔名 (不含副檔名)
    """
    python_path = get_python_path()

    # 產生週一到週五 x 各時間點的 StartCalendarInterval 區塊
    intervals = []
    for hour, minute in hour_minutes:
        for weekday in range(1, 6):  # 週一=1, 週五=5
            intervals.append(
                f"        <dict><key>Weekday</key><integer>{weekday}</integer>"
                f"<key>Hour</key><integer>{hour}</integer>"
                f"<key>Minute</key><integer>{minute}</integer></dict>"
            )
    intervals_xml = "\n".join(intervals)

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{label}</string>

    <key>ProgramArguments</key>
    <array>
        <string>{python_path}</string>
        <string>-m</string>
        <string>src.main</string>
        <string>--force</string>
        <string>--mode</string>
        <string>{mode}</string>
    </array>

    <key>WorkingDirectory</key>
    <string>{PROJECT_ROOT}</string>

    <key>StartCalendarInterval</key>
    <array>
{intervals_xml}
    </array>

    <key>StandardOutPath</key>
    <string>{PROJECT_ROOT}/logs/{log_basename}.log</string>

    <key>StandardErrorPath</key>
    <string>{PROJECT_ROOT}/logs/{log_basename}_error.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
"""


def create_plist_screener_left():
    """左側策略 (回調吸籌) - 13:00, 13:20"""
    return _build_screener_plist(
        label="com.stockscreener.left",
        mode="left",
        hour_minutes=[(13, 0), (13, 20)],
        log_basename="screener_left",
    )


def create_plist_screener_right():
    """
    右側策略 (撒網抓強勢) - 13:05, 13:25, 14:35
    - 13:05, 13:25: 盤中即時篩選 (融資資料用 T-1)
    - 14:35: 盤後完整篩選 (融資資料用當日，比左側 14:30 晚 5 分避併發)
    """
    return _build_screener_plist(
        label="com.stockscreener.right",
        mode="right",
        hour_minutes=[(13, 5), (13, 25), (14, 35)],
        log_basename="screener_right",
    )


def create_plist_institutional():
    """建立盤後法人追蹤的 plist 檔案 (14:30)"""
    python_path = get_python_path()

    plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.stockscreener.institutional</string>

    <key>ProgramArguments</key>
    <array>
        <string>{python_path}</string>
        <string>-m</string>
        <string>src.main</string>
        <string>--inst-only</string>
    </array>

    <key>WorkingDirectory</key>
    <string>{PROJECT_ROOT}</string>

    <key>StartCalendarInterval</key>
    <array>
        <!-- 週一至週五 14:30 (盤後法人資料公布後) -->
        <dict><key>Weekday</key><integer>1</integer><key>Hour</key><integer>14</integer><key>Minute</key><integer>30</integer></dict>
        <dict><key>Weekday</key><integer>2</integer><key>Hour</key><integer>14</integer><key>Minute</key><integer>30</integer></dict>
        <dict><key>Weekday</key><integer>3</integer><key>Hour</key><integer>14</integer><key>Minute</key><integer>30</integer></dict>
        <dict><key>Weekday</key><integer>4</integer><key>Hour</key><integer>14</integer><key>Minute</key><integer>30</integer></dict>
        <dict><key>Weekday</key><integer>5</integer><key>Hour</key><integer>14</integer><key>Minute</key><integer>30</integer></dict>
    </array>

    <key>StandardOutPath</key>
    <string>{PROJECT_ROOT}/logs/institutional.log</string>

    <key>StandardErrorPath</key>
    <string>{PROJECT_ROOT}/logs/institutional_error.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
"""
    return plist_content


def _install_plist(plist_filename: str, content: str, label: str):
    """通用 plist 安裝邏輯"""
    plist_path = PLIST_DIR / plist_filename
    if plist_path.exists():
        subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)

    with open(plist_path, "w") as f:
        f.write(content)
    print(f"\n已建立設定檔: {plist_path}")

    result = subprocess.run(["launchctl", "load", str(plist_path)], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"  ✓ {label} 任務已載入")
    else:
        print(f"  ✗ 載入失敗: {result.stderr}")


def _uninstall_plist(plist_filename: str, label: str):
    """通用 plist 卸載邏輯"""
    plist_path = PLIST_DIR / plist_filename
    if plist_path.exists():
        subprocess.run(["launchctl", "unload", str(plist_path)], capture_output=True)
        plist_path.unlink()
        print(f"{label} 任務已卸載")
    else:
        print(f"沒有找到 {label} 任務")


def install():
    """安裝定時任務"""
    print("=" * 50)
    print("  台股選股 - 定時任務設置")
    print("=" * 50)

    # 確保目錄存在
    PLIST_DIR.mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "logs").mkdir(parents=True, exist_ok=True)

    # 清除舊版單一篩選任務 (升級時)
    legacy_path = PLIST_DIR / PLIST_SCREENER_LEGACY
    if legacy_path.exists():
        subprocess.run(["launchctl", "unload", str(legacy_path)], capture_output=True)
        legacy_path.unlink()
        print(f"\n已移除舊版設定檔: {legacy_path}")

    # 左側策略 (回調吸籌)
    _install_plist(PLIST_SCREENER_LEFT, create_plist_screener_left(), "左側-回調吸籌")

    # 右側策略 (撒網抓強勢 + 散戶警示)
    _install_plist(PLIST_SCREENER_RIGHT, create_plist_screener_right(), "右側-撒網抓強勢")

    # 盤後法人追蹤
    _install_plist(PLIST_INSTITUTIONAL, create_plist_institutional(), "法人追蹤")

    print("\n" + "=" * 50)
    print("排程時間:")
    print("  【左側-回調吸籌】 週一至週五 13:00, 13:20")
    print("  【右側-撒網抓強勢】週一至週五 13:05, 13:25 (盤中), 14:35 (盤後+融資)")
    print("  【法人追蹤】     週一至週五 14:30")
    print(f"\n日誌檔案: {PROJECT_ROOT}/logs/")
    print(f"  - screener_left.log    (左側策略)")
    print(f"  - screener_right.log   (右側策略)")
    print(f"  - institutional.log    (法人追蹤)")
    print("=" * 50)

    return True


def uninstall():
    """卸載定時任務"""
    # 清舊版
    legacy_path = PLIST_DIR / PLIST_SCREENER_LEGACY
    if legacy_path.exists():
        subprocess.run(["launchctl", "unload", str(legacy_path)], capture_output=True)
        legacy_path.unlink()
        print(f"舊版盤中篩選任務已卸載")

    _uninstall_plist(PLIST_SCREENER_LEFT, "左側-回調吸籌")
    _uninstall_plist(PLIST_SCREENER_RIGHT, "右側-撒網抓強勢")
    _uninstall_plist(PLIST_INSTITUTIONAL, "法人追蹤")


def _print_status(stdout: str, label_token: str, display_name: str):
    """印出單一任務狀態"""
    print(f"\n【{display_name}】")
    found = False
    for line in stdout.split("\n"):
        if label_token in line:
            print(f"  狀態: 已啟用")
            print(f"  {line}")
            found = True
            break
    if not found:
        print("  狀態: 未啟用")


def status():
    """查看任務狀態"""
    result = subprocess.run(["launchctl", "list"], capture_output=True, text=True)

    print("=" * 50)
    print("  定時任務狀態")
    print("=" * 50)

    _print_status(result.stdout, "com.stockscreener.left", "左側-回調吸籌")
    _print_status(result.stdout, "com.stockscreener.right", "右側-撒網抓強勢")
    _print_status(result.stdout, "com.stockscreener.institutional", "法人追蹤")

    print("=" * 50)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="管理台股選股定時任務")
    parser.add_argument(
        "action",
        choices=["install", "uninstall", "status"],
        help="install=安裝, uninstall=卸載, status=查看狀態"
    )

    args = parser.parse_args()

    if args.action == "install":
        install()
    elif args.action == "uninstall":
        uninstall()
    elif args.action == "status":
        status()


if __name__ == "__main__":
    main()
