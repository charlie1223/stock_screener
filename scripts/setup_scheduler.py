#!/usr/bin/env python3
"""
設置定時執行任務 (macOS launchd)

排程項目：
1. 盤中篩選 (13:00, 13:20) - 今日訊號
2. 盤後追蹤 (14:30) - 法人佈局追蹤
"""
import os
import sys
from pathlib import Path
import subprocess

PROJECT_ROOT = Path(__file__).parent.parent

# 兩個排程任務
PLIST_SCREENER = "com.stockscreener.plist"
PLIST_INSTITUTIONAL = "com.stockscreener.institutional.plist"

PLIST_DIR = Path.home() / "Library" / "LaunchAgents"


def get_python_path() -> str:
    """獲取 Python 路徑"""
    venv_python = PROJECT_ROOT / "venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def create_plist_screener():
    """建立盤中篩選的 plist 檔案 (13:00, 13:20)"""
    python_path = get_python_path()

    plist_content = f"""<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.stockscreener</string>

    <key>ProgramArguments</key>
    <array>
        <string>{python_path}</string>
        <string>-m</string>
        <string>src.main</string>
        <string>--force</string>
    </array>

    <key>WorkingDirectory</key>
    <string>{PROJECT_ROOT}</string>

    <key>StartCalendarInterval</key>
    <array>
        <!-- 週一至週五 13:00 -->
        <dict><key>Weekday</key><integer>1</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>0</integer></dict>
        <dict><key>Weekday</key><integer>2</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>0</integer></dict>
        <dict><key>Weekday</key><integer>3</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>0</integer></dict>
        <dict><key>Weekday</key><integer>4</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>0</integer></dict>
        <dict><key>Weekday</key><integer>5</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>0</integer></dict>
        <!-- 週一至週五 13:20 -->
        <dict><key>Weekday</key><integer>1</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>20</integer></dict>
        <dict><key>Weekday</key><integer>2</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>20</integer></dict>
        <dict><key>Weekday</key><integer>3</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>20</integer></dict>
        <dict><key>Weekday</key><integer>4</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>20</integer></dict>
        <dict><key>Weekday</key><integer>5</integer><key>Hour</key><integer>13</integer><key>Minute</key><integer>20</integer></dict>
    </array>

    <key>StandardOutPath</key>
    <string>{PROJECT_ROOT}/logs/screener.log</string>

    <key>StandardErrorPath</key>
    <string>{PROJECT_ROOT}/logs/screener_error.log</string>

    <key>EnvironmentVariables</key>
    <dict>
        <key>PATH</key>
        <string>/usr/local/bin:/usr/bin:/bin</string>
    </dict>
</dict>
</plist>
"""
    return plist_content


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


def install():
    """安裝定時任務"""
    print("=" * 50)
    print("  台股選股 - 定時任務設置")
    print("=" * 50)

    # 確保目錄存在
    PLIST_DIR.mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "logs").mkdir(parents=True, exist_ok=True)

    # 安裝盤中篩選任務
    plist_path_screener = PLIST_DIR / PLIST_SCREENER
    if plist_path_screener.exists():
        subprocess.run(["launchctl", "unload", str(plist_path_screener)], capture_output=True)

    with open(plist_path_screener, "w") as f:
        f.write(create_plist_screener())
    print(f"\n已建立盤中篩選設定檔: {plist_path_screener}")

    result = subprocess.run(["launchctl", "load", str(plist_path_screener)], capture_output=True, text=True)
    if result.returncode == 0:
        print("  ✓ 盤中篩選任務已載入")
    else:
        print(f"  ✗ 載入失敗: {result.stderr}")

    # 安裝盤後法人追蹤任務
    plist_path_inst = PLIST_DIR / PLIST_INSTITUTIONAL
    if plist_path_inst.exists():
        subprocess.run(["launchctl", "unload", str(plist_path_inst)], capture_output=True)

    with open(plist_path_inst, "w") as f:
        f.write(create_plist_institutional())
    print(f"\n已建立法人追蹤設定檔: {plist_path_inst}")

    result = subprocess.run(["launchctl", "load", str(plist_path_inst)], capture_output=True, text=True)
    if result.returncode == 0:
        print("  ✓ 法人追蹤任務已載入")
    else:
        print(f"  ✗ 載入失敗: {result.stderr}")

    print("\n" + "=" * 50)
    print("排程時間:")
    print("  【盤中篩選】週一至週五 13:00, 13:20")
    print("  【法人追蹤】週一至週五 14:30")
    print(f"\n日誌檔案: {PROJECT_ROOT}/logs/")
    print("=" * 50)

    return True


def uninstall():
    """卸載定時任務"""
    plist_path_screener = PLIST_DIR / PLIST_SCREENER
    plist_path_inst = PLIST_DIR / PLIST_INSTITUTIONAL

    if plist_path_screener.exists():
        subprocess.run(["launchctl", "unload", str(plist_path_screener)], capture_output=True)
        plist_path_screener.unlink()
        print("盤中篩選任務已卸載")
    else:
        print("沒有找到盤中篩選任務")

    if plist_path_inst.exists():
        subprocess.run(["launchctl", "unload", str(plist_path_inst)], capture_output=True)
        plist_path_inst.unlink()
        print("法人追蹤任務已卸載")
    else:
        print("沒有找到法人追蹤任務")


def status():
    """查看任務狀態"""
    result = subprocess.run(["launchctl", "list"], capture_output=True, text=True)

    print("=" * 50)
    print("  定時任務狀態")
    print("=" * 50)

    if "com.stockscreener" in result.stdout:
        print("\n【盤中篩選】")
        for line in result.stdout.split("\n"):
            if "com.stockscreener" in line and "institutional" not in line:
                print(f"  狀態: 已啟用")
                print(f"  {line}")
                break
        else:
            print("  狀態: 未啟用")
    else:
        print("\n【盤中篩選】狀態: 未啟用")

    if "com.stockscreener.institutional" in result.stdout:
        print("\n【法人追蹤】")
        for line in result.stdout.split("\n"):
            if "com.stockscreener.institutional" in line:
                print(f"  狀態: 已啟用")
                print(f"  {line}")
                break
    else:
        print("\n【法人追蹤】狀態: 未啟用")

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
