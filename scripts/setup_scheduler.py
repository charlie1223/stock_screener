#!/usr/bin/env python3
"""
設置定時執行任務 (macOS launchd)
"""
import os
import sys
from pathlib import Path
import subprocess

PROJECT_ROOT = Path(__file__).parent.parent
PLIST_NAME = "com.stockscreener.plist"
PLIST_PATH = Path.home() / "Library" / "LaunchAgents" / PLIST_NAME


def get_python_path() -> str:
    """獲取 Python 路徑"""
    # 優先使用虛擬環境
    venv_python = PROJECT_ROOT / "venv" / "bin" / "python"
    if venv_python.exists():
        return str(venv_python)
    return sys.executable


def create_plist():
    """建立 launchd plist 檔案"""
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
        <!-- 週一 13:00 -->
        <dict>
            <key>Weekday</key>
            <integer>1</integer>
            <key>Hour</key>
            <integer>13</integer>
            <key>Minute</key>
            <integer>0</integer>
        </dict>
        <!-- 週一 13:20 -->
        <dict>
            <key>Weekday</key>
            <integer>1</integer>
            <key>Hour</key>
            <integer>13</integer>
            <key>Minute</key>
            <integer>20</integer>
        </dict>
        <!-- 週二 13:00 -->
        <dict>
            <key>Weekday</key>
            <integer>2</integer>
            <key>Hour</key>
            <integer>13</integer>
            <key>Minute</key>
            <integer>0</integer>
        </dict>
        <!-- 週二 13:20 -->
        <dict>
            <key>Weekday</key>
            <integer>2</integer>
            <key>Hour</key>
            <integer>13</integer>
            <key>Minute</key>
            <integer>20</integer>
        </dict>
        <!-- 週三 13:00 -->
        <dict>
            <key>Weekday</key>
            <integer>3</integer>
            <key>Hour</key>
            <integer>13</integer>
            <key>Minute</key>
            <integer>0</integer>
        </dict>
        <!-- 週三 13:20 -->
        <dict>
            <key>Weekday</key>
            <integer>3</integer>
            <key>Hour</key>
            <integer>13</integer>
            <key>Minute</key>
            <integer>20</integer>
        </dict>
        <!-- 週四 13:00 -->
        <dict>
            <key>Weekday</key>
            <integer>4</integer>
            <key>Hour</key>
            <integer>13</integer>
            <key>Minute</key>
            <integer>0</integer>
        </dict>
        <!-- 週四 13:20 -->
        <dict>
            <key>Weekday</key>
            <integer>4</integer>
            <key>Hour</key>
            <integer>13</integer>
            <key>Minute</key>
            <integer>20</integer>
        </dict>
        <!-- 週五 13:00 -->
        <dict>
            <key>Weekday</key>
            <integer>5</integer>
            <key>Hour</key>
            <integer>13</integer>
            <key>Minute</key>
            <integer>0</integer>
        </dict>
        <!-- 週五 13:20 -->
        <dict>
            <key>Weekday</key>
            <integer>5</integer>
            <key>Hour</key>
            <integer>13</integer>
            <key>Minute</key>
            <integer>20</integer>
        </dict>
    </array>

    <key>StandardOutPath</key>
    <string>{PROJECT_ROOT}/logs/scheduler.log</string>

    <key>StandardErrorPath</key>
    <string>{PROJECT_ROOT}/logs/scheduler_error.log</string>

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
    print("  台股尾盤選股 - 定時任務設置")
    print("=" * 50)

    # 確保目錄存在
    PLIST_PATH.parent.mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "logs").mkdir(parents=True, exist_ok=True)

    # 先卸載舊的任務
    if PLIST_PATH.exists():
        print(f"\n正在卸載舊的定時任務...")
        subprocess.run(["launchctl", "unload", str(PLIST_PATH)], capture_output=True)

    # 建立 plist 檔案
    plist_content = create_plist()
    with open(PLIST_PATH, "w") as f:
        f.write(plist_content)
    print(f"\n已建立設定檔: {PLIST_PATH}")

    # 載入定時任務
    result = subprocess.run(
        ["launchctl", "load", str(PLIST_PATH)],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print("\n定時任務已成功載入!")
        print("\n執行時間:")
        print("  - 週一至週五 13:00 (第一次篩選)")
        print("  - 週一至週五 13:20 (第二次篩選)")
        print(f"\n日誌檔案: {PROJECT_ROOT}/logs/scheduler.log")
    else:
        print(f"\n載入失敗: {result.stderr}")
        return False

    return True


def uninstall():
    """卸載定時任務"""
    if PLIST_PATH.exists():
        subprocess.run(["launchctl", "unload", str(PLIST_PATH)], capture_output=True)
        PLIST_PATH.unlink()
        print("定時任務已卸載")
    else:
        print("沒有找到定時任務")


def status():
    """查看任務狀態"""
    result = subprocess.run(
        ["launchctl", "list"],
        capture_output=True,
        text=True
    )

    if "com.stockscreener" in result.stdout:
        print("定時任務狀態: 已啟用")

        # 顯示詳細資訊
        for line in result.stdout.split("\n"):
            if "com.stockscreener" in line:
                print(f"  {line}")
    else:
        print("定時任務狀態: 未啟用")


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
