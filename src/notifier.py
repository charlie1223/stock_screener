"""
通知模組 - Discord Webhook 推送
支援將選股結果推送到 Discord
"""
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional
import logging
import os

logger = logging.getLogger(__name__)


class DiscordNotifier:
    """Discord Webhook 通知器"""

    def __init__(self, webhook_url: str = None):
        """
        初始化 Discord 通知器
        Args:
            webhook_url: Discord Webhook URL，若未提供則從環境變數讀取
        """
        self.webhook_url = webhook_url or os.getenv("DISCORD_WEBHOOK_URL", "")
        self.enabled = bool(self.webhook_url)

        if not self.enabled:
            logger.warning("Discord Webhook URL 未設定，通知功能已停用")

    def send_message(self, content: str) -> bool:
        """
        發送純文字訊息
        Args:
            content: 訊息內容 (最多 2000 字元)
        Returns:
            是否發送成功
        """
        if not self.enabled:
            return False

        try:
            # Discord 訊息上限 2000 字元
            if len(content) > 2000:
                content = content[:1997] + "..."

            payload = {"content": content}
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )

            if response.status_code == 204:
                logger.info("Discord 訊息發送成功")
                return True
            else:
                logger.error(f"Discord 發送失敗: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Discord 發送錯誤: {e}")
            return False

    def send_embed(self, title: str, description: str = "",
                   fields: List[Dict] = None, color: int = 0x00FF00) -> bool:
        """
        發送 Embed 格式訊息 (更美觀的卡片式訊息)
        Args:
            title: 標題
            description: 描述
            fields: 欄位列表 [{"name": str, "value": str, "inline": bool}]
            color: 顏色 (16進位)
        Returns:
            是否發送成功
        """
        if not self.enabled:
            return False

        try:
            embed = {
                "title": title,
                "description": description,
                "color": color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "台股選股機器人"}
            }

            if fields:
                embed["fields"] = fields[:25]  # Discord 限制最多 25 個欄位

            payload = {"embeds": [embed]}
            response = requests.post(
                self.webhook_url,
                json=payload,
                timeout=10
            )

            if response.status_code == 204:
                logger.info("Discord Embed 發送成功")
                return True
            else:
                logger.error(f"Discord Embed 發送失敗: {response.status_code}")
                return False

        except Exception as e:
            logger.error(f"Discord Embed 發送錯誤: {e}")
            return False

    def send_screening_results(self, df: pd.DataFrame,
                                strategy_name: str = "回調縮量吸籌策略") -> bool:
        """
        發送選股結果
        Args:
            df: 篩選結果 DataFrame
            strategy_name: 策略名稱
        Returns:
            是否發送成功
        """
        if not self.enabled:
            return False

        if df.empty:
            return self.send_embed(
                title=f"📊 {strategy_name} - 今日選股結果",
                description="今日無符合條件的股票",
                color=0x808080  # 灰色
            )

        # 準備股票清單
        stock_list = []
        for _, row in df.head(15).iterrows():  # 最多顯示 15 檔
            stock_id = row.get("stock_id", "")
            stock_name = row.get("stock_name", "")
            price = row.get("price", 0)
            change_pct = row.get("change_pct", 0)
            industry = row.get("industry", "")

            # 漲跌符號
            sign = "🔺" if change_pct > 0 else ("🔻" if change_pct < 0 else "➖")

            stock_list.append(
                f"{sign} **{stock_id}** {stock_name} | {price:.2f} ({change_pct:+.2f}%)"
            )

        # 產業分布
        industry_summary = ""
        if "industry" in df.columns:
            industry_counts = df["industry"].value_counts().head(5)
            industry_parts = [f"{ind}({cnt})" for ind, cnt in industry_counts.items()]
            industry_summary = " | ".join(industry_parts)

        # 建立 Embed
        fields = [
            {
                "name": f"📈 精選股票 ({len(df)} 檔)",
                "value": "\n".join(stock_list) if stock_list else "無",
                "inline": False
            }
        ]

        if industry_summary:
            fields.append({
                "name": "🏭 產業分布",
                "value": industry_summary,
                "inline": False
            })

        if len(df) > 15:
            fields.append({
                "name": "📋 完整清單",
                "value": f"還有 {len(df) - 15} 檔未顯示，請查看 CSV 檔案",
                "inline": False
            })

        return self.send_embed(
            title=f"📊 {strategy_name} - 今日選股結果",
            description=f"篩選時間: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            fields=fields,
            color=0x00FF00 if len(df) > 0 else 0x808080
        )

    def send_step_summary(self, step_results: dict) -> bool:
        """
        發送逐步篩選摘要
        Args:
            step_results: 篩選結果字典 {step_num: {"name": str, "data": DataFrame}}
        Returns:
            是否發送成功
        """
        if not self.enabled:
            return False

        if not step_results:
            return False

        # 建立步驟摘要
        step_lines = []
        for step_num in sorted(step_results.keys()):
            step_info = step_results[step_num]
            step_name = step_info["name"]
            count = len(step_info["data"])
            step_lines.append(f"步驟{step_num}: {step_name} → **{count}** 檔")

        # 最終結果
        final_step = max(step_results.keys())
        final_count = len(step_results[final_step]["data"])

        fields = [
            {
                "name": "📋 篩選流程",
                "value": "\n".join(step_lines),
                "inline": False
            },
            {
                "name": "🎯 最終結果",
                "value": f"共 **{final_count}** 檔股票通過所有篩選條件",
                "inline": False
            }
        ]

        return self.send_embed(
            title="📊 逐步篩選摘要",
            description=f"執行時間: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            fields=fields,
            color=0x3498DB  # 藍色
        )

    def send_error_alert(self, error_message: str) -> bool:
        """
        發送錯誤警報
        Args:
            error_message: 錯誤訊息
        Returns:
            是否發送成功
        """
        if not self.enabled:
            return False

        return self.send_embed(
            title="⚠️ 選股程式錯誤",
            description=f"```\n{error_message[:1000]}\n```",
            color=0xFF0000  # 紅色
        )


# 全域通知器實例
_notifier: Optional[DiscordNotifier] = None


def get_notifier() -> DiscordNotifier:
    """取得全域通知器實例"""
    global _notifier
    if _notifier is None:
        _notifier = DiscordNotifier()
    return _notifier


def notify_results(df: pd.DataFrame, strategy_name: str = "回調縮量吸籌策略") -> bool:
    """
    快速發送選股結果通知
    Args:
        df: 篩選結果 DataFrame
        strategy_name: 策略名稱
    Returns:
        是否發送成功
    """
    return get_notifier().send_screening_results(df, strategy_name)
