"""
篩選器基類
"""
from abc import ABC, abstractmethod
import pandas as pd
import logging

logger = logging.getLogger(__name__)


class BaseScreener(ABC):
    """篩選器抽象基類"""

    def __init__(self, name: str, step_number: int):
        self.name = name
        self.step_number = step_number
        self.input_count = 0
        self.output_count = 0

    @abstractmethod
    def screen(self, df: pd.DataFrame) -> pd.DataFrame:
        """執行篩選邏輯"""
        pass

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """允許像函數一樣調用篩選器"""
        self.input_count = len(df)
        result = self.screen(df)
        self.output_count = len(result)

        logger.info(
            f"[步驟{self.step_number}] {self.name}: "
            f"{self.input_count} -> {self.output_count} 檔 "
            f"(淘汰 {self.input_count - self.output_count} 檔)"
        )
        return result

    def get_stats(self) -> dict:
        """獲取篩選統計"""
        return {
            "step": self.step_number,
            "name": self.name,
            "input": self.input_count,
            "output": self.output_count,
            "filtered_out": self.input_count - self.output_count,
            "pass_rate": f"{self.output_count/self.input_count*100:.1f}%" if self.input_count else "0%"
        }
