"""计时器工具"""

import time
from contextlib import contextmanager
from typing import Optional
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class Timer:
    """计时器类"""
    
    def __init__(self, name: Optional[str] = None):
        """初始化计时器"""
        self.name = name or "Timer"
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
    
    def start(self):
        """开始计时"""
        self.start_time = time.time()
        logger.debug(f"{self.name} started")
    
    def stop(self) -> float:
        """停止计时并返回耗时"""
        if self.start_time is None:
            raise RuntimeError("Timer not started")
        self.end_time = time.time()
        elapsed = self.end_time - self.start_time
        logger.info(f"{self.name} completed in {elapsed:.2f}s")
        return elapsed
    
    @contextmanager
    def context(self):
        """上下文管理器"""
        self.start()
        try:
            yield self
        finally:
            self.stop()
    
    def elapsed(self) -> Optional[float]:
        """获取已用时间（不停止计时）"""
        if self.start_time is None:
            return None
        end = self.end_time if self.end_time else time.time()
        return end - self.start_time

