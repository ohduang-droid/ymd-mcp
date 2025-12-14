"""Pipeline Step 基类"""

from abc import ABC, abstractmethod
from typing import Any, Dict
from ymda.settings import Settings
from ymda.data.repository import get_repository, SupabaseRepository


class BaseStep(ABC):
    """步骤基类"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self._repository = None
    
    @property
    def repository(self) -> SupabaseRepository:
        """延迟初始化 repository"""
        if self._repository is None:
            self._repository = get_repository(self.settings)
        return self._repository
    
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """执行步骤"""
        pass
    
    def can_continue_on_error(self) -> bool:
        """错误时是否继续执行"""
        return False
