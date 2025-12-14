"""LLM 抽象基类 - 统一接口"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional


class BaseLLMClient(ABC):
    """LLM 客户端抽象基类"""
    
    def __init__(self, api_key: Optional[str] = None):
        """初始化客户端"""
        self.api_key = api_key
    
    @abstractmethod
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> Dict[str, Any]:
        """发送聊天请求"""
        pass
    
    @abstractmethod
    def complete(self, prompt: str, **kwargs) -> str:
        """完成文本生成"""
        pass
    
    @abstractmethod
    def embed(self, text: str) -> List[float]:
        """生成文本嵌入向量"""
        pass

