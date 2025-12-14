"""OpenAI 客户端实现"""

from typing import Any, Dict, List, Optional
import openai
from ymda.llm.base import BaseLLMClient
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class OpenAIClient(BaseLLMClient):
    """OpenAI 客户端"""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4"):
        """初始化 OpenAI 客户端"""
        super().__init__(api_key)
        self.model = model
        self.client = openai.OpenAI(api_key=api_key) if api_key else None
    
    def chat(self, messages: List[Dict[str, str]], **kwargs) -> Dict[str, Any]:
        """发送聊天请求"""
        if not self.client:
            raise ValueError("OpenAI client not initialized")
        
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages,
                **kwargs
            )
            return {
                "content": response.choices[0].message.content,
                "usage": response.usage.model_dump() if response.usage else None,
            }
        except Exception as e:
            logger.error(f"OpenAI chat error: {e}")
            raise
    
    def complete(self, prompt: str, **kwargs) -> str:
        """完成文本生成"""
        messages = [{"role": "user", "content": prompt}]
        response = self.chat(messages, **kwargs)
        return response["content"]
    
    def embed(self, text: str) -> List[float]:
        """生成文本嵌入向量"""
        if not self.client:
            raise ValueError("OpenAI client not initialized")
        
        try:
            response = self.client.embeddings.create(
                model="text-embedding-ada-002",
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"OpenAI embed error: {e}")
            raise

