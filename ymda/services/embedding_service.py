"""Embedding 生成服务"""

from typing import Optional, List
from openai import OpenAI
from ymda.settings import Settings
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class EmbeddingService:
    """Embedding 生成服务（使用 OpenAI API）"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = None
        self.model = "text-embedding-3-small"  # 1536维，匹配数据库schema
        self._initialize()
    
    def _initialize(self):
        """初始化 OpenAI 客户端"""
        api_key = self.settings.openai_api_key
        if not api_key:
            logger.warning("OPENAI_API_KEY 未设置，embedding 功能将不可用")
            return
        
        try:
            self.client = OpenAI(api_key=api_key)
            logger.debug(f"EmbeddingService 初始化成功，模型: {self.model}")
        except Exception as e:
            logger.error(f"初始化 OpenAI 客户端失败: {e}")
    
    def generate_embedding(self, text: str) -> Optional[List[float]]:
        """
        生成文本的 embedding
        
        Args:
            text: 要生成 embedding 的文本
            
        Returns:
            embedding 向量（list of floats），失败返回 None
        """
        if not self.client:
            logger.warning("OpenAI 客户端未初始化，无法生成 embedding")
            return None
        
        if not text or not text.strip():
            logger.debug("文本为空，跳过 embedding 生成")
            return None
        
        try:
            response = self.client.embeddings.create(
                model=self.model,
                input=text
            )
            embedding = response.data[0].embedding
            logger.debug(f"生成 embedding 成功，维度: {len(embedding)}")
            return embedding
        except Exception as e:
            logger.error(f"生成 embedding 失败: {e}")
            return None
    
    def generate_metric_embedding(self, evidence_text: Optional[str]) -> Optional[List[float]]:
        """
        为 metric 的 evidence_text 生成 embedding
        
        Args:
            evidence_text: metric 的证据文本
            
        Returns:
            embedding 向量，如果 evidence_text 为空则返回 None
        """
        if not evidence_text:
            return None
        
        return self.generate_embedding(evidence_text)
