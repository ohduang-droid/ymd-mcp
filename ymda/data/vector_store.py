"""pgvector 管理 - 使用Supabase pgvector"""

import os
from typing import Any, Dict, List, Optional
from datetime import datetime
from langchain_community.vectorstores import PGVector
from langchain_openai import OpenAIEmbeddings
from ymda.settings import Settings
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class VectorStore:
    """向量存储管理器 - 使用Supabase pgvector"""
    
    def __init__(self, settings: Settings):
        """初始化向量存储"""
        self.settings = settings
        self.vector_store: Optional[PGVector] = None
        self.embeddings: Optional[OpenAIEmbeddings] = None
        self._initialize()
    
    def _initialize(self):
        """初始化PGVector和Embeddings"""
        try:
            # 初始化embeddings
            if not self.settings.openai_api_key:
                logger.warning("OpenAI API密钥未配置，向量存储功能不可用")
                return
            
            self.embeddings = OpenAIEmbeddings(
                model="text-embedding-3-small",
                openai_api_key=self.settings.openai_api_key
            )
            
            # 构建Supabase PostgreSQL连接字符串
            if not self.settings.database_url:
                logger.warning("数据库连接字符串未配置，向量存储功能不可用")
                return
            
            # 创建PGVector向量存储
            self.vector_store = PGVector(
                embedding_function=self.embeddings,
                connection_string=self.settings.database_url,
                collection_name="ym_embeddings"
            )
            
            logger.info("向量存储初始化成功")
        except Exception as e:
            logger.error(f"初始化向量存储失败: {e}")
            # 不抛出异常，允许系统在没有向量存储的情况下运行
    
    def store_answer_embedding(
        self, 
        ym_id: str, 
        question_id: str, 
        raw_answer_text: str
    ) -> bool:
        """存储答案的embedding"""
        if not self.vector_store:
            logger.warning("向量存储未初始化，跳过存储")
            return False
        
        try:
            self.vector_store.add_texts(
                texts=[raw_answer_text],
                metadatas=[{
                    "ym_id": ym_id,
                    "question_id": question_id,
                    "type": "answer",
                    "created_at": datetime.now().isoformat()
                }]
            )
            logger.info(f"存储答案embedding成功: YM={ym_id}, Question={question_id}")
            return True
        except Exception as e:
            logger.error(f"存储答案embedding失败: {e}")
            return False
    
    def store_summary_embedding(self, ym_id: str, summary_text: str) -> bool:
        """存储摘要的embedding"""
        if not self.vector_store:
            logger.warning("向量存储未初始化，跳过存储")
            return False
        
        try:
            self.vector_store.add_texts(
                texts=[summary_text],
                metadatas=[{
                    "ym_id": ym_id,
                    "type": "summary",
                    "created_at": datetime.now().isoformat()
                }]
            )
            logger.info(f"存储摘要embedding成功: YM={ym_id}")
            return True
        except Exception as e:
            logger.error(f"存储摘要embedding失败: {e}")
            return False
    
    def search_similar(
        self, 
        query_text: str, 
        k: int = 5,
        filter_dict: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """相似度搜索"""
        if not self.vector_store:
            logger.warning("向量存储未初始化，无法搜索")
            return []
        
        try:
            # 构建过滤条件
            if filter_dict:
                # PGVector的过滤需要特殊处理
                # 这里简化处理，实际使用时可能需要根据PGVector的API调整
                results = self.vector_store.similarity_search_with_score(
                    query_text, 
                    k=k
                )
            else:
                results = self.vector_store.similarity_search_with_score(
                    query_text, 
                    k=k
                )
            
            # 转换结果格式
            return [
                {
                    "text": doc.page_content,
                    "metadata": doc.metadata,
                    "score": score
                }
                for doc, score in results
            ]
        except Exception as e:
            logger.error(f"相似度搜索失败: {e}")
            return []

