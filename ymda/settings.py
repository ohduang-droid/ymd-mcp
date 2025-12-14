"""全局配置"""

import os
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv

load_dotenv()


@dataclass
class Settings:
    """全局配置类"""
    
    # LLM 配置
    openai_api_key: Optional[str] = None
    deep_research_api_key: Optional[str] = None  # Deprecated? Or generic?
    perplexity_api_key: Optional[str] = None

    # 数据库配置
    supabase_url: Optional[str] = None
    supabase_key: Optional[str] = None
    database_url: Optional[str] = None  # PostgreSQL连接字符串，用于pgvector
    supabase_db_password: Optional[str] = None  # Supabase数据库密码
    # Management API 配置（用于执行 SQL 等管理操作）
    supabase_access_token: Optional[str] = None  # Management API access token
    supabase_project_id: Optional[str] = None  # Supabase 项目 ID (project ref)
    
    # Pipeline 配置
    max_retries: int = 3
    timeout: int = 300
    
    def __post_init__(self):
        """从环境变量加载配置"""
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.deep_research_api_key = os.getenv("DEEP_RESEARCH_API_KEY")
        self.perplexity_api_key = os.getenv("PERPLEXITY_API_KEY")
        self.supabase_url = os.getenv("SUPABASE_URL")
        # 优先使用 service_role key（用于服务端操作，绕过 RLS）
        # 如果没有设置 service_role key，则使用 SUPABASE_KEY（可能是 anon key，权限受限）
        self.supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY") or os.getenv("SUPABASE_KEY")
        self.database_url = os.getenv("DATABASE_URL")
        self.supabase_db_password = os.getenv("SUPABASE_DB_PASSWORD")
        # Management API 配置
        self.supabase_access_token = os.getenv("SUPABASE_ACCESS_TOKEN")
        self.supabase_project_id = os.getenv("SUPABASE_PROJECT_ID") or os.getenv("SUPABASE_PROJECT_REF")
        
        # 如果没有提供database_url，尝试从Supabase配置构建
        if not self.database_url and self.supabase_url and self.supabase_db_password:
            # 构建Supabase PostgreSQL连接字符串
            # 格式: postgresql+psycopg2://postgres:[PASSWORD]@db.[PROJECT_REF].supabase.co:5432/postgres
            host = self.supabase_url.replace('https://', '').replace('http://', '')
            self.database_url = f"postgresql+psycopg2://postgres:{self.supabase_db_password}@db.{host}:5432/postgres"
        
        # 从环境变量读取配置覆盖
        if os.getenv("MAX_RETRIES"):
            self.max_retries = int(os.getenv("MAX_RETRIES"))
        if os.getenv("TIMEOUT"):
            self.timeout = int(os.getenv("TIMEOUT"))

