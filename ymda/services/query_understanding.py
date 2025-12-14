"""Query Understanding Service - 解析用户自然语言查询"""

from typing import Dict, List, Any, Optional
import json
from openai import OpenAI
from ymda.settings import Settings
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class QueryUnderstanding:
    """查询理解结果"""
    
    def __init__(
        self,
        semantic_query_text: str,
        matched_field_keys: List[str],
        filters: Optional[Dict[str, Any]] = None
    ):
        self.semantic_query_text = semantic_query_text
        self.matched_field_keys = matched_field_keys
        self.filters = filters or {}
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "semantic_query_text": self.semantic_query_text,
            "matched_field_keys": self.matched_field_keys,
            "filters": self.filters
        }


class QueryUnderstandingService:
    """LLM 查询理解服务"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.client = None
        self.model = "gpt-3.5-turbo"
        self._initialize()
    
    def _initialize(self):
        """初始化 OpenAI 客户端"""
        api_key = self.settings.openai_api_key
        if not api_key:
            logger.warning("OPENAI_API_KEY 未设置，Query Understanding 功能将不可用")
            return
        
        try:
            self.client = OpenAI(api_key=api_key)
            logger.debug(f"QueryUnderstandingService 初始化成功，模型: {self.model}")
        except Exception as e:
            logger.error(f"初始化 OpenAI 客户端失败: {e}")
    
    def parse_query(
        self,
        query_text: str,
        expected_fields: Optional[List[Dict[str, Any]]] = None
    ) -> QueryUnderstanding:
        """
        解析用户查询
        
        Args:
            query_text: 用户输入的自然语言查询
            expected_fields: YMQ 的 expected_fields 定义
            
        Returns:
            QueryUnderstanding 对象
        """
        if not self.client:
            logger.warning("OpenAI 客户端未初始化，返回简化的查询理解")
            return self._fallback_parse(query_text)
        
        try:
            # 构建 prompt
            prompt = self._build_prompt(query_text, expected_fields or [])
            
            # 调用 LLM
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a query understanding assistant for a knowledge base system."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            
            # 解析响应
            result = json.loads(response.choices[0].message.content)
            
            return QueryUnderstanding(
                semantic_query_text=result.get("semantic_query_text", query_text),
                matched_field_keys=result.get("matched_field_keys", []),
                filters=result.get("filters", {})
            )
            
        except Exception as e:
            logger.error(f"Query Understanding 失败: {e}")
            return self._fallback_parse(query_text)
    
    def _build_prompt(
        self,
        query_text: str,
        expected_fields: List[Dict[str, Any]]
    ) -> str:
        """构建 Query Understanding prompt"""
        
        # 格式化 expected_fields
        fields_context = ""
        if expected_fields:
            fields_context = "\n## Available Fields\n\n"
            for field in expected_fields[:10]:  # 限制字段数量避免 token 过多
                key = field.get('key', '')
                desc = field.get('description', '')
                example = field.get('example', '')
                fields_context += f"- **{key}**: {desc}"
                if example:
                    fields_context += f" (例如: {example})"
                fields_context += "\n"
        
        prompt = f"""You are analyzing a user query for a knowledge base search system.

{fields_context}

## User Query
"{query_text}"

## Your Task
Analyze the query and return a JSON object with:

1. **semantic_query_text**: Rewrite the query as keywords optimized for semantic search (English preferred, focus on key concepts)
2. **matched_field_keys**: List of field keys from Available Fields that match the query intent (empty list if none match)
3. **filters**: Extract any numeric filters (min, max, between) mentioned in the query

## Output Format (JSON)
{{
  "semantic_query_text": "profitability factors capex opex payback analysis",
  "matched_field_keys": ["financial.capex.total", "financial.payback_months.base"],
  "filters": {{"min": 10000, "max": 50000}}
}}

## Examples

Query: "美甲机的回本周期一般是多少？"
Output:
{{
  "semantic_query_text": "payback period months nail art machine profitability",
  "matched_field_keys": ["financial.payback_months.base"],
  "filters": {{}}
}}

Query: "低于2万的设备有哪些？"
Output:
{{
  "semantic_query_text": "equipment cost price budget affordable",
  "matched_field_keys": ["financial.capex.total"],
  "filters": {{"max": 20000}}
}}

Now analyze the user query above and return ONLY the JSON object.
"""
        return prompt
    
    def _fallback_parse(self, query_text: str) -> QueryUnderstanding:
        """降级处理：直接使用原始查询"""
        logger.debug("使用 fallback 模式解析查询")
        return QueryUnderstanding(
            semantic_query_text=query_text,
            matched_field_keys=[],
            filters={}
        )
