"""ymd.search MCP Tool

提供 YMD 混合检索功能的 MCP Tool
"""

from typing import Dict, Any, Optional, List
from ymda.settings import Settings
from ymda.services.ymd_search_service import YMDSearchService
from ymda.mcp.schemas import SearchRequest, FilterMetric
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


def ymd_search(
    query: str,
    filters: Optional[Dict[str, List[Dict]]] = None,
    top_k: int = 20,
    mode: str = "auto",
    explain: bool = False,
    **kwargs  # 接受额外参数但忽略
) -> Dict[str, Any]:
    """
    YMD Search - 混合检索 MCP Tool
    
    执行混合检索（Vector + Structured），返回匹配的 metrics
    
    Args:
        query: 用户查询文本
        filters: 过滤条件 (Phase 2支持)
            格式: {"metrics": [{"key": "...", "op": "...", "value": ...}, ...]}
        top_k: 返回结果数量
        mode: 查询模式 (auto | semantic_only | structured_only | hybrid)
        explain: 是否返回详细explain信息
        
    Returns:
        {
            "trace_id": "uuid",
            "query_text": "user query",
            "mode": "semantic_only",
            "results": [...],
            "stats": {...},
            "explain": {...}  # if explain=True
        }
        
    Example:
        >>> result = ymd_search("美甲机的回本周期多久？", top_k=5)
        >>> print(f"Found {len(result['results'])} results")
    """
    try:
        # 构建请求对象
        request = SearchRequest(
            query=query,
            filters=filters,
            top_k=top_k,
            mode=mode,
            explain=explain
        )
        
        # 执行查询
        settings = Settings()
        service = YMDSearchService(settings)
        response = service.search(request)
        
        return response.to_dict()
        
    except Exception as e:
        logger.error(f"ymd.search failed: {e}", exc_info=True)
        return {
            "trace_id": "error",
            "query_text": query,
            "mode": mode,
            "results": [],
            "stats": {
                "mode": mode,
                "returned": 0,
                "error": str(e)
            },
            "error": str(e)
        }


# MCP Tool Metadata (for registration)
TOOL_METADATA = {
    "name": "ymd.search",
    "description": "Hybrid search for YMD metrics using vector similarity + structured filters",
    "parameters": {
        "type": "object",
        "properties": {
            "query": {
                "type": "string",
                "description": "User's natural language query"
            },
            "filters": {
                "type": "object",
                "description": "Structured filters (Phase 2)",
                "properties": {
                    "metrics": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "key": {"type": "string"},
                                "op": {"type": "string", "enum": ["eq", "in", "between", "gte", "lte"]},
                                "value": {},
                                "unit": {"type": "string"}
                            },
                            "required": ["key", "op", "value"]
                        }
                    }
                }
            },
            "top_k": {
                "type": "integer",
                "description": "Number of results to return",
                "default": 20
            },
            "mode": {
                "type": "string",
                "enum": ["auto", "semantic_only", "structured_only", "hybrid"],
                "description": "Query mode",
                "default": "auto"
            },
            "explain": {
                "type": "boolean",
                "description": "Return detailed explanation",
                "default": False
            }
        },
        "required": ["query"]
    }
}
