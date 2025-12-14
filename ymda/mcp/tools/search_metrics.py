"""search_metrics MCP Tool - Hybrid Search for YMD Metrics"""

from typing import Dict, Any, Optional, List
from ymda.settings import Settings
from ymda.services.hybrid_search import HybridSearchService
from ymda.data.repository import get_repository
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


def search_metrics(
    query_text: str,
    top_k: int = 30,
    ymq_id: Optional[int] = None
) -> Dict[str, Any]:
    """
    Hybrid search for YMD metrics using vector + BM25
    
    This tool performs semantic search across the metric knowledge base,
    combining vector similarity (embedding) with keyword matching (BM25).
    
    Args:
        query_text: User's natural language query
        top_k: Number of results to return (default: 30)
        ymq_id: Optional YMQ ID to load expected_fields for better query understanding
        
    Returns:
        {
            "query_text": "user input",
            "semantic_query_text": "LLM generated semantic query",
            "matched_fields": ["financial.capex.total", ...],
            "top_k": 30,
            "results": [
                {
                    "metric_id": 123,
                    "key": "financial.capex.total",
                    "value_numeric": 20000,
                    "value_text": null,
                    "value_json": null,
                    "evidence_text": "CAPEX around 20k...",
                    "evidence_sources": ["https://..."],
                    "vector_score": 0.92,
                    "text_score": 0.41,
                    "hybrid_score": 0.814,
                    "ym_id": 1,
                    "ymq_id": 3
                },
                ...
            ]
        }
    
    Example:
        >>> result = search_metrics("美甲机的回本周期一般是多少？", top_k=10)
        >>> print(f"Found {len(result['results'])} results")
        >>> print(f"Top result: {result['results'][0]['key']}")
    """
    try:
        settings = Settings()
        
        # 获取 expected_fields（如果提供了 ymq_id）
        expected_fields = None
        if ymq_id:
            try:
                repository = get_repository(settings)
                ymq_data = repository.client.table('ymq').select('expected_fields').eq('id', ymq_id).execute()
                if ymq_data.data and len(ymq_data.data) > 0:
                    expected_fields_json = ymq_data.data[0].get('expected_fields')
                    if expected_fields_json and isinstance(expected_fields_json, dict):
                        expected_fields = expected_fields_json.get('fields', [])
                        logger.debug(f"Loaded {len(expected_fields)} expected_fields from YMQ {ymq_id}")
            except Exception as e:
                logger.warning(f"Failed to load expected_fields for YMQ {ymq_id}: {e}")
        
        # 执行混合检索
        search_service = HybridSearchService(settings)
        result = search_service.search(
            query_text=query_text,
            top_k=top_k,
            expected_fields=expected_fields
        )
        
        return result.to_dict()
        
    except Exception as e:
        logger.error(f"search_metrics failed: {e}")
        return {
            "query_text": query_text,
            "semantic_query_text": query_text,
            "matched_fields": [],
            "top_k": top_k,
            "results": [],
            "error": str(e)
        }


# MCP Tool metadata (for registration)
TOOL_METADATA = {
    "name": "search_metrics",
    "description": "Hybrid search for YMD metrics using vector similarity + BM25 keyword matching",
    "parameters": {
        "type": "object",
        "properties": {
            "query_text": {
                "type": "string",
                "description": "User's natural language query"
            },
            "top_k": {
                "type": "integer",
                "description": "Number of results to return",
                "default": 30
            },
            "ymq_id": {
                "type": "integer",
                "description": "Optional YMQ ID to load expected_fields for better query understanding",
                "default": None
            }
        },
        "required": ["query_text"]
    }
}
