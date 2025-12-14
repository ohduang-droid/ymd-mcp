"""MCP Schemas - Data structures for YMD Search MCP Tools

包含 MCP 请求/响应的数据结构定义
"""

from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional
from datetime import datetime


@dataclass
class FilterMetric:
    """单个 filter 指标定义"""
    key: str
    op: str  # eq, in, between, gte, lte
    value: Any  # 可以是单值、列表或范围
    unit: Optional[str] = None


@dataclass
class SearchRequest:
    """YMD Search 请求"""
    query: str
    filters: Optional[Dict[str, List[FilterMetric]]] = None  # {"metrics": [...]}
    top_k: int = 20
    mode: str = "auto"  # auto | semantic_only | structured_only | hybrid
    explain: bool = False
    return_config: Optional[Dict[str, bool]] = None  # {"include_sql": True, ...}
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)


@dataclass
class SearchStats:
    """查询统计信息"""
    mode: str
    accepted_filters: int = 0
    rejected_filters: int = 0
    candidate_count: Optional[int] = None
    semantic_recall: Optional[int] = None
    returned: int = 0
    latency_ms: Optional[float] = None
    matched_registry_keys: Optional[int] = None
    retrieved_chunks: Optional[int] = None
    matched_metrics: Optional[int] = None
    fallback_used: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return asdict(self)


@dataclass
class SearchResponse:
    """YMD Search 响应"""
    trace_id: str
    query_text: str
    mode: str
    results: List[Dict[str, Any]]
    stats: SearchStats
    explain: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return {
            "trace_id": self.trace_id,
            "query_text": self.query_text,
            "mode": self.mode,
            "results": self.results,
            "stats": self.stats.to_dict() if isinstance(self.stats, SearchStats) else self.stats,
            "explain": self.explain
        }


# SSE 事件定义 (Phase 3 使用)

@dataclass
class PlanEvent:
    """Plan 事件"""
    trace_id: str
    registry_version: str
    mode: str
    accepted_filters: List[Dict]
    rejected_filters: List[Dict]
    notes: Dict[str, Any]


@dataclass
class SQLEvent:
    """SQL 事件"""
    trace_id: str
    stage: str
    sql: str
    params: List[Any]
    candidate_count: Optional[int] = None


@dataclass
class SemanticEvent:
    """Semantic Search 事件"""
    trace_id: str
    embedding: Dict[str, Any]
    topN: int
    scoped: bool
    scope: Optional[Dict] = None


@dataclass
class MergeEvent:
    """Merge 事件"""
    trace_id: str
    rerank: Dict[str, Any]
    result_top_k: int


@dataclass
class ResultEvent:
    """Result 事件 (分批)"""
    trace_id: str
    items: List[Dict[str, Any]]
    cursor: Optional[str] = None


@dataclass
class DoneEvent:
    """Done 事件"""
    trace_id: str
    stats: Dict[str, Any]
    reproducibility: Dict[str, Any]


@dataclass
class ErrorEvent:
    """Error 事件"""
    trace_id: str
    code: str
    message: str
    details: Optional[Dict] = None
