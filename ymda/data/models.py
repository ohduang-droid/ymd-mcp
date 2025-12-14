"""ORM + 数据模型"""

from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Any, Dict, Optional, List


@dataclass
class BaseModel:
    """基础数据模型"""
    
    id: Optional[int] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典（用于Supabase插入）"""
        data = asdict(self)
        # 处理datetime序列化
        # Iterate over a list of items to allow modification of dictionary during iteration
        for key, value in list(data.items()):
            if isinstance(value, datetime):
                data[key] = value.isoformat()
            elif value is None:
                # 移除None值，让Supabase使用默认值
                data.pop(key, None)
        return data
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "BaseModel":
        """从字典创建实例"""
        # 处理datetime反序列化
        for key, value in data.items():
            if isinstance(value, str) and key.endswith('_at'):
                try:
                    data[key] = datetime.fromisoformat(value.replace('Z', '+00:00'))
                except (ValueError, AttributeError):
                    pass
        return cls(**data)


@dataclass
class YM(BaseModel):
    """Yield Machine数据模型"""
    
    ym_id: str = None
    name: str = None
    short_desc: Optional[str] = None
    category: Optional[str] = None
    status: str = 'active'
    tags: Optional[Dict[str, Any]] = None
    seed_links: Optional[Dict[str, Any]] = None
    notes: Optional[str] = None


@dataclass
class YMQuestion(BaseModel):
    """问题数据模型"""
    
    question_id: str = None
    question_text: str = None
    type: str = None
    target_field: Optional[str] = None
    importance: Optional[str] = None


@dataclass
class ResearchRun(BaseModel):
    """Deep Research执行记录 - 新版架构
    
    变更说明:
    - 删除: parsed_struct, parsed_text, embedding (迁移到 research_chunk)
    - 新增: status, is_latest, error_message
    - raw_output 仅存储 LLM 原始响应
    """
    
    ym_id: int = None  # 外键指向 ym.id
    ymq_id: int = None # 外键指向 ymq.id
    model_name: str = "sonar-reasoning"
    input_payload: Optional[Dict[str, Any]] = None
    raw_output: Optional[Dict[str, Any]] = None
    
    # 新版字段
    status: str = 'running'  # running / parsed / failed
    is_latest: bool = False  # 同一(ym_id, ymq_id)只有一个true
    error_message: Optional[str] = None
    parsed_ok: bool = False  # 保留用于兼容性
    
    def __post_init__(self):
        if self.input_payload is None:
            self.input_payload = {}
        if self.raw_output is None:
            self.raw_output = {}

@dataclass
class Metric(BaseModel):
    """结构化指标 - 新版架构 (纯事实层)
    
    变更说明:
    - 删除: evidence_text, evidence_sources, embedding (迁移到 research_chunk + metric_provenance)
    - 新增: range_min/max, unit, value_raw, unit_raw, confidence
    - 证据通过 metric_provenance 表关联到 research_chunk
    """
    
    research_run_id: int = None  # 外键指向 research_run.id
    key: str = None  # 必须在 metric_key_registry 中
    
    # 结构化值 (归一化后)
    value_numeric: Optional[float] = None
    range_min: Optional[float] = None
    range_max: Optional[float] = None
    unit: Optional[str] = None  # 标准单位 (USD, hours, months)
    
    # 其他类型值
    value_text: Optional[str] = None
    value_json: Optional[Dict[str, Any]] = None
    
    # 原始值 (审计与溯源)
    value_raw: Optional[str] = None  # LLM 抽取的原始表达
    unit_raw: Optional[str] = None   # LLM 抽取的原始单位
    
    # 置信度
    confidence: Optional[float] = None


@dataclass
class ResearchChunk(BaseModel):
    """研究切片 - v1 架构
    
    从"文本切片"升级为"研究判断对象" (Research Decision Atom)
    
    v1 新增字段:
    - chunk_type: 语义类型（7种枚举）
    - metric_focus: 关联的 metric keys（JSON数组）
    - subsection: 二级分类（可选）
    - chunk_version: 版本标识（v0/v1）
    """
    
    research_run_id: int = None
    chunk_uid: str = None  # 稳定业务ID，如 "rr_123_chunk_007"
    
    # v1 语义字段 ✨
    chunk_type: Optional[str] = None  # numeric_estimate | reasoning | final_judgement | strategy_pattern | risk_analysis | metric_summary_row | background_context
    metric_focus: Optional[List[str]] = None  # 关联的 metric keys，如 ["financial.capex.total"]
    subsection: Optional[str] = None  # 二级分类，如 "financial.capex"
    chunk_version: str = "v1"  # 版本标识 (v0=旧逻辑, v1=新逻辑)
    
    # 分类与来源
    section: Optional[str] = None  # 一级分类：financial | location | machine | risk | market ...
    content: str = None  # chunk 原文
    
    source_kind: Optional[str] = None  # web|doc|manual|internal
    source_url: Optional[str] = None
    source_title: Optional[str] = None
    retrieved_at: Optional[str] = None
    
    embedding: Optional[List[float]] = None  # 向量（可选）



@dataclass
class MetricKeyRegistry(BaseModel):
    """Metric 字段注册表 (SSOT for metric.key)
    
    所有合法的 metric.key 必须在此表中定义
    用于: 字段语义理解、NLA查询生成、Hybrid Search key grounding
    """
    
    key: str = None  # UNIQUE, namespace key (e.g. financial.capex.total)
    canonical_name: str = None  # 人类可读名称 (e.g. "Total CAPEX")
    description: Optional[str] = None  # 字段含义
    value_type: str = None  # numeric / range / text / boolean / json (改名: type → value_type)
    query_capability: Optional[str] = None  # strong_structured / filter_only / describe_only / semantic_only
    unit: Optional[str] = None  # 标准单位 (USD, hours, months)
    constraints: Optional[Dict[str, Any]] = None  # 验证规则
    embedding: Optional[List[float]] = None  # vector(1536) for key grounding



@dataclass
class MetricProvenance(BaseModel):
    """Metric 证据关联表 (物理表)
    
    绑定 metric 与 research_chunk,实现证据可追溯
    """
    
    metric_id: int = None  # 外键指向 metric.id
    research_chunk_id: int = None  # ✅ 外键指向 research_chunk.id (修复FK)
    quote: Optional[str] = None  # 证据引用文本片段
    span_start: Optional[int] = None  # quote在chunk中的起始位置
    span_end: Optional[int] = None  # quote在chunk中的结束位置
    relevance: Optional[float] = None  # 该chunk对该metric的相关性 (0-1)


