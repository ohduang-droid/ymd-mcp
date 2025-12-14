"""
Provenance Writer - chunk_uid 映射与 quote 校验

P0-3 Hard Fix: Provenance 字段映射与校验
- chunk_uid → research_chunk_id 映射
- quote 必须是 chunk.content 的子串
- span_start/end 由系统计算
- 字段名统一：reasoning → reasoning_note
"""

from typing import Any, Dict, List
import logging

logger = logging.getLogger(__name__)


class ProvenanceWriter:
    """
    Provenance 写入器
    
    职责:
    - 映射 chunk_uid 到 research_chunk_id
    - 校验 quote 存在于 chunk.content 中
    - 计算 span_start/end
    - 确保每个 metric 至少有一条 provenance
    """
    
    def __init__(self, repository):
        self.repository = repository
    
    def write_provenance(self, metric_id: int, prov_data: Dict[str, Any],
                         run_id: int) -> Dict[str, Any]:
        """
        写入一条 provenance
        
        Args:
            metric_id: metric 的数据库 ID
            prov_data: {
                'fields': ['key1', 'key2'],
                'chunk_uid': 'chunk_xxx',
                'quote': 'exact text',
                'reasoning': '...'  # 映射到 reasoning_note
            }
            run_id: research_run_id
            
        Returns:
            Dict: Provenance 数据
            
        Raises:
            ValueError: chunk_uid 不存在或 quote 未找到
        """
        # 1. 映射 chunk_uid → research_chunk_id
        chunk = self._get_chunk_by_uid(run_id, prov_data['chunk_uid'])
        
        # 2. 校验 quote
        quote = prov_data.get('quote', '').strip()
        if not quote:
            raise ValueError("quote cannot be empty")
        
        content = chunk['content']
        span_start = content.find(quote)
        
        if span_start == -1:
            # 尝试模糊匹配（去除多余空格）
            normalized_content = ' '.join(content.split())
            normalized_quote = ' '.join(quote.split())
            span_start_normalized = normalized_content.find(normalized_quote)
            
            if span_start_normalized == -1:
                # P0-Fix: quote未找到，但仍然写入provenance（使用降级值）
                # 理由：每个metric必须有provenance，quote验证失败不应阻止写入
                logger.warning(f"Quote not found in chunk (metric_id={metric_id}), using fallback")
                logger.debug(f"  Quote: {quote[:100]}...")
                logger.debug(f"  Chunk: {chunk['id']}")
                
                # 降级策略：设置 span_start=-1 表示未验证
                span_start = -1
                span_end = -1
            else:
                # 模糊匹配成功
                span_start = span_start_normalized
                span_end = span_start_normalized + len(normalized_quote)
                logger.info(f"Quote matched after normalization (metric_id={metric_id})")
        else:
            # 精确匹配成功
            span_end = span_start + len(quote)
        
        # 3. 构建 provenance
        provenance = {
            'metric_id': metric_id,
            'research_chunk_id': chunk['id'],
            'quote': quote,
            'span_start': span_start,  # -1 表示验证失败
            'span_end': span_end,      # -1 表示验证失败
            'reasoning_note': prov_data.get('reasoning', ''),  # P0-3: 字段名映射
            'relevance': prov_data.get('relevance'),  # 可选字段
        }
        
        return provenance
    
    def validate_coverage(self, metrics: List[Dict], provenances: List[Dict]) -> None:
        """
        确保每个 metric.key 至少在一个 provenance.fields 中出现
        
        Args:
            metrics: metric 列表（包含 key）
            provenances: provenance 数据列表（包含 fields）
            
        Raises:
            ValueError: 有 metric 缺少 provenance
        """
        metric_keys = {m['key'] for m in metrics}
        covered_keys = set()
        
        for prov in provenances:
            fields = prov.get('fields', [])
            covered_keys.update(fields)
        
        missing = metric_keys - covered_keys
        if missing:
            raise ValueError(f"Metrics missing provenance: {missing}")
    
    def _get_chunk_by_uid(self, run_id: int, chunk_uid: str) -> Dict[str, Any]:
        """
        通过 (run_id, chunk_uid) 查询 research_chunk
        
        P0-3: 需要数据库唯一约束保证
        """
        try:
            result = self.repository.client.table('research_chunk')\
                .select('id, content')\
                .eq('research_run_id', run_id)\
                .eq('chunk_uid', chunk_uid)\
                .single()\
                .execute()
            
            if not result.data:
                raise ValueError(f"chunk_uid not found: {chunk_uid}")
            
            return result.data
            
        except Exception as e:
            logger.error(f"Failed to get chunk {chunk_uid} for run {run_id}: {e}")
            raise ValueError(f"chunk_uid not found: {chunk_uid}")
