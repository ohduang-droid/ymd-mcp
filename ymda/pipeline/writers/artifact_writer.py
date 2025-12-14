"""
Artifact Writer - research_artifact 写入

P0-5 Hard Fix: 修复字段拼写错误，统一序列化
P1-1: Registry version 明确来源
"""

import json
import hashlib
from typing import Any, Dict, List
import logging

from ymda.utils.expected_fields_parser import FieldSpec

logger = logging.getLogger(__name__)


class ArtifactWriter:
    """
    Artifact 写入器
    
    P0-5 修正：修复字段拼写错误，统一序列化
    """
    
    def __init__(self, repository):
        self.repository = repository
    
    def write_artifact(self, run_id: int, ym_id: int, ymq_id: int,
                       metrics: List[Dict], field_specs: List[FieldSpec],
                       extractor_model: str) -> Dict[str, Any]:
        """
        写入 research_artifact
        
        Args:
            run_id: research_run_id
            ym_id: ym 数据库 ID
            ymq_id: ymq 数据库 ID
            metrics: 已保存的 metric 列表（包含 id 和 key）
            field_specs: 期望字段列表
            extractor_model: 使用的 extractor 模型名
            
        Returns:
            Dict: Artifact 数据
        """
        extracted_keys = {m['key'] for m in metrics}
        required_keys = {fs.key for fs in field_specs if fs.required}
        missing_required = required_keys - extracted_keys
        
        # P0-5: 先构建 dict，确保纯 ASCII 字段名
        content_dict = {
            'metric_ids': [m['id'] for m in metrics],
            'ym_id': ym_id,
            'ymq_id': ymq_id,
            'extracted_field_count': len(metrics),
            'required_missing_count': len(missing_required),
            'required_missing_keys': list(missing_required),  # P0-5: 修正拼写（无零宽字符）
            'registry_version': self._get_registry_version(),  # P1-1
            'extractor_model_name': extractor_model,
            'extractor_prompt_hash': self._hash_extractor_prompt()
        }
        
        # 统一 json.dumps
        artifact = {
            'research_run_id': run_id,
            'kind': 'metric_set',
            'content': json.dumps(content_dict, ensure_ascii=True)  # P0-5: 强制 ASCII
        }
        
        return artifact
    
    def _get_registry_version(self) -> str:
        """
        P1-1: Registry version 明确来源
        
        使用 max(updated_at) 作为 version
        """
        try:
            result = self.repository.client.table('metric_key_registry')\
                .select('updated_at')\
                .order('updated_at', desc=True)\
                .limit(1)\
                .execute()
            
            if result.data:
                return result.data[0]['updated_at']
            
            return 'unknown'
            
        except Exception as e:
            logger.warning(f"Failed to get registry version: {e}")
            return 'unknown'
    
    def _hash_extractor_prompt(self) -> str:
        """
        计算 extractor prompt 的哈希值（用于可复现性）
        
        实际实现中应该从 ExtractorAgent 获取 prompt 模板
        """
        # TODO: 从 ExtractorAgent 获取实际 prompt
        prompt_text = "EXTRACTOR_PROMPT_TEMPLATE_V1"  # 占位符
        return hashlib.sha256(prompt_text.encode()).hexdigest()[:16]
