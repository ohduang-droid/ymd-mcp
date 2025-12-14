"""Registry 注册步骤

从 ymq.expected_fields 生成/更新 metric_key_registry
"""

from typing import Any, Dict, List
from ymda.pipeline.steps.validate_step import BaseStep
from ymda.data.models import MetricKeyRegistry
from ymda.data.repository import get_repository
from ymda.services.embedding_service import EmbeddingService
from ymda.settings import Settings
from ymda.utils.logger import get_logger
from ymda.utils.schema_utils import flatten_expected_fields  # 新增

logger = get_logger(__name__)


class RegistryStep(BaseStep):
    """Registry 生成步骤
    
    职责:
    1. 解析 ymq.expected_fields 中的所有 field 定义
    2. 为每个 key 生成/更新 metric_key_registry
    3. 生成 registry embedding (基于 key + canonical_name + description + type + unit)
    """
    
    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.repository = get_repository(settings)
        self.embedding_service = EmbeddingService(settings)
        logger.debug("RegistryStep 初始化成功")
    
    def generate_registry_embedding_text(self, field: Dict[str, Any]) -> str:
        """生成 registry embedding 的输入文本
        
        格式:
        key: financial.capex.total
        name: Total CAPEX
        description: Total upfront investment required to deploy one machine
        type: range
        unit: USD
        
        Args:
            field: expected_fields 中的单个字段定义
            
        Returns:
            格式化的文本
        """
        parts = []
        
        if field.get('key'):
            parts.append(f"key: {field['key']}")
        
        if field.get('canonical_name'):
            parts.append(f"name: {field['canonical_name']}")
        
        if field.get('description'):
            parts.append(f"description: {field['description']}")
        
        if field.get('type'):
            parts.append(f"type: {field['type']}")
        
        if field.get('unit'):
            parts.append(f"unit: {field['unit']}")
        
        if field.get('query_capability'):
            parts.append(f"query_capability: {field['query_capability']}")
        
        return '\n'.join(parts)
    
    def _validate_use_fields(self, use_fields: List[Dict[str, Any]]):
        """校验 use_fields 中的 key 是否全部注册"""
        if not isinstance(use_fields, list) or not use_fields:
            raise ValueError("expected_fields.use_fields 不能为空")
        
        if not self.repository:
            raise ValueError("Repository 未初始化，无法校验 use_fields")
        
        keys = []
        for idx, field in enumerate(use_fields):
            if not isinstance(field, dict):
                raise ValueError(f"use_fields[{idx}] 必须是对象")
            key = field.get('key')
            if not key:
                raise ValueError(f"use_fields[{idx}] 缺少 key")
            keys.append(key)
        
        result = self.repository.client.table('metric_key_registry')\
            .select('key')\
            .in_('key', keys)\
            .execute()
        
        existing = {row['key'] for row in (result.data or [])}
        missing = [k for k in keys if k not in existing]
        if missing:
            raise ValueError(f"use_fields 包含未注册字段: {missing}")
        
        logger.info(f"use_fields 校验通过: {len(keys)} 个字段已在 registry 中")
    
    def process_expected_fields(self, expected_fields: Dict[str, Any]) -> List[MetricKeyRegistry]:
        """处理 expected_fields, 生成 registry 列表（新版：递归展开树状结构）
        
        Args:
            expected_fields: YMQ 的 expected_fields JSONB（树状结构）
            
        Returns:
            MetricKeyRegistry 列表
        """
        if not expected_fields or not isinstance(expected_fields, dict):
            return []
        
        if "use_fields" in expected_fields:
            # use_fields 只进行校验，不生成 registry
            self._validate_use_fields(expected_fields["use_fields"])
            return []
        
        # Step 1: 递归展开树状结构为平铺字段
        flattened = flatten_expected_fields(expected_fields)
        
        if not flattened:
            logger.warning("expected_fields展开后为空")
            return []
        
        logger.info(f"展开 expected_fields: {len(flattened)} 个字段")
        
        registries = []
        
        # Step 2: 遍历平铺后的字段
        for key, field_def in flattened.items():
            try:
                query_capability = field_def.get('query_capability')
                if not query_capability:
                    raise ValueError(f"字段 {key} 缺少 query_capability，无法写入 registry")
                
                # 生成 embedding 输入文本（使用完整field_def）
                embed_text = self.generate_registry_embedding_text({
                    'key': key,
                    **field_def  # 展开所有字段定义
                })
                
                # 生成 embedding
                embedding = None
                try:
                    embedding = self.embedding_service.generate_embedding(embed_text)
                    logger.debug(f"Registry key '{key}' embedding 生成成功")
                except Exception as e:
                    logger.error(f"Registry key '{key}' embedding 生成失败: {e}")
                
                # 创建 MetricKeyRegistry 对象
                registry = MetricKeyRegistry(
                    key=key,
                    canonical_name=field_def.get('canonical_name'),
                    description=field_def.get('description'),
                    value_type=field_def.get('type', 'text'),  # 映射字段: field.type → registry.value_type
                    query_capability=query_capability,
                    unit=field_def.get('unit'),
                    constraints=field_def.get('constraints'),
                    embedding=embedding
                )
                
                registries.append(registry)
                
            except Exception as e:
                logger.error(f"处理 field 失败: {key} - {e}")
                continue
        
        return registries
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """执行 Registry 生成
        
        输入:
            context['question_list']: 问题列表 (含 expected_fields)
            
        输出:
            context['registry_keys_created']: 总key数
        """
        logger.info("开始 Registry 生成步骤")
        
        question_list = context.get("question_list", [])
        
        if not question_list:
            logger.warning("没有问题定义,跳过 Registry 生成")
            context['registry_keys_created'] = 0
            return context
        
        total_keys = 0
        
        for question in question_list:
            try:
                question_id = question.get('question_id') or question.get('key')
                expected_fields = question.get('expected_fields')
                
                if not expected_fields:
                    logger.debug(f"Question {question_id} 没有 expected_fields, 跳过")
                    continue
                
                # 处理 expected_fields
                registries = self.process_expected_fields(expected_fields)
                
                if not registries:
                    logger.debug(f"Question {question_id} 没有生成任何 registry")
                    continue
                
                # Upsert 到数据库
                if self.repository:
                    for registry in registries:
                        try:
                            # 构建 registry 数据
                            data = {
                                'canonical_name': registry.canonical_name,
                                'description': registry.description,
                                'value_type': registry.value_type,  # 使用 value_type 字段
                                'query_capability': registry.query_capability,
                                'unit': registry.unit,
                                'constraints': registry.constraints,
                                'embedding': registry.embedding
                            }
                            
                            success = self.repository.upsert_metric_key_registry(
                                registry.key, 
                                data
                            )
                            
                            if success:
                                total_keys += 1
                                logger.debug(f"✅ Registry key upsert 成功: {registry.key}")
                            else:
                                logger.warning(f"⚠️ Registry key upsert 失败: {registry.key}")
                        
                        except Exception as e:
                            logger.error(f"Upsert registry key 失败 ({registry.key}): {e}")
                            continue
                else:
                    logger.warning("Repository 未初始化,无法保存 registry")
                
            except Exception as e:
                logger.error(f"处理 question 失败: {e}")
                raise
        
        context['registry_keys_created'] = total_keys
        logger.info(f"Registry 生成步骤完成: 共创建/更新 {total_keys} 个 key")
        
        return context
