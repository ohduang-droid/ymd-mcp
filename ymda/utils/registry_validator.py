"""
Registry Validator - 批量验证 keys 并带回完整 registry 信息

P0-1 Hard Fix: Registry 模板库约束
- 批量查询 metric_key_registry
- 返回 matched (带完整 registry 信息) + missing keys
- 校验 registry.type 在支持范围内
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import logging

from ymda.utils.expected_fields_parser import FieldSpec

logger = logging.getLogger(__name__)


@dataclass
class RegistryEntry:
    """Registry 条目 - 完整信息"""
    key: str
    value_type: str                            # numeric/text/json/range/enum/bool/list_text/list_enum (改名: type → value_type)
    canonical_name: str
    description: Optional[str] = None
    allowed_values: Optional[List[str]] = None  # enum 专用
    unit: Optional[str] = None                  # numeric/range 专用
    # 可根据实际 registry 表结构添加更多字段


@dataclass
class ValidationResult:
    """验证结果"""
    matched: List[Tuple[FieldSpec, RegistryEntry]]  # 匹配的字段（带 registry 信息）
    missing: List[str]                               # 缺失的 keys
    unsupported_types: List[Tuple[str, str]]        # (key, value_type) - 不支持的类型


class RegistryValidator:
    """
    Registry 验证器
    
    功能:
    1. 批量查询 metric_key_registry WHERE key IN (...)
    2. 返回 matched (带完整 registry 信息) + missing keys
    3. 校验 registry.value_type 在支持范围内
    """
    
    # P0-2: 支持的 8 种类型
    SUPPORTED_TYPES = {
        'numeric', 'text', 'json', 'range', 'enum',
        'bool', 'list_text', 'list_enum'
    }
    LEGACY_TYPE_MAPPING = {
        'number': 'numeric',
        'float': 'numeric',
        'int': 'numeric',
        'boolean': 'bool'
    }
    
    def __init__(self, repository):
        """
        Args:
            repository: SupabaseRepository 实例
        """
        self.repository = repository
    
    def validate(self, field_specs: List[FieldSpec]) -> ValidationResult:
        """
        批量验证字段
        
        Args:
            field_specs: 解析后的字段列表
            
        Returns:
            ValidationResult: 验证结果
        """
        if not field_specs:
            return ValidationResult(matched=[], missing=[], unsupported_types=[])
        
        # 提取所有 keys
        keys = [spec.key for spec in field_specs]
        
        # 批量查询 registry
        logger.info(f"Validating {len(keys)} keys against registry")
        registry_map = self._batch_query_registry(keys)
        
        # 分类结果
        matched = []
        missing = []
        unsupported_types = []
        
        for spec in field_specs:
            if spec.key not in registry_map:
                missing.append(spec.key)
            else:
                entry = registry_map[spec.key]
                # 校验类型
                if entry.value_type not in self.SUPPORTED_TYPES:
                    unsupported_types.append((spec.key, entry.value_type))
                else:
                    matched.append((spec, entry))
        
        # 日志汇总
        logger.info(f"Registry validation: {len(matched)} matched, "
                   f"{len(missing)} missing, {len(unsupported_types)} unsupported types")
        
        if missing:
            logger.warning(f"Missing keys: {missing}")
        if unsupported_types:
            logger.warning(f"Unsupported types: {unsupported_types}")
        
        return ValidationResult(
            matched=matched,
            missing=missing,
            unsupported_types=unsupported_types
        )
    
    def _batch_query_registry(self, keys: List[str]) -> Dict[str, RegistryEntry]:
        """
        批量查询 registry
        
        Args:
            keys: 要查询的 key 列表
            
        Returns:
            Dict[key -> RegistryEntry]
        """
        try:
            # SELECT * FROM metric_key_registry WHERE key IN (...)
            result = self.repository.client.table('metric_key_registry')\
                .select('*')\
                .in_('key', keys)\
                .execute()
            
            if not result.data:
                return {}
            
            # 构建 map
            registry_map = {}
            for row in result.data:
                entry = self._row_to_entry(row)
                registry_map[entry.key] = entry
            
            logger.debug(f"Found {len(registry_map)} registry entries for {len(keys)} keys")
            return registry_map
            
        except Exception as e:
            logger.error(f"Failed to query registry: {e}")
            return {}
    
    def _row_to_entry(self, row: Dict[str, Any]) -> RegistryEntry:
        """将数据库行转换为 RegistryEntry"""
        # 根据实际表结构调整字段映射
        value_type = row.get('value_type', 'text')
        normalized_type = self.LEGACY_TYPE_MAPPING.get(
            value_type.lower() if isinstance(value_type, str) else value_type,
            value_type
        )
        if normalized_type != value_type:
            logger.debug(
                f"Normalized registry type for {row['key']}: {value_type} -> {normalized_type}"
            )
            value_type = normalized_type
        
        return RegistryEntry(
            key=row['key'],
            value_type=value_type,  # 使用 value_type 字段，缺省 text
            canonical_name=row.get('canonical_name', row['key']),
            description=row.get('description'),
            allowed_values=row.get('allowed_values'),  # 可能是 JSON 数组
            unit=row.get('unit')
        )
