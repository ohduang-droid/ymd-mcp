"""
Expected Fields Parser - 统一解析 3 种格式

P0-1 Hard Fix: Registry 模板库约束
- expected_fields 只能引用 registry 中的 keys
- 禁止自带 type/schema
- FieldSpec 最小化: {key, required, role[]}
- role 必须白名单
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class FieldSpec:
    """字段规格 - 最小化设计"""
    key: str          # 必须非空、trim、无空格
    required: bool    # 缺省 false
    role: List[str]   # 白名单: ['filter', 'rank', 'describe']
    
    def __post_init__(self):
        """P0-1: 严格校验"""
        # 清理和校验 key
        self.key = self.key.strip()
        if not self.key:
            raise ValueError("Field key cannot be empty")
        if ' ' in self.key:
            raise ValueError(f"Field key cannot contain spaces: '{self.key}'")
        
        # 校验 role
        for r in self.role:
            if r not in ALLOWED_ROLES:
                raise ValueError(f"Invalid role '{r}'. Allowed: {ALLOWED_ROLES}")


# 全局常量
ALLOWED_ROLES = {'filter', 'rank', 'describe'}


class ParsingError(Exception):
    """解析错误"""
    pass


class ExpectedFieldsParser:
    """
    Expected Fields 解析器
    
    支持 3 种格式:
    1. {"use_fields": [...]}
    2. {"fields": [...]}
    3. 直接数组 [...]
    
    解析后:
    - 去重（同 key 保留最严格 required=true）
    - 严格校验 key/role
    - 忽略 type/schema/desc/example 等冗余字段
    - 为空 → raise ParsingError
    """
    
    def parse(self, expected_fields: Any) -> List[FieldSpec]:
        """
        解析 expected_fields
        
        Args:
            expected_fields: 任意格式的 expected_fields
            
        Returns:
            List[FieldSpec]: 解析后的字段列表
            
        Raises:
            ParsingError: 解析失败
        """
        if not expected_fields:
            raise ParsingError("expected_fields_empty_or_null")
        
        # 识别格式并提取字段列表
        field_list = self._extract_field_list(expected_fields)
        
        if not field_list:
            raise ParsingError("expected_fields_empty_after_parse")
        
        # 解析每个字段
        specs = []
        for idx, field in enumerate(field_list):
            try:
                spec = self._parse_single_field(field)
                specs.append(spec)
            except Exception as e:
                logger.warning(f"Skip invalid field at index {idx}: {e}")
                continue
        
        if not specs:
            raise ParsingError("expected_fields_empty_after_parse")
        
        # P0-1: 去重（同 key 保留最严格 required=true）
        specs = self._deduplicate(specs)
        
        logger.info(f"Parsed {len(specs)} field specs from expected_fields")
        return specs
    
    def _extract_field_list(self, expected_fields: Any) -> List[Dict]:
        """识别格式并提取字段列表"""
        # 格式1: {"use_fields": [...]}
        if isinstance(expected_fields, dict) and 'use_fields' in expected_fields:
            field_list = expected_fields['use_fields']
            if not isinstance(field_list, list):
                raise ParsingError("use_fields must be a list")
            return field_list
        
        # 格式2: {"fields": [...]}
        if isinstance(expected_fields, dict) and 'fields' in expected_fields:
            field_list = expected_fields['fields']
            if not isinstance(field_list, list):
                raise ParsingError("fields must be a list")
            return field_list
        
        # 格式3: 直接数组 [...]
        if isinstance(expected_fields, list):
            return expected_fields
        
        # 无法识别的格式
        raise ParsingError("unknown_format")
    
    def _parse_single_field(self, field: Dict) -> FieldSpec:
        """
        解析单个字段
        
        P0-1 规则:
        - 只提取 key, required, role
        - 忽略 type, schema, description, example 等
        - 禁止 expected_fields 自带 type（type 只能来自 registry）
        """
        if not isinstance(field, dict):
            raise ValueError(f"Field must be a dict, got {type(field)}")
        
        # 提取 key（必须）
        key = field.get('key')
        if not key:
            raise ValueError("Field missing required 'key'")
        
        # P0-1: 禁止自带 type
        if 'type' in field:
            logger.warning(f"Field {key} has 'type' - ignoring (type comes from registry only)")
        
        # 提取 required（缺省 false）
        required = field.get('required', False)
        if not isinstance(required, bool):
            # 尝试转换
            required = str(required).lower() in ('true', '1', 'yes')
        
        # 提取 role（缺省空列表）
        role = field.get('role', [])
        if not isinstance(role, list):
            role = [role] if role else []
        
        return FieldSpec(
            key=key,
            required=required,
            role=role
        )
    
    def _deduplicate(self, specs: List[FieldSpec]) -> List[FieldSpec]:
        """
        去重：同 key 保留最严格的 required=true
        
        规则:
        - 如果同一个 key 出现多次
        - 保留 required=true 的版本
        - 如果都是 true 或都是 false，保留第一个
        - role 合并（去重）
        """
        key_map: Dict[str, FieldSpec] = {}
        
        for spec in specs:
            if spec.key not in key_map:
                key_map[spec.key] = spec
            else:
                existing = key_map[spec.key]
                # 如果新的 required=true，替换
                if spec.required and not existing.required:
                    # 合并 role
                    merged_roles = list(set(existing.role + spec.role))
                    key_map[spec.key] = FieldSpec(
                        key=spec.key,
                        required=True,
                        role=merged_roles
                    )
                elif spec.required == existing.required:
                    # required 相同，合并 role
                    merged_roles = list(set(existing.role + spec.role))
                    existing.role = merged_roles
        
        return list(key_map.values())
