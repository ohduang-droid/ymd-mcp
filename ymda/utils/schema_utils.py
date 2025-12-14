"""Schema utilities for tree-based expected_fields parsing."""

from typing import Dict, Any
from ymda.utils.logger import get_logger

logger = get_logger(__name__)

# Registry fields are authored once in the tree DSL, so we require a stable set
# of attributes for every leaf node.
REQUIRED_LEAF_FIELDS = ["canonical_name", "description", "type", "query_capability"]
VALID_TYPES = {"number", "range", "text", "enum", "boolean", "json"}
VALID_QUERY_CAPABILITIES = {
    "strong_structured",
    "filter_only",
    "describe_only",
    "semantic_only"
}


def is_leaf_node(node: Any) -> bool:
    """判断是否为叶子节点"""
    return isinstance(node, dict) and "canonical_name" in node


def flatten_expected_fields(tree: Dict[str, Any], prefix: str = "") -> Dict[str, Dict[str, Any]]:
    """递归展开expected_fields树状结构为平铺映射
    
    将嵌套的树状结构转换为 {key: field_def} 的平铺字典
    只提取叶子节点（包含canonical_name的节点）
    
    示例输入（树状结构）:
    {
      "financial": {
        "capex": {
          "total": {
            "canonical_name": "Total CAPEX",
            "description": "...",
            "type": "range",
            "unit": "USD",
            "required": true
          }
        }
      }
    }
    
    示例输出:
    {
      "financial.capex.total": {
        "canonical_name": "Total CAPEX",
        "description": "...",
        "type": "range",
        "unit": "USD",
        "required": true
      }
    }
    
    Args:
        tree: 树状expected_fields结构
        prefix: 当前路径前缀（递归用）
        
    Returns:
        平铺的字段映射 {key: field_definition}
    """
    if not isinstance(tree, dict):
        logger.warning(f"expected_fields不是字典类型: {type(tree)}")
        return {}
    
    result = {}
    
    for key, value in tree.items():
        current_path = f"{prefix}.{key}" if prefix else key
        
        if is_leaf_node(value):
            missing_fields = [f for f in REQUIRED_LEAF_FIELDS if not value.get(f)]
            if missing_fields:
                raise ValueError(
                    f"字段 '{current_path}' 缺少必需字段: {missing_fields}"
                )
            
            field_type = value.get("type")
            if field_type not in VALID_TYPES:
                raise ValueError(
                    f"字段 '{current_path}' 的type '{field_type}' 不合法，"
                    f"允许值: {sorted(VALID_TYPES)}"
                )
            
            capability = value.get("query_capability")
            if capability not in VALID_QUERY_CAPABILITIES:
                raise ValueError(
                    f"字段 '{current_path}' 的 query_capability '{capability}' 不合法，"
                    f"允许值: {sorted(VALID_QUERY_CAPABILITIES)}"
                )
            
            # 默认 required=true，除非显式声明
            if 'required' not in value:
                value['required'] = True
            
            result[current_path] = value
            logger.debug(f"展开字段: {current_path} (type={field_type})")
        elif isinstance(value, dict):
            result.update(flatten_expected_fields(value, current_path))
        else:
            logger.warning(
                f"路径 '{current_path}' 的值既不是叶子节点也不是字典: {type(value)}"
            )
    
    return result


def validate_flattened_schema(flattened: Dict[str, Dict[str, Any]]) -> bool:
    """验证平铺后的schema是否合法
    
    Args:
        flattened: 平铺的字段映射
        
    Returns:
        是否合法
    """
    if not flattened:
        logger.warning("展开后的schema为空")
        return False
    
    for key, field_def in flattened.items():
        # 检查必需字段
        if "canonical_name" not in field_def:
            logger.error(f"字段 '{key}' 缺少 canonical_name")
            return False
        
        if "type" not in field_def:
            logger.error(f"字段 '{key}' 缺少 type")
            return False
        
        # 检查type合法性
        valid_types = ["number", "range", "text", "enum", "boolean", "json"]
        if field_def["type"] not in valid_types:
            logger.error(
                f"字段 '{key}' 的type '{field_def['type']}' 不合法"
            )
            return False
    
    logger.debug(f"Schema验证通过: {len(flattened)} 个字段")
    return True


def build_extractor_schema(flattened: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    """从平铺schema构建ExtractorAgent使用的schema
    
    Extractor不需要知道树状结构，只需要字段列表
    
    Args:
        flattened: 平铺的字段映射
        
    Returns:
        Extractor使用的schema格式
    """
    fields = []
    
    for key, field_def in flattened.items():
        fields.append({
            "key": key,
            "canonical_name": field_def.get("canonical_name"),
            "description": field_def.get("description"),
            "type": field_def.get("type"),
            "unit": field_def.get("unit"),
            "required": field_def.get("required", True)
        })
    
    return {"fields": fields}
