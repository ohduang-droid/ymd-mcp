"""JSON 工具"""

import json
from typing import Any, Dict, List


class JSONUtils:
    """JSON 工具类"""
    
    @staticmethod
    def safe_load(json_string: str, default: Any = None) -> Any:
        """安全加载 JSON 字符串"""
        try:
            return json.loads(json_string)
        except (json.JSONDecodeError, TypeError):
            return default
    
    @staticmethod
    def safe_dump(data: Any, default: Any = None) -> str:
        """安全转储为 JSON 字符串"""
        try:
            return json.dumps(data, ensure_ascii=False, indent=2)
        except (TypeError, ValueError):
            return default if default is not None else "{}"
    
    @staticmethod
    def merge(*dicts: Dict[str, Any]) -> Dict[str, Any]:
        """合并多个字典"""
        result = {}
        for d in dicts:
            result.update(d)
        return result
    
    @staticmethod
    def flatten(data: Dict[str, Any], separator: str = ".") -> Dict[str, Any]:
        """扁平化嵌套字典"""
        result = {}
        
        def _flatten(obj: Any, prefix: str = ""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    new_key = f"{prefix}{separator}{key}" if prefix else key
                    _flatten(value, new_key)
            elif isinstance(obj, list):
                for i, item in enumerate(obj):
                    new_key = f"{prefix}{separator}{i}" if prefix else str(i)
                    _flatten(item, new_key)
            else:
                result[prefix] = obj
        
        _flatten(data)
        return result

