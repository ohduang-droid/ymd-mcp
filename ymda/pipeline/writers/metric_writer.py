"""
Metric Writer - Registry 驱动的类型解析与写入

P0-2 Hard Fix: Registry 驱动的类型校验与解析
P0-3 Hard Fix: 解析失败处理规则
P0-4 Hard Fix: list 类型约束
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
import re
import logging

from ymda.utils.registry_validator import RegistryEntry

logger = logging.getLogger(__name__)


@dataclass
class ParsedNumeric:
    """解析后的数值"""
    value: float
    unit: Optional[str] = None


@dataclass
class ParsedRange:
    """解析后的范围"""
    min: float
    max: float
    unit: Optional[str] = None


class MetricWriter:
    """
    Metric 写入器 - Registry 驱动
    
    P0-1 修正：移除所有 TYPE_ROUTING，使用专用解析函数
    P0-3: 解析失败时的处理规则
    P0-4: list 类型约束
    """
    
    def write_metric(self, key: str, value: Any, registry_entry: RegistryEntry,
                     run_id: int, is_required: bool) -> Optional[Dict[str, Any]]:
        """
        根据 registry.type 调用专用解析函数
        永远写入 value_raw (审计用)
        
        Args:
            key: metric key
            value: 原始值
            registry_entry: registry 条目
            run_id: research_run_id
            is_required: 是否为 required 字段
            
        Returns:
            - Dict: Metric 数据（解析成功）
            - None: 解析失败且 required=true（调用者应计入 required_missing）
        """
        metric = {
            'research_run_id': run_id,
            'key': key,
            'value_raw': str(value),  # 永远保存原始值
            # 以下字段根据 type 填充
            'value_numeric': None,
            'value_text': None,
            'value_json': None,
            'range_min': None,
            'range_max': None,
            'unit': None
        }
        
        try:
            if registry_entry.value_type == 'numeric':
                result = self._parse_numeric(value)
                if result is None:
                    return self._handle_parse_failure(metric, is_required, 'numeric')
                metric['value_numeric'] = result.value
                metric['unit'] = result.unit
                
            elif registry_entry.value_type == 'range':
                result = self._parse_range(value)
                if result is None:
                    return self._handle_parse_failure(metric, is_required, 'range')
                # P1-3: 验证 min <= max
                if result.min > result.max:
                    logger.warning(f"Range min > max for {key}: {result.min} > {result.max}")
                    return self._handle_parse_failure(metric, is_required, 'range')
                metric['range_min'] = result.min
                metric['range_max'] = result.max
                metric['unit'] = result.unit
                
            elif registry_entry.value_type == 'enum':
                result = self._parse_enum(value, registry_entry.allowed_values, is_required, key)
                if result is None:
                    return None  # required enum 不在 allowed_values
                metric['value_text'] = result
                
            elif registry_entry.value_type == 'bool':
                metric['value_text'] = self._parse_bool(value)
                
            elif registry_entry.value_type == 'list_text':
                result = self._parse_list_text(value, key)
                if result is None:
                    return self._handle_parse_failure(metric, is_required, 'list_text')
                metric['value_json'] = result
                
            elif registry_entry.value_type == 'list_enum':
                result = self._parse_list_enum(value, registry_entry.allowed_values, is_required, key)
                if result is None:
                    return None  # required list_enum 包含非法值
                metric['value_json'] = result
                
            elif registry_entry.value_type == 'text':
                metric['value_text'] = str(value)
                
            elif registry_entry.value_type == 'json':
                if not isinstance(value, (dict, list)):
                    raise ValueError(f"json type requires object/array, got: {type(value)}")
                metric['value_json'] = value
            
            else:
                raise ValueError(f"Unsupported type: {registry_entry.value_type}")
            
            return metric
            
        except Exception as e:
            logger.error(f"Parse error for {key}: {e}")
            return self._handle_parse_failure(metric, is_required, registry_entry.value_type)
    
    def _handle_parse_failure(self, metric: Dict, is_required: bool, type_name: str) -> Optional[Dict]:
        """
        P0-3: 解析失败处理规则
        
        - required=true: 不写 metric，返回 None
        - required=false: 降级为 value_text（带 warning）
        """
        if is_required:
            # required=true: 不写 metric，返回 None
            logger.warning(f"Required field {metric['key']} parse failed ({type_name})")
            return None
        else:
            # required=false: 降级为 value_text（带 warning）
            logger.warning(f"Optional field {metric['key']} parse failed, fallback to text")
            metric['value_text'] = metric['value_raw']
            return metric
    
    # ==================== 解析函数 ====================
    
    def _parse_numeric(self, value: Any) -> Optional[ParsedNumeric]:
        """
        字符串解析 + 单位提取:
        - "20-40" → 取下界 20
        - "$20k" → 20000, unit="USD"
        - "10%" → 10, unit="%"
        - "~200" / "about 200" → 200
        - "10-12 months" → 10, unit="months"
        
        失败 → 返回 None
        """
        if isinstance(value, (int, float)):
            return ParsedNumeric(value=float(value))
        
        value_str = str(value).strip()
        
        # 模式1: 百分比 "10%"
        if '%' in value_str:
            match = re.search(r'([\d.]+)\s*%', value_str)
            if match:
                return ParsedNumeric(value=float(match.group(1)), unit='%')
        
        # 模式2: 货币 "$20k" / "¥1000"
        currency_match = re.match(r'[$¥€£]\s*([\d.]+)\s*([km])?', value_str, re.I)
        if currency_match:
            num_str, suffix = currency_match.groups()
            value_num = float(num_str)
            if suffix:
                suffix = suffix.lower()
                if suffix == 'k':
                    value_num *= 1000
                elif suffix == 'm':
                    value_num *= 1000000
            # 尝试识别货币
            if value_str.startswith('$'):
                unit = 'USD'
            elif value_str.startswith('¥'):
                unit = 'CNY'
            elif value_str.startswith('€'):
                unit = 'EUR'
            elif value_str.startswith('£'):
                unit = 'GBP'
            else:
                unit = None
            return ParsedNumeric(value=value_num, unit=unit)
        
        # 模式3: 数字 + k/m/b 后缀 "20k"
        km_match = re.match(r'~?\s*about\s*|approximately\s*)??([\d.,]+)\s*([kmb])\b', value_str, re.I)
        if km_match:
            num_str, suffix = km_match.groups()[-2:]  # 取最后两组
            num_str = num_str.replace(',', '')
            value_num = float(num_str)
            suffix = suffix.lower()
            if suffix == 'k':
                value_num *= 1000
            elif suffix == 'm':
                value_num *= 1000000
            elif suffix == 'b':
                value_num *= 1000000000
            return ParsedNumeric(value=value_num)
        
        # 模式4: 范围 "20-40" → 取下界
        range_match = re.match(r'([\d.]+)\s*[-–]\s*([\d.]+)', value_str)
        if range_match:
            lower = float(range_match.group(1))
            return ParsedNumeric(value=lower)
        
        # 模式5: 数字 + 单位 "10 months" / "200 CNY"
        unit_match = re.match(r'~?\s*(?:about\s+|approximately\s+)?([\d.,]+)\s+([a-zA-Z]+)', value_str, re.I)
        if unit_match:
            num_str, unit = unit_match.groups()
            num_str = num_str.replace(',', '')
            return ParsedNumeric(value=float(num_str), unit=unit)
        
        # 模式6: 纯数字（可能有逗号、波浪号）
        clean_str = re.sub(r'[~,\s]|about|approximately', '', value_str, flags=re.I).strip()
        try:
            return ParsedNumeric(value=float(clean_str))
        except ValueError:
            pass
        
        # 解析失败
        return None
    
    def _parse_range(self, value: Any) -> Optional[ParsedRange]:
        """
        支持多种格式:
        - {"min": 200, "max": 1500, "unit": "CNY"}
        - {"lo": 10, "hi": 12}
        - 字符串: "200-1500" / "200 to 1500"
        
        失败 → 返回 None
        """
        # 格式1: dict
        if isinstance(value, dict):
            # 尝试 min/max
            if 'min' in value and 'max' in value:
                try:
                    return ParsedRange(
                        min=float(value['min']),
                        max=float(value['max']),
                        unit=value.get('unit')
                    )
                except (ValueError, TypeError):
                    pass
            
            # 尝试 lo/hi
            if 'lo' in value and 'hi' in value:
                try:
                    return ParsedRange(
                        min=float(value['lo']),
                        max=float(value['hi']),
                        unit=value.get('unit')
                    )
                except (ValueError, TypeError):
                    pass
        
        # 格式2: 字符串 "200-1500" / "200 to 1500"
        if isinstance(value, str):
            # 模式: "200-1500" / "200–1500" (em dash)
            match = re.match(r'([\d.,]+)\s*[-–]\s*([\d.,]+)(?:\s+([a-zA-Z]+))?', value.strip())
            if match:
                try:
                    min_str, max_str, unit = match.groups()
                    return ParsedRange(
                        min=float(min_str.replace(',', '')),
                        max=float(max_str.replace(',', '')),
                        unit=unit
                    )
                except ValueError:
                    pass
            
            # 模式: "200 to 1500"
            match = re.match(r'([\d.,]+)\s+to\s+([\d.,]+)(?:\s+([a-zA-Z]+))?', value.strip(), re.I)
            if match:
                try:
                    min_str, max_str, unit = match.groups()
                    return ParsedRange(
                        min=float(min_str.replace(',', '')),
                        max=float(max_str.replace(',', '')),
                        unit=unit
                    )
                except ValueError:
                    pass
        
        return None
    
    def _parse_enum(self, value: Any, allowed_values: Optional[List[str]],
                    is_required: bool, key: str) -> Optional[str]:
        """P1-2: Enum 校验"""
        value_str = str(value)
        
        if allowed_values:
            if value_str not in allowed_values:
                if is_required:
                    logger.warning(f"Required enum {key}={value_str} not in {allowed_values}")
                    return None  # 视为 missing
                else:
                    logger.warning(f"Optional enum {key}={value_str} not in allowed list, writing anyway")
        
        return value_str
    
    def _parse_bool(self, value: Any) -> str:
        """布尔值统一为 'true'/'false'"""
        if isinstance(value, bool):
            return 'true' if value else 'false'
        
        value_str = str(value).lower().strip()
        if value_str in ('true', '1', 'yes', 'y'):
            return 'true'
        elif value_str in ('false', '0', 'no', 'n'):
            return 'false'
        else:
            return value_str  # 保持原样
    
    def _parse_list_text(self, value: Any, key: str) -> Optional[List[str]]:
        """P0-4: list_text 约束"""
        if not isinstance(value, list):
            logger.warning(f"{key}: list_text requires array, got {type(value)}")
            return None
        
        if len(value) == 0:
            logger.debug(f"{key}: empty array treated as missing")
            return None  # 空数组视为 missing
        
        if not all(isinstance(item, str) for item in value):
            logger.warning(f"{key}: list_text contains non-string items")
            return None
        
        return value
    
    def _parse_list_enum(self, value: Any, allowed_values: Optional[List[str]],
                         is_required: bool, key: str) -> Optional[List[str]]:
        """P0-4: list_enum 约束"""
        if not isinstance(value, list):
            logger.warning(f"{key}: list_enum requires array, got {type(value)}")
            return None
        
        if len(value) == 0:
            logger.debug(f"{key}: empty array treated as missing")
            return None  # 空数组视为 missing
        
        # 每一项必须在 allowed_values
        if allowed_values:
            invalid = [v for v in value if v not in allowed_values]
            if invalid:
                if is_required:
                    logger.warning(f"Required {key}: list_enum contains invalid values: {invalid}")
                    return None
                else:
                    logger.warning(f"Optional {key}: list_enum has invalid values {invalid}, writing anyway")
        
        return value
