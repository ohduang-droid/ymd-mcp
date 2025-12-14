"""单位归一化工具

LLM 抽取的原始值存储在 value_raw/unit_raw
本模块负责将原始值归一化为标准单位:
- 货币: 统一为 USD
- 时间: 统一为 hours 或 months
- 数值: 解析 k/万/千 等表达
"""

import re
from typing import Optional, Tuple, Dict
from ymda.utils.logger import get_logger

logger = get_logger(__name__)

# 货币汇率配置 (可以从环境变量或配置文件读取)
EXCHANGE_RATES = {
    'USD': 1.0,
    'CNY': 0.14,  # 1 CNY = 0.14 USD (示例汇率,实际应从API获取)
    'EUR': 1.1,
    'GBP': 1.27,
    'JPY': 0.0068,
}

# 时间单位转换 (统一为 hours 或 months)
TIME_UNITS = {
    # 转为 hours
    'hour': 1.0,
    'hours': 1.0,
    'h': 1.0,
    'day': 24.0,
    'days': 24.0,
    'd': 24.0,
    'week': 168.0,
    'weeks': 168.0,
    'w': 168.0,
    
    # 转为 months (分开处理)
    'month': 1.0,
    'months': 1.0,
    'year': 12.0,
    'years': 12.0,
    'y': 12.0,
}


def parse_number_expression(value_raw: str) -> Optional[float]:
    """解析数值表达式
    
    支持:
    - "20k" -> 20000
    - "2万" -> 20000
    - "3.5千" -> 3500
    - "1.2M" -> 1200000
    
    Args:
        value_raw: 原始数值表达
        
    Returns:
        解析后的数值,失败返回None
    """
    if not value_raw:
        return None
    
    try:
        # 清理空格
        value_str = str(value_raw).strip()
        
        # 匹配模式: 数字 + 单位
        # 支持: 20k, 2万, 3.5千, 1.2M
        pattern = r'([\d.]+)\s*([kKmM万千百])?'
        match = re.match(pattern, value_str)
        
        if not match:
            # 尝试直接转换为float
            return float(value_str.replace(',', ''))
        
        num_part = match.group(1)
        unit_part = match.group(2) or ''
        
        base_value = float(num_part)
        
        # 单位映射
        multipliers = {
            'k': 1000,
            'K': 1000,
            'M': 1000000,
            'm': 1000000,
            '千': 1000,
            '万': 10000,
            '百': 100,
        }
        
        multiplier = multipliers.get(unit_part, 1)
        result = base_value * multiplier
        
        logger.debug(f"数值解析: '{value_raw}' -> {result}")
        return result
        
    except Exception as e:
        logger.warning(f"数值解析失败: '{value_raw}' - {e}")
        return None


def normalize_currency(value_raw: str, unit_raw: str) -> Tuple[Optional[float], str]:
    """归一化货币
    
    Args:
        value_raw: 原始数值表达 (e.g. "20k")
        unit_raw: 原始单位 (e.g. "CNY", "USD", "人民币")
        
    Returns:
        (归一化后的数值, 标准单位)
        标准单位固定为 "USD"
    """
    # 解析数值
    numeric_value = parse_number_expression(value_raw)
    if numeric_value is None:
        return None, "USD"
    
    # 识别货币单位
    currency = identify_currency(unit_raw)
    
    # 转换为 USD
    exchange_rate = EXCHANGE_RATES.get(currency, 1.0)
    usd_value = numeric_value * exchange_rate
    
    logger.debug(f"货币归一化: {value_raw} {unit_raw} -> {usd_value} USD")
    return usd_value, "USD"


def identify_currency(unit_raw: str) -> str:
    """识别货币单位
    
    Args:
        unit_raw: 原始单位表达
        
    Returns:
        标准货币代码 (USD, CNY, EUR等)
    """
    if not unit_raw:
        return "USD"  # 默认USD
    
    unit_lower = unit_raw.lower().strip()
    
    # 货币别名映射
    currency_aliases = {
        'usd': 'USD',
        'dollar': 'USD',
        'dollars': 'USD',
        '$': 'USD',
        'cny': 'CNY',
        'rmb': 'CNY',
        '人民币': 'CNY',
        '元': 'CNY',
        'eur': 'EUR',
        'euro': 'EUR',
        '欧元': 'EUR',
        'gbp': 'GBP',
        'pound': 'GBP',
        '英镑': 'GBP',
        'jpy': 'JPY',
        'yen': 'JPY',
        '日元': 'JPY',
    }
    
    return currency_aliases.get(unit_lower, 'USD')


def normalize_time(value_raw: str, unit_raw: str) -> Tuple[Optional[float], str]:
    """归一化时间
    
    统一为:
    - 短期时间 (< 1个月): hours
    - 长期时间 (>= 1个月): months
    
    Args:
        value_raw: 原始数值表达
        unit_raw: 原始时间单位 (e.g. "天", "day", "年", "year")
        
    Returns:
        (归一化后的数值, 标准单位)
        标准单位为 "hours" 或 "months"
    """
    # 解析数值
    numeric_value = parse_number_expression(value_raw)
    if numeric_value is None:
        return None, "hours"
    
    # 识别时间单位
    unit_key = identify_time_unit(unit_raw)
    
    if not unit_key:
        return numeric_value, "hours"
    
    # 判断应该转为 hours 还是 months
    if unit_key in ['month', 'months', 'year', 'years', 'y']:
        # 转为 months
        if unit_key in ['year', 'years', 'y']:
            result = numeric_value * 12
        else:
            result = numeric_value
        standard_unit = "months"
    else:
        # 转为 hours
        multiplier = TIME_UNITS.get(unit_key, 1.0)
        result = numeric_value * multiplier
        standard_unit = "hours"
    
    logger.debug(f"时间归一化: {value_raw} {unit_raw} -> {result} {standard_unit}")
    return result, standard_unit


def identify_time_unit(unit_raw: str) -> Optional[str]:
    """识别时间单位
    
    Args:
        unit_raw: 原始时间单位表达
        
    Returns:
        标准时间单位key,失败返回None
    """
    if not unit_raw:
        return None
    
    unit_lower = unit_raw.lower().strip()
    
    # 时间单位别名
    time_aliases = {
        'hour': 'hour',
        'hours': 'hours',
        'h': 'h',
        '小时': 'hour',
        'day': 'day',
        'days': 'days',
        'd': 'd',
        '天': 'day',
        'week': 'week',
        'weeks': 'weeks',
        'w': 'w',
        '周': 'week',
        'month': 'month',
        'months': 'months',
        'm': 'month',
        '月': 'month',
        'year': 'year',
        'years': 'years',
        'y': 'y',
        '年': 'year',
    }
    
    return time_aliases.get(unit_lower)


def normalize_unit(
    value_raw: str,
    unit_raw: str,
    expected_type: str = 'numeric'
) -> Tuple[Optional[float], Optional[str]]:
    """统一入口: 根据类型归一化单位
    
    Args:
        value_raw: 原始数值表达
        unit_raw: 原始单位
        expected_type: 期望类型 (numeric, currency, time)
        
    Returns:
        (归一化后的数值, 标准单位)
    """
    if not value_raw:
        return None, None
    
    # 根据类型分发
    if expected_type == 'currency' or is_currency_unit(unit_raw):
        return normalize_currency(value_raw, unit_raw)
    elif expected_type == 'time' or is_time_unit(unit_raw):
        return normalize_time(value_raw, unit_raw)
    else:
        # 纯数值,只解析表达式
        numeric = parse_number_expression(value_raw)
        return numeric, unit_raw


def is_currency_unit(unit_raw: str) -> bool:
    """判断是否为货币单位"""
    if not unit_raw:
        return False
    unit_lower = unit_raw.lower().strip()
    currency_keywords = ['usd', 'cny', 'eur', 'gbp', 'jpy', 'dollar', 'rmb', '元', '美元', '$', '¥']
    return any(kw in unit_lower for kw in currency_keywords)


def is_time_unit(unit_raw: str) -> bool:
    """判断是否为时间单位"""
    if not unit_raw:
        return False
    unit_lower = unit_raw.lower().strip()
    time_keywords = ['hour', 'day', 'week', 'month', 'year', '小时', '天', '周', '月', '年', 'h', 'd', 'w', 'm', 'y']
    return any(kw in unit_lower for kw in time_keywords)
