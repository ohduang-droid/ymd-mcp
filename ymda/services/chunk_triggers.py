"""
Chunk 拆分触发器 - v1

实现4个硬性拆分触发条件（T1-T4），优先级高于 chunk_type 判定
"""

import re
from typing import Dict, List, Tuple
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class ChunkTriggers:
    """硬性拆分触发器
    
    按优先级顺序检查：T1 → T2 → T3 → T4
    触发即拆分，不可跳过
    """
    
    def __init__(self):
        # 判断关键词
        self.judgement_keywords = [
            '最重要', '决定性', '权重', '首要', '关键',
            'most important', 'critical', 'key factor', 'primary'
        ]
    
    def check_all(self, content: str, metric_focus: List[str]) -> Tuple[bool, str, List[str]]:
        """检查所有触发器
        
        Args:
            content: chunk内容
            metric_focus: 关联的metric keys
            
        Returns:
            (是否触发, 触发器名称, 拆分建议)
        """
        # T1: 出现第二个独立数值
        if self.check_t1_multiple_numbers(content):
            return True, "T1_MULTIPLE_NUMBERS", self._suggest_split_by_numbers(content)
        
        # T2: 数值 + 判断同句
        if self.check_t2_number_and_judgement(content):
            return True, "T2_NUMBER_AND_JUDGEMENT", self._suggest_split_by_judgement(content)
        
        # T3: base 与 elasticity 混合
        if self.check_t3_base_and_elasticity(metric_focus):
            return True, "T3_BASE_AND_ELASTICITY", self._suggest_split_by_metric_type(content, metric_focus)
        
        # T4: 表格 + 解释
        if self.check_t4_table_and_explanation(content):
            return True, "T4_TABLE_AND_EXPLANATION", self._suggest_split_by_table(content)
        
        return False, "", []
    
    def check_t1_multiple_numbers(self, content: str) -> bool:
        """T1: 出现第二个独立数值
        
        匹配模式:
        - 数字+单位: 123%, $456, 7.8k
        - 区间: 200-400, 10~20
        - 带单位的数值
        """
        # 正则匹配数值模式
        patterns = [
            r'\$\s*\d+[\d,]*\.?\d*[kKmMbB]?',  # $123, $1.5k, $2M
            r'\d+\.?\d*\s*%',  # 10%, 0.5%
            r'\d+[\d,]*\.?\d*\s*(?:USD|CNY|RMB|美元|元)',  # 123 USD, 456元
            r'\d+\.?\d*\s*[-–~]\s*\d+\.?\d*',  # 200-400, 10~20
        ]
        
        numbers = []
        for pattern in patterns:
            numbers.extend(re.findall(pattern, content))
        
        # 至少2个不同的数值
        unique_numbers = list(set(numbers))
        return len(unique_numbers) >= 2
    
    def check_t2_number_and_judgement(self, content: str) -> bool:
        """T2: 数值 + 判断同句
        
        条件: 同时含数值 AND 判断关键词
        """
        has_number = bool(re.search(r'\d+', content))
        has_judgement = any(kw in content for kw in self.judgement_keywords)
        
        return has_number and has_judgement
    
    def check_t3_base_and_elasticity(self, metric_focus: List[str]) -> bool:
        """T3: base 与 elasticity 混合
        
        检查 metric_focus 中是否同时含 .base 和 .elasticity
        """
        if not metric_focus:
            return False
        
        has_base = any('.base' in m for m in metric_focus)
        has_elasticity = any('elasticity' in m for m in metric_focus)
        
        return has_base and has_elasticity
    
    def check_t4_table_and_explanation(self, content: str) -> bool:
        """T4: 表格 + 解释
        
        简化判断:
        - 含表格标记（| 或 tab）
        - 含解释关键词
        """
        has_table = '|' in content or '\t' in content
        has_explanation = any(kw in content for kw in 
            ['说明', '解释', '注', 'note', 'explanation', '表示', '指'])
        
        return has_table and has_explanation
    
    # ========== 拆分建议方法 ==========
    
    def _suggest_split_by_numbers(self, content: str) -> List[str]:
        """按句子拆分（针对T1）"""
        # 简化：按句号拆分
        sentences = re.split(r'[。.!！]', content)
        return [s.strip() for s in sentences if s.strip() and re.search(r'\d+', s)]
    
    def _suggest_split_by_judgement(self, content: str) -> List[str]:
        """拆分为数值部分 + 判断部分（针对T2）"""
        chunks = []
        
        # 查找判断部分
        for kw in self.judgement_keywords:
            if kw in content:
                # 简化：以关键词为分割点
                parts = content.split(kw, 1)
                if len(parts) == 2:
                    # 数值部分
                    if parts[0].strip() and re.search(r'\d+', parts[0]):
                        chunks.append(parts[0].strip())
                    # 判断部分
                    chunks.append(kw + parts[1].strip())
                break
        
        return chunks if len(chunks) > 1 else [content]
    
    def _suggest_split_by_metric_type(self, content: str, metric_focus: List[str]) -> List[str]:
        """按 metric 类型拆分（针对T3）"""
        # 简化：按句子拆分，每个chunk关联不同类型的metric
        chunks = []
        
        base_metrics = [m for m in metric_focus if '.base' in m]
        elasticity_metrics = [m for m in metric_focus if 'elasticity' in m]
        
        sentences = re.split(r'[。.!！]', content)
        
        for s in sentences:
            s = s.strip()
            if not s:
                continue
            
            # 简单判断：含"增长/提升/变化"的句子 → elasticity
            if any(kw in s for kw in ['增长', '提升', '变化', 'increase', 'growth']):
                chunks.append(s)  # elasticity chunk
            elif re.search(r'\d+', s):
                chunks.append(s)  # base chunk
        
        return chunks if chunks else [content]
    
    def _suggest_split_by_table(self, content: str) -> List[str]:
        """拆分表格 + 解释（针对T4）"""
        # 简化：按换行拆分，表格行 vs 说明行
        lines = content.split('\n')
        
        table_lines = []
        explanation_lines = []
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            if '|' in line or '\t' in line:
                table_lines.append(line)
            else:
                explanation_lines.append(line)
        
        chunks = []
        if table_lines:
            chunks.append('\n'.join(table_lines))
        if explanation_lines:
            chunks.append(' '.join(explanation_lines))
        
        return chunks if len(chunks) > 1 else [content]
