"""
Chunk 验证器 - v1

实现3个验证规则，确保chunk符合"研究判断最小单元"的要求
"""

import re
from typing import Dict, List, Tuple
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class ChunkValidators:
    """Chunk 验证规则
    
    3个必须通过的验证:
    1. 是否只回答一个问题？
    2. 是否可独立作为证据？
    3. 脱离上下文是否仍清晰？
    """
    
    def validate_all(self, chunk: Dict) -> Tuple[bool, str]:
        """执行所有验证
        
        Args:
            chunk: 待验证的chunk字典
            
        Returns:
            (是否通过, 失败原因)
        """
        content = chunk.get('content', '')
        metric_focus = chunk.get('metric_focus', [])
        
        # 验证1: 单一问题
        if not self.validate_single_question(content):
            return False, "multiple_questions"
        
        # 验证2: 独立证据
        if not self.validate_independent_evidence(content, metric_focus):
            return False, "not_independent"
        
        # 验证3: 上下文独立
        if not self.validate_context_free(content):
            return False, "needs_context"
        
        return True, ""
    
    def validate_single_question(self, content: str) -> bool:
        """验证1: 是否只回答一个问题？
        
        简化判断: 不含过多独立句（≤3个句子）
        """
        if not content.strip():
            return False
        
        # 按句号/问号/叹号拆分
        sentences = re.split(r'[。.!！?？]', content)
        valid_sentences = [s.strip() for s in sentences if s.strip() and len(s.strip()) > 5]
        
        # 允许最多3个句子（1个主要陈述 + 1-2个补充）
        return len(valid_sentences) <= 3
    
    def validate_independent_evidence(self, content: str, metric_focus: List[str]) -> bool:
        """验证2: 是否可独立作为证据？
        
        条件（至少满足一个）:
        - 含有 metric 相关词汇
        - 含有明确数值/判断
        - 长度足够（>20字符）
        """
        if not content.strip() or len(content.strip()) < 20:
            return False
        
        # 检查是否含 metric 提及
        if metric_focus:
            # 提取 metric 的最后一段（如 financial.capex.total → total）
            metric_terms = []
            for m in metric_focus:
                parts = m.split('.')
                if parts:
                    metric_terms.append(parts[-1])  # 最后一段
                    if len(parts) >= 2:
                        metric_terms.append(parts[-2])  # 倒数第二段
            
            # 检查是否含这些术语
            content_lower = content.lower()
            if any(term.lower() in content_lower for term in metric_terms):
                return True
        
        # 检查是否含数值
        if re.search(r'\d+', content):
            return True
        
        # 检查是否含判断性表达
        judgement_indicators = [
            '重要', '关键', '主要', '次要', '建议', '应当', '适合',
            'important', 'key', 'recommend', 'suggest'
        ]
        if any(kw in content for kw in judgement_indicators):
            return True
        
        return False
    
    def validate_context_free(self, content: str) -> bool:
        """验证3: 脱离上下文是否仍清晰？
        
        检查:
        - 不过度使用代词（他/它/这/那）
        - 不缺少主语
        """
        if not content.strip():
            return False
        
        # 统计代词usage
        pronouns = {
            'zh': ['他', '她', '它', '这', '那', '其', '此'],
            'en': ['it', 'this', 'that', 'these', 'those']
        }
        
        pronoun_count = 0
        for lang_pronouns in pronouns.values():
            for p in lang_pronouns:
                # 统计代词出现次数
                pronoun_count += content.count(p)
        
        # 允许少量代词（≤2个），过多说明依赖上下文
        if pronoun_count > 2:
            return False
        
        # 检查是否过短且全是代词开头
        if len(content) < 30:
            first_word = content.strip()[:2]
            if first_word in ['它', '这', '那', 'It', 'Th']:
                return False
        
        return True
