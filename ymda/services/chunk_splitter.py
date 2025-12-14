"""
Chunk 拆分器 v1 - 核心逻辑

将 ResearchStep 产出的 provenance 证据，经由硬性触发器与稳定规则，
整理为可被 YMD / NLA / SQL 精确使用的研究判断原子。
"""

import re
from typing import Any, Dict, List, Optional
from ymda.data.models import ResearchChunk
from ymda.services.chunk_triggers import ChunkTriggers
from ymda.services.chunk_validators import ChunkValidators
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


# Chunk类型枚举
CHUNK_TYPES = {
    'numeric_estimate': '可直接填字段的数值/比例',
    'reasoning': '数值或结论背后的因果解释',
    'final_judgement': '主次/权重/谁更重要',
    'strategy_pattern': '行动或配置建议',
    'risk_analysis': '成本、风险、约束',
    'metric_summary_row': '表格中的单一高可信行',
    'background_context': '不参与决策的背景'
}

# Section分类（一级）
SECTIONS = [
    'financial', 'location', 'machine', 'risk', 'market',
    'operator', 'landlord', 'platform', 'conclusion'
]


class ChunkSplitter:
    """
    YMD Chunk 拆分器 v1
    
    职责:
    - 将 provenance.evidence_text 转换为 ResearchChunk
    - 应用硬性拆分触发器（T1-T4）
    - 判定 chunk_type
    - 分类 section/subsection
    - 补充拆分 raw_answer_text
    - 验证chunk质量
    """
    
    def __init__(self, metric_key_registry: List[str]):
        """
        Args:
            metric_key_registry: 所有合法的 metric keys
        """
        self.metric_keys = metric_key_registry
        self.triggers = ChunkTriggers()
        self.validators = ChunkValidators()
        
        logger.info(f"ChunkSplitter v1 初始化: {len(metric_key_registry)} metric keys")
    
    def split(self,
              research_run_id: int,
              raw_answer_text: str,
              provenance: List[Dict],
              metric_key_registry: List[str]) -> List[ResearchChunk]:
        """
        主入口：拆分 research 结果为 chunks
        
        Args:
            research_run_id: research_run ID
            raw_answer_text: 原始报告文本
            provenance: LLM生成的证据列表 [{fields, evidence_text}, ...]
            metric_key_registry: metric keys列表（可选，覆盖初始化的）
            
        Returns:
            ResearchChunk 列表
        """
        if metric_key_registry:
            self.metric_keys = metric_key_registry
        
        logger.info(f"开始拆分 run_id={research_run_id}, provenance={len(provenance)} 条")
        
        # Phase 1: 基于 provenance 生成候选 chunk
        candidate_chunks = self._phase1_from_provenance(research_run_id, provenance)
        logger.debug(f"Phase 1: 生成 {len(candidate_chunks)} 个候选chunk")
        
        # Phase 2: 应用硬性拆分触发器
        triggered_chunks = self._phase2_apply_triggers(candidate_chunks)
        logger.debug(f"Phase 2: 触发器拆分后 {len(triggered_chunks)} 个chunk")
        
        # Phase 3: metric_focus 修正
        corrected_chunks = self._phase3_correct_metric_focus(triggered_chunks)
        logger.debug(f"Phase 3: metric_focus 修正完成")
        
        # Phase 4: chunk_type 判定
        typed_chunks = self._phase4_determine_chunk_type(corrected_chunks)
        logger.debug(f"Phase 4: chunk_type 判定完成")
        
        # Phase 5: section/subsection 分类
        classified_chunks = self._phase5_classify_section(typed_chunks)
        logger.debug(f"Phase 5: section 分类完成")
        
        # Phase 6: 验证
        validated_chunks = self._phase6_validate(classified_chunks)
        logger.debug(f"Phase 6: 验证后 {len(validated_chunks)} 个chunk")
        
        # ✨ 转换 validated_chunks 为 ResearchChunk 对象
        validated_research_chunks = self.convert_to_research_chunks(validated_chunks)
        
        # Phase 7: 补充拆分 raw_answer_text（辅路径，20%）
        background_chunks = self._phase7_supplement_raw_text(
            research_run_id, raw_answer_text, validated_chunks  # 仍传入Dict列表用于去重
        )
        logger.debug(f"Phase 7: 补充 {len(background_chunks)} 个背景chunk")
        
        all_chunks = validated_research_chunks + background_chunks
        logger.info(f"拆分完成: 共 {len(all_chunks)} 个chunk (主路径: {len(validated_research_chunks)}, 辅路径: {len(background_chunks)})")
        
        return all_chunks
    
    # ========== Phase 1: 从 provenance 生成候选 chunk ==========
    
    def _phase1_from_provenance(self, run_id: int, provenance: List[Dict]) -> List[Dict]:
        """Phase 1: 为每个 provenance 条目创建一个初始 chunk
        
        逻辑:
        - content = provenance.evidence_text
        - metric_focus = provenance.fields
        - chunk_uid = f"rr_{run_id}_prov_{idx:04d}"
        """
        candidate_chunks = []
        
        for idx, prov in enumerate(provenance):
            evidence_text = prov.get('evidence_text', '').strip()
            fields = prov.get('fields', [])
            
            if not evidence_text:
                logger.warning(f"Provenance {idx} 缺少 evidence_text，跳过")
                continue
            
            chunk = {
                'research_run_id': run_id,
                'chunk_uid': f"rr_{run_id}_prov_{idx:04d}",
                'content': evidence_text,
                'metric_focus': fields,
                'source_kind': 'provenance',  # 标记来源
                'chunk_version': 'v1'
            }
            
            candidate_chunks.append(chunk)
        
        return candidate_chunks
    
    # ========== Phase 2: 应用硬性拆分触发器 ==========
    
    def _phase2_apply_triggers(self, chunks: List[Dict]) -> List[Dict]:
        """Phase 2: 对每个 chunk 依次检查 T1-T4
        
        触发器优先级（按顺序）:
        1. T1: 出现第二个独立数值
        2. T2: 数值 + 判断同句
        3. T3: base 与 elasticity 混合
        4. T4: 表格 + 解释
        
        如果触发 → 拆分为多个子 chunk
        """
        triggered_chunks = []
        
        for chunk in chunks:
            content = chunk.get('content', '')
            metric_focus = chunk.get('metric_focus', [])
            
            # 检查触发器
            is_triggered, trigger_name, split_suggestions = self.triggers.check_all(
                content, metric_focus
            )
            
            if is_triggered and len(split_suggestions) > 1:
                logger.info(f"触发器 {trigger_name}: 拆分为 {len(split_suggestions)} 个子chunk")
                
                # 根据建议拆分
                for sub_idx, sub_content in enumerate(split_suggestions):
                    sub_chunk = chunk.copy()
                    sub_chunk['content'] = sub_content
                    sub_chunk['chunk_uid'] = f"{chunk['chunk_uid']}_t{sub_idx}"
                    sub_chunk['_trigger'] = trigger_name  # 记录触发原因
                    
                    triggered_chunks.append(sub_chunk)
            else:
                # 不触发，保留原chunk
                triggered_chunks.append(chunk)
        
        return triggered_chunks
    
    # ========== Phase 3: metric_focus 修正 ==========
    
    def _phase3_correct_metric_focus(self, chunks: List[Dict]) -> List[Dict]:
        """Phase 3: 修正 metric_focus
        
        规则:
        - numeric_estimate: 尽量1个（如果>1且未触发T1/T3，保留第一个）
        - reasoning/risk_analysis: 允许1-4个
        - final_judgement/strategy_pattern: 可为空或1个
        - metric_summary_row: 必须1个
        """
        for chunk in chunks:
            metric_focus = chunk.get('metric_focus', [])
            chunk_type = chunk.get('chunk_type')  # 此时可能还没判定
            
            # 如果 metric_focus 为空，尝试从 content 推断
            if not metric_focus:
                chunk['metric_focus'] = self._infer_metric_focus(chunk['content'])
        
        return chunks
    
    def _infer_metric_focus(self, content: str) -> List[str]:
        """从 content 推断可能的 metric_focus"""
        inferred = []
        
        # 简化：匹配 metric_keys 中的关键词
        for key in self.metric_keys:
            # 提取最后一段作为关键词
            key_term = key.split('.')[-1]
            if key_term.lower() in content.lower():
                inferred.append(key)
        
        return inferred[:2]  # 最多推断2个
    
    # ========== Phase 4: chunk_type 判定 ==========
    
    def _phase4_determine_chunk_type(self, chunks: List[Dict]) -> List[Dict]:
        """Phase 4: 判定 chunk_type（第一次命中即停止）
        
        判定顺序:
        1. metric_summary_row - 来源为表格
        2. final_judgement - 含关键词（最重要/决定性/权重）
        3. numeric_estimate - 含数值/区间/百分比
        4. strategy_pattern - 含关键词（应当/建议/适合）
        5. risk_analysis - 含关键词（风险/成本/回收期）
        6. reasoning - 因果解释
        7. background_context - 兜底
        """
        for chunk in chunks:
            chunk['chunk_type'] = self._determine_chunk_type_single(chunk)
        
        return chunks
    
    def _determine_chunk_type_single(self, chunk: Dict) -> str:
        """判定单个chunk的类型"""
        content = chunk.get('content', '')
        
        # 1. metric_summary_row - 表格标记
        if '|' in content or '\t' in content:
            return 'metric_summary_row'
        
        # 2. final_judgement - 判断关键词
        judgement_kw = ['最重要', '决定性', '权重', '首要', '关键因素', 
                       'most important', 'critical', 'key factor']
        if any(kw in content for kw in judgement_kw):
            return 'final_judgement'
        
        # 3. numeric_estimate - 数值/区间/百分比
        if re.search(r'\d+\.?\d*\s*%', content) or \
           re.search(r'\$\s*\d+', content) or \
           re.search(r'\d+\s*[-–~]\s*\d+', content):
            return 'numeric_estimate'
        
        # 4. strategy_pattern - 策略关键词
        strategy_kw = ['应当', '建议', '适合', '推荐', 'recommend', 'suggest', 'should']
        if any(kw in content for kw in strategy_kw):
            return 'strategy_pattern'
        
        # 5. risk_analysis - 风险关键词
        risk_kw = ['风险', '成本', '回收期', '压缩', 'risk', 'cost', 'payback']
        if any(kw in content for kw in risk_kw):
            return 'risk_analysis'
        
        # 6. reasoning - 因果解释（含"因为/由于/导致"）
        reasoning_kw = ['因为', '由于', '导致', '影响', 'because', 'due to', 'affect']
        if any(kw in content for kw in reasoning_kw):
            return 'reasoning'
        
        # 7. background_context - 兜底
        return 'background_context'
    
    # ========== Phase 5: section/subsection 分类 ==========
    
    def _phase5_classify_section(self, chunks: List[Dict]) -> List[Dict]:
        """Phase 5: 分类 section/subsection
        
        逻辑:
        1. 如果有 metric_focus → 从第一个 metric 提取
        2. 否则 → 关键词分类
        """
        for chunk in chunks:
            metric_focus = chunk.get('metric_focus', [])
            
            if metric_focus:
                # 从 metric_key 提取 section/subsection
                first_metric = metric_focus[0]
                parts = first_metric.split('.')
                
                if len(parts) >= 1:
                    chunk['section'] = parts[0]  # financial
                if len(parts) >= 2:
                    chunk['subsection'] = '.'.join(parts[:2])  # financial.capex
            else:
                # 关键词分类
                chunk['section'] = self._classify_section_by_keywords(chunk['content'])
        
        return chunks
    
    def _classify_section_by_keywords(self, content: str) -> str:
        """基于关键词分类 section"""
        keyword_map = {
            'financial': ['成本', '收入', '利润', '投资', 'cost', 'revenue', 'profit', 'capex'],
            'location': ['地点', '客流', '位置', 'location', 'traffic', 'foot'],
            'machine': ['机器', '设备', '性能', 'machine', 'equipment', 'performance'],
            'risk': ['风险', '问题', 'risk', 'challenge'],
            'market': ['市场', '需求', 'market', 'demand'],
        }
        
        for section, keywords in keyword_map.items():
            if any(kw in content for kw in keywords):
                return section
        
        return 'conclusion'  # 默认
    
    # ========== Phase 6: 验证 ==========
    
    def _phase6_validate(self, chunks: List[Dict]) -> List[Dict]:
        """Phase 6: 验证chunk质量
        
        如果验证失败 → 降级为 background_context
        """
        validated_chunks = []
        
        for chunk in chunks:
            is_valid, failure_reason = self.validators.validate_all(chunk)
            
            if not is_valid:
                logger.warning(f"Chunk {chunk.get('chunk_uid')} 验证失败: {failure_reason}，降级为 background_context")
                chunk['chunk_type'] = 'background_context'
                chunk['_validation_failed'] = failure_reason
            
            validated_chunks.append(chunk)
        
        return validated_chunks
    
    # ========== Phase 7: 补充拆分 raw_answer_text ==========
    
    def _phase7_supplement_raw_text(self,
                                    run_id: int,
                                    raw_answer_text: str,
                                    existing_chunks: List[Dict]) -> List[ResearchChunk]:
        """Phase 7: 补充拆分 raw_answer_text（辅路径）
        
        规则:
        - 轻量结构切分（按段落）
        - 与 provenance chunks 去重
        - 全部标记为 background_context
        """
        if not raw_answer_text or not raw_answer_text.strip():
            return []
        
        # 提取已有chunk的内容（用于去重）
        existing_contents = set(c.get('content', '') for c in existing_chunks)
        
        # 按段落拆分
        paragraphs = re.split(r'\n\n+', raw_answer_text)
        
        background_chunks = []
        bg_idx = 0
        
        for para in paragraphs:
            para = para.strip()
            
            # 过滤：太短、已存在
            if len(para) < 50:
                continue
            
            # 简单去重：相似度检查
            if self._is_similar_to_existing(para, existing_contents):
                continue
            
            # 创建 background chunk
            chunk = ResearchChunk(
                research_run_id=run_id,
                chunk_uid=f"rr_{run_id}_bg_{bg_idx:04d}",
                content=para,
                chunk_type='background_context',
                metric_focus=[],
                section='background',
                chunk_version='v1',
                source_kind='raw_text'
            )
            
            background_chunks.append(chunk)
            bg_idx += 1
        
        return background_chunks
    
    def _is_similar_to_existing(self, new_content: str, existing_contents: set) -> bool:
        """简单相似度检查（避免重复）"""
        # 简化：检查前50字符是否相同
        prefix = new_content[:50]
        
        for existing in existing_contents:
            if existing.startswith(prefix) or prefix in existing:
                return True
        
        return False
    
    # ========== 辅助方法 ==========
    
    def convert_to_research_chunks(self, chunk_dicts: List[Dict]) -> List[ResearchChunk]:
        """将字典转换为 ResearchChunk 对象"""
        research_chunks = []
        
        for chunk_dict in chunk_dicts:
            try:
                # 过滤掉内部字段（_开头）
                filtered_dict = {k: v for k, v in chunk_dict.items() if not k.startswith('_')}
                
                chunk = ResearchChunk(**filtered_dict)
                research_chunks.append(chunk)
            except Exception as e:
                logger.error(f"转换chunk失败: {e}, chunk={chunk_dict}")
                continue
        
        return research_chunks
