"""Hybrid Search Service - 混合检索服务"""

from typing import Dict, List, Any, Optional
import json
from ymda.settings import Settings
from ymda.services.embedding_service import EmbeddingService
from ymda.services.query_understanding import QueryUnderstandingService, QueryUnderstanding
from ymda.data.repository import get_repository
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class SearchResult:
    """检索结果"""
    
    def __init__(
        self,
        query_text: str,
        semantic_query_text: str,
        matched_fields: List[str],
        top_k: int,
        results: List[Dict[str, Any]],
        diagnostics: Optional[Dict[str, Any]] = None
    ):
        self.query_text = query_text
        self.semantic_query_text = semantic_query_text
        self.matched_fields = matched_fields
        self.top_k = top_k
        self.results = results
        self.diagnostics = diagnostics or {}
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "query_text": self.query_text,
            "semantic_query_text": self.semantic_query_text,
            "matched_fields": self.matched_fields,
            "top_k": self.top_k,
            "results": self.results,
            "diagnostics": self.diagnostics
        }


class HybridSearchService:
    """混合检索服务（Vector + BM25）"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.embedding_service = EmbeddingService(settings)
        self.query_understanding = QueryUnderstandingService(settings)
        self.repository = get_repository(settings)
        
        # 混合检索权重（可配置）
        self.vector_weight = 0.7
        self.bm25_weight = 0.3
        
        # BM25 参数
        self.bm25_k1 = 1.5
        self.bm25_b = 0.75
        
        # 语料库统计（延迟加载）
        self._corpus_stats = None
        self._latest_chunk_version: Optional[str] = None
        
        logger.debug("HybridSearchService 初始化成功")
    
    def _get_or_load_corpus_stats(self) -> Dict[str, Any]:
        """获取或加载语料库统计信息（带缓存）"""
        if self._corpus_stats is None:
            logger.info("加载语料库统计信息...")
            self._corpus_stats = self._get_corpus_statistics()
            logger.info(f"语料库统计: {self._corpus_stats['total_docs']} 个文档, "
                       f"平均长度 {self._corpus_stats['avg_doc_length']:.1f} 词")
        return self._corpus_stats
    
    def _chunk_version_key(self, version: str) -> tuple:
        """用于比较 chunk_version 的排序键"""
        import re
        if not version:
            return (0.0, "")
        match = re.search(r'\d+(?:\.\d+)?', str(version))
        numeric = float(match.group()) if match else 0.0
        return (numeric, str(version))
    
    def _get_latest_chunk_version(self) -> Optional[str]:
        """自动检测 chunk_version"""
        if self._latest_chunk_version is not None:
            return self._latest_chunk_version
        
        try:
            result = self.repository.client.table('research_chunk')\
                .select('chunk_version')\
                .not_.is_('chunk_version', 'null')\
                .limit(200)\
                .execute()
            
            versions = {
                row.get('chunk_version')
                for row in (result.data or [])
                if row.get('chunk_version')
            }
            
            if not versions:
                logger.warning("未检测到 chunk_version，默认使用 v1")
                self._latest_chunk_version = 'v1'
            else:
                self._latest_chunk_version = max(versions, key=self._chunk_version_key)
                logger.info(f"检测到最新 chunk_version: {self._latest_chunk_version}")
        except Exception as e:
            logger.warning(f"获取 chunk_version 失败: {e}")
            self._latest_chunk_version = 'v1'
        
        return self._latest_chunk_version
    
    def _analyze_query_intent(self, query_text: str) -> str:
        """分析查询意图 (Stage 1-2)
        
        Args:
            query_text: 用户查询文本
            
        Returns:
            'DECISION' 或 'EXPLAIN'
            
        规则:
        - DECISION: 决策型查询（是否/值不值得/哪个更/决定/影响/应该/建议/推荐/值得/合适/可行）
        - EXPLAIN: 解释型查询（其他所有查询）
        - 保守策略：不确定时判定为 DECISION
        """
        # 决策型触发词（扩展版）
        decision_keywords = [
            # 核心决策词
            '是否', '值不值得', '值得', '哪个更', '决定', '影响',
            # 建议类
            '应该', '建议', '推荐', '可行', '合适',
            # 比较类
            '更好', '优势', '劣势', '对比',
            # 疑问类（决策导向）
            '要不要', '该不该', '能不能',
            # 英文（可选）
            'should', 'recommend', 'better', 'worth', 'feasible'
        ]
        
        query_lower = query_text.lower()
        
        for keyword in decision_keywords:
            if keyword in query_lower:
                logger.debug(f"Query intent: DECISION (matched: '{keyword}')")
                return 'DECISION'
        
        # 默认为 EXPLAIN
        logger.debug("Query intent: EXPLAIN (no decision keywords matched)")
        return 'EXPLAIN'

    
    def search_registry_keys(
        self,
        query_text: str,
        top_k: int = 10
    ) -> List[str]:
        """Layer 1: 在metric_key_registry中召回相关keys
        
        Args:
            query_text: 查询文本
            top_k: 返回的key数量
            
        Returns:
            matched_keys: 匹配的metric keys列表
        """
        logger.debug(f"Layer 1: Registry key grounding, top_k={top_k}")
        
        try:
            # 生成query embedding
            query_embedding = self.embedding_service.generate_embedding(query_text)
            
            # 查询metric_key_registry (获取所有有embedding的keys)
            result = self.repository.client.table('metric_key_registry')\
                .select('key, embedding, canonical_name')\
                .not_.is_('embedding', 'null')\
                .execute()
            
            if not result.data:
                logger.warning("metric_key_registry表为空")
                return []
            
            # Python端计算相似度
            keys_with_scores = []
            for row in result.data:
                similarity = self._cosine_similarity(
                    query_embedding,
                    row['embedding']
                )
                keys_with_scores.append((
                    row['key'],
                    similarity,
                    row.get('canonical_name', '')
                ))
            
            # 排序并返回top_k
            keys_with_scores.sort(key=lambda x: x[1], reverse=True)
            matched_keys = [k for k, s, _ in keys_with_scores[:top_k]]
            
            logger.info(f"Registry key grounding: {len(matched_keys)} keys matched")
            logger.debug(f"Top keys: {matched_keys[:3]}")
            
            return matched_keys
            
        except Exception as e:
            logger.error(f"Registry key grounding失败: {e}")
            return []
    
    def search(
        self,
        query_text: str,
        top_k: int = 30,
        expected_fields: Optional[List[Dict[str, Any]]] = None
    ) -> SearchResult:
        """
        执行混合检索（新版：3层检索架构）
        
        Args:
            query_text: 用户查询文本
            top_k: 返回结果数量
            expected_fields: YMQ 的 expected_fields（用于查询理解）
            
        Returns:
            SearchResult 对象
        """
        try:
            logger.info(f"=== Chunk-based Hybrid Search ===")
            logger.info(f"Query: {query_text}")
            
            # Stage 1-2: Query Intent Analysis
            intent = self._analyze_query_intent(query_text)
            logger.info(f"Query intent: {intent}")
            
            # Layer 0: Query Understanding (可选，保留兼容性)
            understanding = self.query_understanding.parse_query(query_text, expected_fields)
            semantic_query = understanding.semantic_query_text
            
            logger.debug(f"Semantic query: {semantic_query}")
            
            # Layer 1: Registry Key Grounding
            matched_keys = self.search_registry_keys(semantic_query, top_k=10)
            
            if not matched_keys:
                logger.warning("No keys matched in registry - 降级到纯语义检索模式")
                matched_keys = []  # 空列表表示全库检索
            
            chunk_version = self._get_latest_chunk_version()
            
            # Stage 3-6: 主检索（带 chunk_version + chunk_type过滤）
            primary_chunks = self.search_chunks(
                semantic_query, 
                matched_keys=matched_keys,
                intent=intent,  # ✅ 新增：根据意图过滤 chunk_type
                top_k=8,  # 主证据限制 6-8 条
                chunk_version=chunk_version
            )
            
            # Stage 7: 背景补充检索（独立）
            background_chunks = self.search_background_context(
                semantic_query,
                top_k=2,
                chunk_version=chunk_version
            )
            
            # 合并 chunks
            all_chunks = primary_chunks + background_chunks
            metrics = []
            classified_results: List[Dict[str, Any]] = []
            
            if all_chunks:
                metrics = self._chunks_to_metrics(all_chunks, matched_keys if matched_keys else None)
                # Stage 8: 结果分类与排序（保持语义顺序）
                classified_results = self._classify_and_order_results(metrics, all_chunks)
            else:
                logger.warning("No chunks found - 将直接启用 fallback SQL")
            
            fallback_used = False
            final_results = classified_results
            
            if not final_results:
                logger.warning("Hybrid pipeline empty, falling back to metric SQL")
                fallback_used = True
                query_embedding = self.embedding_service.generate_embedding(semantic_query)
                final_results = self._execute_hybrid_sql(
                    query_embedding=query_embedding,
                    query_text=semantic_query,
                    top_k=top_k
                )
            
            logger.info(f"=== Search Complete ===")
            logger.info(f"Intent: {intent}, Matched keys: {len(matched_keys)}")
            logger.info(f"Primary chunks: {len(primary_chunks)}, Background: {len(background_chunks)}")
            logger.info(f"Total metrics: {len(final_results)} (fallback={fallback_used})")
            
            diagnostics = {
                "matched_registry_keys": len(matched_keys),
                "retrieved_chunks": len(all_chunks),
                "matched_metrics": len(final_results),
                "fallback_used": fallback_used
            }
            
            return SearchResult(
                query_text=query_text,
                semantic_query_text=semantic_query,
                matched_fields=matched_keys,
                top_k=top_k,
                results=final_results,
                diagnostics=diagnostics
            )
            
        except Exception as e:
            logger.error(f"混合检索失败: {e}", exc_info=True)
            raise
    
    def _execute_hybrid_sql(
        self,
        query_embedding: List[float],
        query_text: str,
        top_k: int,
        expected_fields_map: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """执行混合检索 SQL
        
        ⚠️ DEPRECATED: 此方法已废弃，不再在新架构中使用
        新架构使用 search_registry_keys() + search_chunks() + _chunks_to_metrics()
        """
        
        try:
            # 方案：使用 Supabase 的 select + 自定义列
            # 注意：Supabase PostgREST 支持在 select 中使用表达式
            
            embedding_str = None
            if query_embedding:
                embedding_str = '[' + ','.join(map(str, query_embedding)) + ']'
            
            # 构建查询
            # 使用 Supabase 的 select 语法，包含计算列
            # ⚠️ DEPRECATED: 此方法使用旧的metric表结构
            result = self.repository.client.table('metric').select(
                '''
                id,
                key,
                value_numeric,
                value_text,
                value_json,
                research_run!inner(ym_id, ymq_id)
                '''
            ).limit(top_k * 3).execute()  # 获取更多结果用于后处理
            
            # Python 端计算 scores
            # 获取语料库统计（用于 BM25）
            corpus_stats = self._get_or_load_corpus_stats()
            
            results_with_scores = []
            for row in result.data:
                # 计算 vector score
                metric_embedding = row.get('embedding')
                if metric_embedding:
                    vector_score = self._cosine_similarity(query_embedding, metric_embedding)
                else:
                    vector_score = 0.0
                
                # 构建增强文本（包含结构化字段 + expected_fields description）
                enhanced_text = self._build_enhanced_text(row, expected_fields_map)
                
                # 计算 BM25 score（使用增强文本）
                bm25_score = self._calculate_true_bm25(
                    query_text,
                    enhanced_text,  # 使用增强文本而不是仅 evidence_text
                    corpus_stats,
                    k1=self.bm25_k1,
                    b=self.bm25_b
                )
                
                # 归一化 BM25 分数（BM25 分数范围不固定，需要归一化到 0-1）
                # 使用 sigmoid 函数归一化
                import math
                normalized_bm25 = 1 / (1 + math.exp(-bm25_score / 10))
                
                # 计算 hybrid score
                hybrid_score = (
                    self.vector_weight * vector_score +
                    self.bm25_weight * normalized_bm25
                )
                
                # 格式化结果
                research_run = row.get('research_run', {})
                results_with_scores.append({
                    'metric_id': row.get('id'),
                    'key': row.get('key'),
                    'value_numeric': row.get('value_numeric'),
                    'value_text': row.get('value_text'),
                    'value_json': row.get('value_json'),
                    # 'evidence_text': row.get('evidence_text'),  # DEPRECATED
                    # 'evidence_sources': row.get('evidence_sources'),  # DEPRECATED
                    'ym_id': research_run.get('ym_id') if isinstance(research_run, dict) else None,
                    'ymq_id': research_run.get('ymq_id') if isinstance(research_run, dict) else None,
                    'vector_score': round(vector_score, 4),
                    'bm25_score': round(normalized_bm25, 4),
                    'hybrid_score': round(hybrid_score, 4)
                })
            
            # 按 hybrid_score 排序并限制结果数量
            results_with_scores.sort(key=lambda x: x['hybrid_score'], reverse=True)
            return results_with_scores[:top_k]
            
        except Exception as e:
            logger.error(f"执行混合检索失败: {e}")
            raise
    
    def _build_enhanced_text(
        self, 
        metric_row: Dict[str, Any],
        expected_fields_map: Optional[Dict[str, Dict[str, Any]]] = None
    ) -> str:
        """
        构建增强的文本用于 BM25 评分
        
        将结构化字段（key, value_*）与 evidence_text 组合，
        并整合 expected_fields 的 description，
        使得 BM25 能够匹配结构化内容和中文查询
        
        Args:
            metric_row: metric 数据行
            expected_fields_map: expected_fields 的 key -> field_def 映射
            
        Returns:
            增强的文本字符串
        """
        parts = []
        
        # 1. Key (结构化语义字段名)
        # 例如: "financial.capex.total" -> "financial capex total"
        key = metric_row.get('key', '')
        if key:
            # 将点号分隔的 key 转换为空格分隔的词
            key_words = key.replace('.', ' ').replace('_', ' ')
            parts.append(key_words)
            
            # 1.1 从 expected_fields 获取 description（关键优化）
            if expected_fields_map and key in expected_fields_map:
                field_def = expected_fields_map[key]
                description = field_def.get('description', '')
                if description:
                    parts.append(description)
                    logger.debug(f"为 key={key} 添加 description: {description}")
        
        # 2. Value (根据类型添加)
        # 数值型: "value 20000"
        if metric_row.get('value_numeric') is not None:
            parts.append(f"value {metric_row['value_numeric']}")
        
        # 文本型: 直接添加
        if metric_row.get('value_text'):
            parts.append(metric_row['value_text'])
        
        # JSON 型: 提取关键信息
        if metric_row.get('value_json'):
            try:
                import json
                value_json = metric_row['value_json']
                if isinstance(value_json, dict):
                    # 提取 JSON 中的文本值
                    for v in value_json.values():
                        if isinstance(v, (str, int, float)):
                            parts.append(str(v))
                elif isinstance(value_json, str):
                    # 如果是 JSON 字符串，尝试解析
                    parsed = json.loads(value_json)
                    if isinstance(parsed, dict):
                        for v in parsed.values():
                            if isinstance(v, (str, int, float)):
                                parts.append(str(v))
            except:
                pass  # 忽略 JSON 解析错误
        
        # 3. Evidence text (原始证据文本)
        # ⚠️ DEPRECATED: metric不再有evidence_text
        # if metric_row.get('evidence_text'):
        #     parts.append(metric_row['evidence_text'])
        
        # 组合所有部分
        enhanced_text = ' '.join(parts)
        
        return enhanced_text
    
    def search_chunks(
        self,
        query_text: str,
        matched_keys: Optional[List[str]] = None,
        intent: str = 'EXPLAIN',
        top_k: int = 30,
        chunk_version: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Stage 3-6: 主向量检索（在过滤后的 chunk 集合中）
        
        Args:
            query_text: 查询文本
            matched_keys: Layer 1的匹配keys (用于 metric_focus 过滤)
            intent: 查询意图 ('DECISION' 或 'EXPLAIN')
            top_k: 返回chunk数量
            
        Returns:
            chunks with hybrid_score and chunk_type
        """
        logger.debug(f"Stage 3-6: Main chunk search (intent={intent}, top_k={top_k})")
        
        try:
            # 生成query embedding
            query_embedding = self.embedding_service.generate_embedding(query_text)
            
            def _base_query():
                qb = self.repository.client.table('research_chunk')\
                    .select('chunk_uid, content, embedding, chunk_type, metric_focus, research_run_id, chunk_version')\
                    .not_.is_('embedding', 'null')
                if chunk_version:
                    qb = qb.eq('chunk_version', chunk_version)
                return qb
            
            # Stage 3: chunk_version 自动过滤
            query_builder = _base_query()
            
            # Stage 4: 硬过滤 - chunk_type (基于 intent)
            if intent == 'DECISION':
                query_builder = query_builder.in_(
                    'chunk_type',
                    ['numeric_estimate', 'final_judgement', 'metric_summary_row']
                )
                logger.debug("Chunk type filter: DECISION (numeric_estimate, final_judgement, metric_summary_row)")
            else:
                query_builder = query_builder.neq('chunk_type', 'background_context')
                logger.debug("Chunk type filter: EXPLAIN (exclude background_context)")
            
            # 执行查询
            result = query_builder.limit(top_k * 3).execute()
            min_required = max(3, top_k // 2) or 1
            
            if not result.data or len(result.data) < min_required:
                logger.info("Chunk type filter too strict, relaxing constraints")
                result = _base_query().limit(top_k * 3).execute()
            
            if not result.data:
                logger.warning(f"No v1 chunks found for intent={intent}")
                return []
            
            # 获取语料库统计(用于BM25)
            corpus_stats = self._get_or_load_corpus_stats()
            
            # Stage 6: 计算 hybrid score 并排序
            chunks_with_scores = []
            for row in result.data:
                # Stage 5: Python 端 metric_focus 过滤（如果提供了 matched_keys）
                if matched_keys and row.get('metric_focus'):
                    # 检查是否有重叠
                    metric_focus = row['metric_focus']
                    if isinstance(metric_focus, list):
                        has_overlap = any(key in metric_focus for key in matched_keys)
                        if not has_overlap:
                            continue  # 跳过无关 chunk
                
                # Vector score
                vector_score = self._cosine_similarity(
                    query_embedding,
                    row['embedding']
                )
                
                # BM25 score
                bm25_score = self._calculate_true_bm25(
                    query_text,
                    row['content'],
                    corpus_stats
                )
                
                # Hybrid
                hybrid_score = self.vector_weight * vector_score + self.bm25_weight * bm25_score
                
                chunks_with_scores.append({
                    'chunk_uid': row['chunk_uid'],
                    'content': row['content'],
                    'chunk_type': row.get('chunk_type'),
                    'metric_focus': row.get('metric_focus'),
                    'research_run_id': row['research_run_id'],
                    'vector_score': vector_score,
                    'bm25_score': bm25_score,
                    'hybrid_score': hybrid_score
                })
            
            # 排序
            chunks_with_scores.sort(key=lambda x: x['hybrid_score'], reverse=True)
            top_chunks = chunks_with_scores[:top_k]
            
            logger.info(f"Main chunk search: {len(top_chunks)} chunks (after metric_focus filter)")
            return top_chunks
            
        except Exception as e:
            logger.error(f"Main chunk search failed: {e}", exc_info=True)
            return []
    
    def _chunks_to_metrics(
        self,
        chunks: List[Dict[str, Any]],
        matched_keys: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Layer 3: 通过provenance关联chunk到metric
        
        Args:
            chunks: Layer 2的chunks结果
            matched_keys: Layer 1的匹配keys (用于过滤)
            
        Returns:
            metrics with evidence
        """
        if not chunks:
            return []
        
        logger.debug(f"Layer 3: Chunks to metrics correlation")
        
        try:
            chunk_uids = [c['chunk_uid'] for c in chunks]
            
            # 查询metric_provenance
            provenance_result = self.repository.client.table('metric_provenance')\
                .select('chunk_uid, metric_id, quote, confidence')\
                .in_('chunk_uid', chunk_uids)\
                .execute()
            
            if not provenance_result.data:
                logger.warning("No provenance found for chunks")
                return []
            
            # 查询metrics
            metric_ids = list(set(p['metric_id'] for p in provenance_result.data))
            
            metrics_result = self.repository.client.table('metric')\
                .select('*')\
                .in_('id', metric_ids)\
                .execute()
            
            if not metrics_result.data:
                return []
            
            # 组合结果
            results = []
            chunk_map = {c['chunk_uid']: c for c in chunks}
            
            for metric in metrics_result.data:
                # 过滤matched_keys
                if matched_keys and metric['key'] not in matched_keys:
                    continue
                
                # 找到对应的provenance
                prov = next(
                    (p for p in provenance_result.data if p['metric_id'] == metric['id']),
                    None
                )
                
                if not prov:
                    continue
                
                # 找到对应的chunk
                chunk = chunk_map.get(prov['chunk_uid'])
                
                results.append({
                    'metric_id': metric['id'],
                    'key': metric['key'],
                    'value_numeric': metric.get('value_numeric'),
                    'value_text': metric.get('value_text'),
                    'value_json': metric.get('value_json'),
                    'unit': metric.get('unit'),
                    'confidence': prov.get('confidence'),
                    'evidence_chunk': chunk['content'] if chunk else None,
                    'chunk_uid': prov['chunk_uid'],
                    'quote': prov.get('quote'),
                    'hybrid_score': chunk['hybrid_score'] if chunk else 0,
                    'research_run_id': metric.get('research_run_id')
                })
            
            # 按hybrid_score排序
            results.sort(key=lambda x: x['hybrid_score'], reverse=True)
            
            logger.info(f"Chunks to metrics: {len(results)} metrics found")
            return results
            
        except Exception as e:
            logger.error(f"Chunks to metrics失败: {e}")
            return []
    
    def search_background_context(
        self,
        query_text: str,
        top_k: int = 2,
        chunk_version: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Stage 7: 独立的 background_context 检索
        
        Args:
            query_text: 查询文本
            top_k: 返回chunk数量（文档建议 1-2 条）
            
        Returns:
            background chunks with hybrid_score
        """
        logger.debug(f"Stage 7: Background context retrieval (top_k={top_k})")
        
        try:
            # 生成query embedding
            query_embedding = self.embedding_service.generate_embedding(query_text)
            
            def _base_query():
                qb = self.repository.client.table('research_chunk')\
                    .select('chunk_uid, content, embedding, chunk_type, research_run_id, chunk_version')\
                    .eq('chunk_type', 'background_context')\
                    .not_.is_('embedding', 'null')
                if chunk_version:
                    qb = qb.eq('chunk_version', chunk_version)
                return qb
            
            # 查询 background_context chunks
            result = _base_query().limit(top_k * 2).execute()
            if (not result.data) and chunk_version:
                logger.info("No background_context for current version, relaxing version filter")
                result = self.repository.client.table('research_chunk')\
                    .select('chunk_uid, content, embedding, chunk_type, research_run_id, chunk_version')\
                    .eq('chunk_type', 'background_context')\
                    .not_.is_('embedding', 'null')\
                    .limit(top_k * 2)\
                    .execute()
            
            if not result.data:
                logger.debug("No background_context chunks found")
                return []
            
            # 获取语料库统计
            corpus_stats = self._get_or_load_corpus_stats()
            
            # 计算 hybrid score
            chunks_with_scores = []
            for row in result.data:
                vector_score = self._cosine_similarity(
                    query_embedding,
                    row['embedding']
                )
                
                bm25_score = self._calculate_true_bm25(
                    query_text,
                    row['content'],
                    corpus_stats
                )
                
                hybrid_score = self.vector_weight * vector_score + self.bm25_weight * bm25_score
                
                chunks_with_scores.append({
                    'chunk_uid': row['chunk_uid'],
                    'content': row['content'],
                    'chunk_type': 'background_context',
                    'research_run_id': row['research_run_id'],
                    'vector_score': vector_score,
                    'bm25_score': bm25_score,
                    'hybrid_score': hybrid_score
                })
            
            # 排序并返回 top_k
            chunks_with_scores.sort(key=lambda x: x['hybrid_score'], reverse=True)
            top_chunks = chunks_with_scores[:top_k]
            
            logger.info(f"Background context: {len(top_chunks)} chunks")
            return top_chunks
            
        except Exception as e:
            logger.error(f"Background context retrieval failed: {e}", exc_info=True)
            return []
    
    def _classify_and_order_results(
        self,
        metrics: List[Dict[str, Any]],
        chunks: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Stage 8: 结果分类与排序（保持语义顺序）
        
        Args:
            metrics: 从 _chunks_to_metrics 返回的 metrics
            chunks: 所有 chunks（含 chunk_type）
            
        Returns:
            按语义顺序排列的结果:
            1. primary_evidence (numeric_estimate, metric_summary_row)
            2. judgements (final_judgement)
            3. background (background_context)
        """
        logger.debug("Stage 8: Classifying and ordering results")
        
        # 创建 chunk_uid -> chunk 映射
        chunk_map = {c['chunk_uid']: c for c in chunks}
        
        # 分类
        primary_evidence = []
        judgements = []
        background = []
        other = []
        
        for metric in metrics:
            chunk_uid = metric.get('chunk_uid')
            chunk = chunk_map.get(chunk_uid)
            
            if not chunk:
                other.append(metric)
                continue
            
            chunk_type = chunk.get('chunk_type')
            
            if chunk_type in ['numeric_estimate', 'metric_summary_row']:
                primary_evidence.append(metric)
            elif chunk_type == 'final_judgement':
                judgements.append(metric)
            elif chunk_type == 'background_context':
                background.append(metric)
            else:
                # reasoning, strategy_pattern, risk_analysis 等
                other.append(metric)
        
        # 按语义顺序合并（保持各组内的 hybrid_score 排序）
        ordered_results = primary_evidence + judgements + other + background
        
        logger.info(f"Result classification: primary={len(primary_evidence)}, "
                   f"judgements={len(judgements)}, background={len(background)}, other={len(other)}")
        
        return ordered_results

    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """计算余弦相似度"""
        import math
        
        if not vec1 or not vec2 or len(vec1) != len(vec2):
            return 0.0
        
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        magnitude1 = math.sqrt(sum(a * a for a in vec1))
        magnitude2 = math.sqrt(sum(b * b for b in vec2))
        
        if magnitude1 == 0 or magnitude2 == 0:
            return 0.0
        
        return dot_product / (magnitude1 * magnitude2)
    
    def _calculate_bm25_score(
        self,
        query: str,
        text: str,
        avg_doc_length: float = 200.0,
        k1: float = 1.5,
        b: float = 0.75
    ) -> float:
        """
        计算 BM25 分数
        
        BM25 公式:
        score = Σ IDF(qi) * (f(qi, D) * (k1 + 1)) / (f(qi, D) + k1 * (1 - b + b * |D| / avgdl))
        
        Args:
            query: 查询文本
            text: 文档文本
            avg_doc_length: 平均文档长度
            k1: term frequency saturation parameter (通常 1.2-2.0)
            b: length normalization parameter (通常 0.75)
            
        Returns:
            BM25 分数
        """
        if not query or not text:
            return 0.0
        
        # 分词
        query_terms = query.lower().split()
        doc_terms = text.lower().split()
        
        if not query_terms or not doc_terms:
            return 0.0
        
        # 计算文档长度
        doc_length = len(doc_terms)
        
        # 构建词频字典
        term_freq = {}
        for term in doc_terms:
            term_freq[term] = term_freq.get(term, 0) + 1
        
        # 计算 BM25 分数
        score = 0.0
        for query_term in query_terms:
            if query_term not in term_freq:
                continue
            
            # Term frequency in document
            tf = term_freq[query_term]
            
            # IDF (简化版本，假设所有词的 IDF 相同)
            # 在真实场景中，需要从整个语料库计算 IDF
            # IDF = log((N - df + 0.5) / (df + 0.5))
            # 这里使用固定值作为近似
            idf = 2.0  # 简化的 IDF 值
            
            # BM25 公式
            numerator = tf * (k1 + 1)
            denominator = tf + k1 * (1 - b + b * (doc_length / avg_doc_length))
            
            score += idf * (numerator / denominator)
        
        return score
    
    def _get_corpus_statistics(self) -> Dict[str, Any]:
        """
        获取语料库统计信息（用于 BM25 IDF 计算）
        
        Returns:
            {
                "total_docs": int,
                "avg_doc_length": float,
                "term_doc_freq": Dict[str, int]  # 每个词出现在多少个文档中
            }
        """
        try:
            # ⚠️ DEPRECATED: 新架构metric表无evidence_text字段
            # 使用research_chunk表代替
            result = self.repository.client.table('research_chunk').select('content').not_.is_('content', 'null').limit(1000).execute()
            
            if not result.data:
                return {
                    "total_docs": 0,
                    "avg_doc_length": 200.0,
                    "term_doc_freq": {}
                }
            
            total_docs = len(result.data)
            total_length = 0
            term_doc_freq = {}
            
            for row in result.data:                
                text = row.get('content', '')  # 使用chunk content代替evidence_text
                if not text:
                    continue
                
                terms = text.lower().split()
                total_length += len(terms)
                
                # 记录每个词出现在哪些文档中（用 set 去重）
                unique_terms = set(terms)
                for term in unique_terms:
                    term_doc_freq[term] = term_doc_freq.get(term, 0) + 1
            
            avg_doc_length = total_length / total_docs if total_docs > 0 else 200.0
            
            return {
                "total_docs": total_docs,
                "avg_doc_length": avg_doc_length,
                "term_doc_freq": term_doc_freq
            }
            
        except Exception as e:
            logger.error(f"获取语料库统计信息失败: {e}")
            return {
                "total_docs": 0,
                "avg_doc_length": 200.0,
                "term_doc_freq": {}
            }
    
    def _calculate_true_bm25(
        self,
        query: str,
        text: str,
        corpus_stats: Dict[str, Any],
        k1: float = 1.5,
        b: float = 0.75
    ) -> float:
        """
        使用真实语料库统计计算 BM25 分数
        
        Args:
            query: 查询文本
            text: 文档文本
            corpus_stats: 语料库统计信息
            k1: term frequency saturation parameter
            b: length normalization parameter
            
        Returns:
            BM25 分数
        """
        if not query or not text:
            return 0.0
        
        # 分词
        query_terms = query.lower().split()
        doc_terms = text.lower().split()
        
        if not query_terms or not doc_terms:
            return 0.0
        
        # 获取统计信息
        total_docs = corpus_stats.get('total_docs', 1)
        avg_doc_length = corpus_stats.get('avg_doc_length', 200.0)
        term_doc_freq = corpus_stats.get('term_doc_freq', {})
        
        # 计算文档长度
        doc_length = len(doc_terms)
        
        # 构建词频字典
        term_freq = {}
        for term in doc_terms:
            term_freq[term] = term_freq.get(term, 0) + 1
        
        # 计算 BM25 分数
        score = 0.0
        for query_term in query_terms:
            if query_term not in term_freq:
                continue
            
            # Term frequency in document
            tf = term_freq[query_term]
            
            # Document frequency (多少个文档包含这个词)
            df = term_doc_freq.get(query_term, 0)
            
            # IDF 计算
            # IDF = log((N - df + 0.5) / (df + 0.5) + 1)
            import math
            if df > 0:
                idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1)
            else:
                idf = math.log(total_docs + 1)  # 词不在语料库中，给一个默认 IDF
            
            # BM25 公式
            numerator = tf * (k1 + 1)
            denominator = tf + k1 * (1 - b + b * (doc_length / avg_doc_length))
            
            score += idf * (numerator / denominator)
        
        return score
    
    def _post_filter_by_fields(
        self,
        results: List[Dict[str, Any]],
        matched_field_keys: List[str]
    ) -> List[Dict[str, Any]]:
        """根据匹配的字段后过滤
        
        ⚠️ DEPRECATED: 此方法已废弃，不再在新架构中使用
        新架构在 _chunks_to_metrics() 中直接过滤
        """
        
        if not matched_field_keys:
            return results
        
        filtered = []
        for result in results:
            metric_key = result.get('key', '')
            
            # 检查 metric.key 是否与匹配的字段相关
            if self._matches_any_field(metric_key, matched_field_keys):
                filtered.append(result)
        
        logger.debug(f"Post-filter: {len(results)} -> {len(filtered)} 条结果")
        return filtered
    
    def _matches_any_field(self, metric_key: str, matched_keys: List[str]) -> bool:
        """检查 metric key 是否匹配任何目标字段"""
        
        for field_key in matched_keys:
            # 前缀匹配或包含匹配
            if metric_key.startswith(field_key) or field_key in metric_key:
                return True
        
        return False
