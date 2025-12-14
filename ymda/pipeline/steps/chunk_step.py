"""Chunk 切片步骤

将 research_run.raw_output 切分为语义chunk,并生成embedding

v1 变更:
- 主路径: 基于 provenance.evidence_text 拆分（80%）
- 辅路径: 补充拆分 raw_answer_text（20%）
- 新增: chunk_type, metric_focus, subsection 字段
"""

from typing import Any, Dict, List
from langchain_text_splitters import RecursiveCharacterTextSplitter
from ymda.pipeline.steps.validate_step import BaseStep
from ymda.data.models import ResearchChunk
from ymda.data.repository import get_repository
from ymda.services.embedding_service import EmbeddingService
from ymda.services.chunk_splitter import ChunkSplitter
from ymda.settings import Settings
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class ChunkStep(BaseStep):
    """Chunk 切片步骤 v1
    
    职责:
    1. 读取 research_run.raw_output + provenance
    2. 使用 ChunkSplitter v1 进行语义拆分
    3. 生成 chunk_uid (格式: rr_{run_id}_prov_{index} 或 rr_{run_id}_bg_{index})
    4. 为每个 chunk 生成 embedding
    5. 保存到 research_chunk 表
    """
    
    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.repository = get_repository(settings)
        self.embedding_service = EmbeddingService(settings)
        
        # ✨ 新增: ChunkSplitter v1 (延迟初始化，需要 metric_key_registry)
        self.chunk_splitter = None
        
        # ⚠️ 保留旧逻辑作为兜底（仅在无provenance时使用）
        self.fallback_splitter = RecursiveCharacterTextSplitter(
            chunk_size=1200,
            chunk_overlap=100,
            separators=["\n\n", "\n", "。", ". ", " "]
        )
        
        logger.debug("ChunkStep v1 初始化成功")
    
    def split_text(self, text: str) -> List[str]:
        """切分文本（兜底方法，仅在无provenance时使用）
        
        Args:
            text: 完整文本
            
        Returns:
            切片列表
        """
        if not text:
            return []
        
        try:
            chunks = self.fallback_splitter.split_text(text)
            logger.info(f"文本切分完成（兜底逻辑）: {len(chunks)} 个chunk")
            return chunks
        except Exception as e:
            logger.error(f"文本切分失败: {e}")
            return []
    
    def create_chunks_for_run(
        self, 
        research_run_id: int, 
        raw_text: str
    ) -> List[ResearchChunk]:
        """为一个 research_run 创建 chunks（兜底方法）
        
        Args:
            research_run_id: run ID
            raw_text: 原始文本 (从 raw_output 提取)
            
        Returns:
            ResearchChunk 列表
        """
        # 切分文本
        text_chunks = self.split_text(raw_text)
        
        if not text_chunks:
            logger.warning(f"Run {research_run_id} 没有生成任何chunk")
            return []
        
        # 创建 ResearchChunk 对象
        chunks = []
        for idx, content in enumerate(text_chunks):
            # 生成稳定的 chunk_uid
            chunk_uid = f"rr_{research_run_id}_chunk_{idx:04d}"
            
            # 生成 embedding
            embedding = None
            try:
                embedding = self.embedding_service.generate_embedding(content)
                logger.debug(f"Chunk {chunk_uid} embedding 生成成功")
            except Exception as e:
                logger.error(f"Chunk {chunk_uid} embedding 生成失败: {e}")
            
            chunk = ResearchChunk(
                research_run_id=research_run_id,
                chunk_uid=chunk_uid,
                content=content,
                embedding=embedding,
                chunk_version='v0',  # 标记为旧逻辑
                chunk_type='background_context',
                section='background'
            )
            
            chunks.append(chunk)
        
        logger.info(f"为 run {research_run_id} 创建了 {len(chunks)} 个chunk（兜底逻辑）")
        return chunks
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """执行切片 (v1)
        
        输入:
            context['research_results']: 研究结果列表 (含 run_id + answer)
            context['metric_key_registry']: metric keys 列表（可选）
            
        输出:
            context['chunks_created']: 总chunk数
        """
        logger.info("开始 Chunk 切片步骤 (v1)")
        
        research_results = context.get("research_results", [])
        metric_key_registry = context.get("metric_key_registry", [])
        
        if not research_results:
            logger.warning("没有研究结果需要切片")
            context['chunks_created'] = 0
            return context
        
        # 检查是否有 metric_key_registry
        if not metric_key_registry:
            logger.warning("metric_key_registry 为空，将使用兜底逻辑")
            use_v1_logic = False
        else:
            use_v1_logic = True
            # 初始化 ChunkSplitter v1
            self.chunk_splitter = ChunkSplitter(metric_key_registry)
        
        total_chunks = 0
        
        for result in research_results:
            try:
                run_id = result.get('run_id')
                answer = result.get('answer', {})
                raw_text = answer.get('raw_answer_text', '')
                
                if not run_id:
                    logger.warning(f"结果缺少 run_id，跳过")
                    continue
                
                # ✨ v1 逻辑: 提取 provenance
                structured_answer = answer.get('structured_answer', {})
                provenance = structured_answer.get('provenance', [])
                
                if use_v1_logic:
                    # ✨ 主路径: 使用 ChunkSplitter v1
                    # 即使没有 provenance，v1 也会通过 Phase 7 处理 raw_answer_text
                    logger.info(f"Run {run_id}: 使用 v1 逻辑拆分, provenance={len(provenance)} 条")
                    
                    chunks = self.chunk_splitter.split(
                        research_run_id=run_id,
                        raw_answer_text=raw_text,
                        provenance=provenance,  # 可以为空列表，Phase 7 会处理 raw_text
                        metric_key_registry=metric_key_registry
                    )
                    
                    # 转换为 ResearchChunk 对象（如果需要）
                    if chunks and isinstance(chunks[0], dict):
                        chunks = self.chunk_splitter.convert_to_research_chunks(chunks)
                    
                else:
                    # ⚠️ 兜底逻辑: 仅在没有 metric_key_registry 时使用
                    logger.warning(f"Run {run_id}: 无 metric_key_registry，使用兜底逻辑")
                    chunks = self.create_chunks_for_run(run_id, raw_text)
                
                if not chunks:
                    logger.warning(f"Run {run_id}: 未生成任何chunk")
                    continue
                
                # 生成 embedding
                for chunk in chunks:
                    if chunk.embedding is None:  # 如果还没有embedding
                        try:
                            chunk.embedding = self.embedding_service.generate_embedding(chunk.content)
                            logger.debug(f"Chunk {chunk.chunk_uid} embedding 生成成功")
                        except Exception as e:
                            logger.error(f"Chunk {chunk.chunk_uid} embedding 生成失败: {e}")
                
                # 保存到数据库
                if chunks and self.repository:
                    success = self.repository.save_research_chunks(chunks)
                    if success:
                        total_chunks += len(chunks)
                        
                        # 统计chunk_type分布
                        type_dist = {}
                        for c in chunks:
                            ct = c.chunk_type or 'unknown'
                            type_dist[ct] = type_dist.get(ct, 0) + 1
                        
                        logger.info(f"✅ Run {run_id}: 保存了 {len(chunks)} 个chunk (v1)")
                        logger.info(f"   Chunk类型分布: {type_dist}")
                    else:
                        logger.warning(f"⚠️ Run {run_id}: chunk保存失败")
                else:
                    logger.warning("Repository 未初始化,无法保存chunks")
                
            except Exception as e:
                logger.error(f"处理结果失败: {e}")
                import traceback
                traceback.print_exc()
                continue
        
        context['chunks_created'] = total_chunks
        logger.info(f"Chunk 切片步骤完成 (v1): 共创建 {total_chunks} 个chunk")
        
        return context
