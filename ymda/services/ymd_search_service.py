"""YMD Search Service - 统一查询服务

提供统一的查询入口,支持多种查询模式:
- semantic_only: 纯向量检索
- structured_only: 纯结构化查询 (Phase 2)
- hybrid: 混合检索 (Phase 2)
- auto: 自动选择模式
"""

import uuid
import time
from typing import Dict, List, Any, Optional
from ymda.settings import Settings
from ymda.services.hybrid_search import HybridSearchService, SearchResult
from ymda.mcp.schemas import SearchRequest, SearchResponse, SearchStats
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class YMDSearchService:
    """YMD 统一查询服务"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
        self.hybrid_search_service = HybridSearchService(settings)
        logger.debug("YMDSearchService 初始化成功")
    
    def search(self, request: SearchRequest) -> SearchResponse:
        """
        执行查询
        
        Args:
            request: SearchRequest 对象
            
        Returns:
            SearchResponse 对象
        """
        # Step 0: Generate trace_id & timer
        trace_id = str(uuid.uuid4())
        start_time = time.time()
        
        logger.info(f"[{trace_id}] YMD Search started: mode={request.mode}, query={request.query[:50]}...")
        
        try:
            # Step 1: Mode decision
            actual_mode = self._decide_mode(request)
            logger.info(f"[{trace_id}] Mode决策: {request.mode} -> {actual_mode}")
            
            # Step 2: Execute query
            if actual_mode == "semantic_only":
                search_result = self._semantic_only_search(request, trace_id)
            elif actual_mode == "structured_only":
                raise NotImplementedError("structured_only mode not yet implemented (Phase 2)")
            elif actual_mode == "hybrid":
                raise NotImplementedError("hybrid mode not yet implemented (Phase 2)")
            else:
                raise ValueError(f"Unknown mode: {actual_mode}")
            
            # Step 3: Build stats
            latency_ms = (time.time() - start_time) * 1000
            stats = self._build_stats(actual_mode, search_result, latency_ms)
            
            # Step 4: Build response
            response = SearchResponse(
                trace_id=trace_id,
                query_text=request.query,
                mode=actual_mode,
                results=search_result.results,
                stats=stats,
                explain=self._build_explain(request, actual_mode, search_result.results) if request.explain else None
            )
            
            logger.info(f"[{trace_id}] YMD Search completed: {len(search_result.results)} results in {latency_ms:.0f}ms")
            return response
            
        except Exception as e:
            logger.error(f"[{trace_id}] YMD Search failed: {e}", exc_info=True)
            raise
    
    def _decide_mode(self, request: SearchRequest) -> str:
        """
        模式决策
        
        MVP: 只支持 semantic_only
        Phase 2: 根据 filters 决策 auto 模式
        """
        if request.mode == "auto":
            # MVP: auto 退化为 semantic_only
            # Phase 2: 根据 filters 决策
            if request.filters and request.filters.get("metrics"):
                logger.warning("auto mode with filters -> fallback to semantic_only (structured not yet supported)")
            return "semantic_only"
        
        return request.mode
    
    def _semantic_only_search(self, request: SearchRequest, trace_id: str) -> SearchResult:
        """
        纯语义检索 (复用 HybridSearchService)
        
        Args:
            request: SearchRequest
            trace_id: 用于日志
            
        Returns:
            结果列表
        """
        logger.debug(f"[{trace_id}] Executing semantic_only search")
        
        # 调用现有 HybridSearchService
        search_result: SearchResult = self.hybrid_search_service.search(
            query_text=request.query,
            top_k=request.top_k,
            expected_fields=None  # MVP 暂不支持
        )
        
        return search_result
    
    def _build_stats(self, mode: str, search_result: SearchResult, latency_ms: float) -> SearchStats:
        """构建统计信息"""
        diagnostics = getattr(search_result, "diagnostics", {}) or {}
        return SearchStats(
            mode=mode,
            returned=len(search_result.results),
            latency_ms=latency_ms,
            matched_registry_keys=diagnostics.get("matched_registry_keys"),
            retrieved_chunks=diagnostics.get("retrieved_chunks"),
            matched_metrics=diagnostics.get("matched_metrics"),
            fallback_used=diagnostics.get("fallback_used", False)
        )
    
    def _build_explain(
        self,
        request: SearchRequest,
        actual_mode: str,
        results: List[Dict]
    ) -> Dict[str, Any]:
        """
        构建 explain 信息
        
        MVP: 基础信息
        Phase 2: 添加 SQL, registry_version, filters 等
        """
        return {
            "requested_mode": request.mode,
            "actual_mode": actual_mode,
            "mode_decision_reason": "MVP: auto->semantic_only (structured not yet supported)",
            "top_k": request.top_k,
            "returned": len(results),
            "notes": "Phase 1 MVP: only semantic_only mode supported"
        }
