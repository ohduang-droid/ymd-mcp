"""
Store Step - 完全重构版

P0-3, P0-6 修正：集成所有Writer，实现完整的错误处理和finalize机制
"""

from typing import Any, Dict, List, Optional
import logging

from ymda.pipeline.steps.base_step import BaseStep
from ymda.pipeline.writers.metric_writer import MetricWriter
from ymda.pipeline.writers.provenance_writer import ProvenanceWriter
from ymda.pipeline.writers.artifact_writer import ArtifactWriter

logger = logging.getLogger(__name__)


class StoreStep(BaseStep):
    """
    Store Step - 数据存储与Finalize
    
    职责:
    1. 写入 metrics (MetricWriter)
    2. 写入 provenance (ProvenanceWriter)
    3. 写入 artifact (ArtifactWriter)
    4. Finalize (根据 required_missing_count)
    
    P0-6 修正：明确判断 extraction_failed，避免幽灵 run
    """
    
    def __init__(self, settings):
        super().__init__(settings)
        self.metric_writer = MetricWriter()
        self.prov_writer = ProvenanceWriter(self.repository)
        self.artifact_writer = ArtifactWriter(self.repository)
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行存储步骤
        
        输入: context with extraction results
        输出: context with stored=true/false
        
        失败处理:
        - 任一步失败 → rollback_failed_run
        - P0-6: 明确跳过 extraction_failed 的 run
        """
        research_results = context.get('research_results', [])
        stored_count = 0
        
        for result in research_results:
            run_id = result.get('run_id')
            
            if not run_id:
                logger.warning("Result missing run_id, skipping")
                continue
            
            # P0-6: 明确判断失败标记
            if result.get('extraction_failed', False):
                logger.info(f"Skipping run {run_id}: extraction failed")
                continue
            
            extraction = result.get('extraction')
            if not extraction:
                logger.warning(f"Skipping run {run_id}: no extraction data")
                continue
            
            # 处理单个run
            success = self._process_single_run(result)
            if success:
                stored_count += 1
        
        # 更新context
        context['stored'] = stored_count > 0
        context['stored_count'] = stored_count
        
        logger.info(f"StoreStep completed: {stored_count}/{len(research_results)} runs stored")
        return context
    
    def _process_single_run(self, result: Dict[str, Any]) -> bool:
        """
        处理单个run的存储
        
        Args:
            result: 包含 run_id, extraction, ym_db_id, ymq_db_id
            
        Returns:
            是否成功
        """
        run_id = result['run_id']
        extraction = result['extraction']
        ym_db_id = result.get('ym_db_id')
        ymq_db_id = result.get('ymq_db_id')
        
        try:
            logger.info(f"Run {run_id}: Starting storage")
            
            # 1. 写入 metrics (P0-3: 跟踪 required_missing)
            metrics, required_missing_keys = self._write_metrics(
                extraction, run_id, ymq_db_id
            )
            
            if not metrics:
                # ⭐ 区分两种情况：
                # 1. 抽取失败（应该有数据但没抽到）
                # 2. 报告中确实没有相关数据
                
                structured = extraction.get('structured', {})
                provenance = extraction.get('provenance', [])
                
                logger.warning(f"Run {run_id}: No metrics extracted")
                logger.warning(f"  - structured 字段数: {len(structured)}")
                logger.warning(f"  - provenance 条目数: {len(provenance)}")
                logger.warning(f"  - required_missing_keys: {required_missing_keys}")
                
                # 如果 structured 为空，说明 LLM 没有找到任何数据
                if not structured:
                    logger.warning(f"  → 可能原因：报告中没有包含期望字段的具体数值数据")
                    logger.warning(f"  → 建议：检查报告内容是否包含相关数据，或调整 expected_fields")
                
                self.repository.rollback_failed_run(run_id, 'no_metrics_extracted')
                return False
            
            logger.info(f"Run {run_id}: Wrote {len(metrics)} metrics")
            
            # 2. 写入 provenance
            provenances = self._write_provenances(
                extraction, metrics, run_id
            )
            
            logger.info(f"Run {run_id}: Wrote {len(provenances)} provenance entries")
            
            # 3. 写入 artifact
            artifact = self._write_artifact(
                extraction, metrics, required_missing_keys,
                run_id, ym_db_id, ymq_db_id
            )
            
            logger.info(f"Run {run_id}: Wrote artifact")
            
            # 4. Finalize (决策 parsed_ok)
            required_missing_count = len(required_missing_keys)
            
            if required_missing_count == 0:
                # P0-5: 完全成功
                self._finalize_success(run_id, ym_db_id, ymq_db_id)
                logger.info(f"Run {run_id}: Finalized as PARSED (all required fields present)")
            else:
                # P0-4: 部分成功
                self._finalize_partial(run_id, required_missing_keys)
                logger.info(f"Run {run_id}: Finalized as PARTIAL ({required_missing_count} required fields missing)")
            
            return True
            
        except Exception as e:
            logger.error(f"Run {run_id}: Storage failed - {e}", exc_info=True)
            self._rollback_failed_run(run_id, str(e))
            return False
    
    def _write_metrics(self, extraction: Dict, run_id: int, ymq_db_id: int) -> tuple:
        """
        写入 metrics
        
        P0-3: 跟踪 required_missing_keys
        
        Returns:
            (saved_metrics, required_missing_keys)
        """
        field_specs = extraction['field_specs']
        registry_entries = extraction['registry_entries']
        structured = extraction['structured']
        
        metrics_to_save = []
        required_missing_keys = []
        
        # 遍历所有 field_specs
        for field_spec in field_specs:
            key = field_spec.key
            value = structured.get(key)
            
            # 字段未抽取
            if value is None:
                if field_spec.required:
                    required_missing_keys.append(key)
                logger.debug(f"Field {key} not extracted (required={field_spec.required})")
                continue
            
            # 查找对应的 registry entry
            registry_entry = self._find_registry_entry(key, registry_entries)
            if not registry_entry:
                logger.warning(f"Registry entry not found for {key}, skipping")
                if field_spec.required:
                    required_missing_keys.append(key)
                continue
            
            # 调用 MetricWriter
            metric_data = self.metric_writer.write_metric(
                key=key,
                value=value,
                registry_entry=registry_entry,
                run_id=run_id,
                is_required=field_spec.required
            )
            
            # P0-3: 解析失败且 required=true
            if metric_data is None:
                required_missing_keys.append(key)
                logger.warning(f"Metric {key} parse failed (required={field_spec.required})")
            else:
                metrics_to_save.append(metric_data)
        
        # 批量保存到数据库
        if metrics_to_save:
            saved_metrics = self._save_metrics_to_db(metrics_to_save)
            return saved_metrics, required_missing_keys
        else:
            return [], required_missing_keys
    
    def _write_provenances(self, extraction: Dict, metrics: List[Dict], 
                          run_id: int) -> List[Dict]:
        """
        写入 provenance
        
        P0-3: 确保每个 metric 至少有 1 条 provenance
        """
        provenance_data_list = extraction.get('provenance', [])
        
        # 先验证覆盖率
        self.prov_writer.validate_coverage(metrics, provenance_data_list)
        
        # 写入每条 provenance
        provenances_to_save = []
        
        for prov_data in provenance_data_list:
            fields = prov_data.get('fields', [])
            
            # 为每个 field 创建一条 provenance
            for field_key in fields:
                # 查找对应的 metric
                metric = self._find_metric_by_key(metrics, field_key)
                if not metric:
                    logger.warning(f"Metric not found for provenance field: {field_key}")
                    continue
                
                # 调用 ProvenanceWriter
                try:
                    prov = self.prov_writer.write_provenance(
                        metric_id=metric['id'],
                        prov_data=prov_data,
                        run_id=run_id
                    )
                    provenances_to_save.append(prov)
                except Exception as e:
                    logger.error(f"Failed to write provenance for {field_key}: {e}")
                    # 不中断，继续处理其他
        
        # 批量保存
        if provenances_to_save:
            self._save_provenances_to_db(provenances_to_save)
        
        return provenances_to_save
    
    def _write_artifact(self, extraction: Dict, metrics: List[Dict],
                       required_missing_keys: List[str],
                       run_id: int, ym_db_id: int, ymq_db_id: int) -> Dict:
        """
        写入 artifact
        
        P0-5: 使用 ArtifactWriter
        """
        field_specs = extraction['field_specs']
        
        # 调用 ArtifactWriter
        artifact_data = self.artifact_writer.write_artifact(
            run_id=run_id,
            ym_id=ym_db_id,
            ymq_id=ymq_db_id,
            metrics=metrics,
            field_specs=field_specs,
            extractor_model='gpt-4o-mini'  # TODO: 从 settings 获取
        )
        
        # 保存到数据库
        result = self.repository.client.table('research_artifact')\
            .insert(artifact_data)\
            .execute()
        
        return result.data[0] if result.data else {}
    
    def _finalize_success(self, run_id: int, ym_id: int, ymq_id: int):
        """
        P0-5: Finalize 成功的 run
        
        status='parsed', parsed_ok=true, is_latest=true
        清除同 (ym_id, ymq_id) 的旧 latest
        """
        self.repository.finalize_research_run(run_id, ym_id, ymq_id)
    
    def _finalize_partial(self, run_id: int, missing_keys: List[str]):
        """
        P0-4: Finalize 部分成功的 run
        
        status='partial', parsed_ok=false, is_latest=false
        """
        error_msg = f"required_fields_missing: {','.join(missing_keys)}"
        self.repository.finalize_research_run_partial(run_id)
        
        # 更新 error_message
        self.repository.client.table('research_run')\
            .update({'error_message': error_msg})\
            .eq('id', run_id)\
            .execute()
    
    def _rollback_failed_run(self, run_id: int, error_message: str):
        """
        P0-5: Rollback 失败的 run
        
        删除 metric/provenance/artifact
        更新 run: status='failed', is_latest=false, error_message
        保留 run 和 chunks
        """
        self.repository.rollback_failed_run(run_id, error_message)
    
    # ========== 辅助方法 ==========
    
    def _find_registry_entry(self, key: str, registry_entries: List[tuple]) -> Optional[Any]:
        """从 registry_entries 中查找 key 对应的 entry"""
        for field_spec, entry in registry_entries:
            if entry.key == key:
                return entry
        return None
    
    def _find_metric_by_key(self, metrics: List[Dict], key: str) -> Optional[Dict]:
        """从 metrics 中查找 key 对应的 metric"""
        for metric in metrics:
            if metric.get('key') == key:
                return metric
        return None
    
    def _save_metrics_to_db(self, metrics_data: List[Dict]) -> List[Dict]:
        """批量保存 metrics 到数据库，返回带 id 的记录"""
        try:
            result = self.repository.client.table('metric')\
                .insert(metrics_data)\
                .execute()
            
            saved_metrics = result.data if result.data else []
            with_embedding = sum(1 for m in metrics_data if m.get('embedding'))
            
            logger.info(f"保存指标成功: {len(saved_metrics)} 条（{with_embedding} 条包含 embedding）")
            
            return saved_metrics
        except Exception as e:
            logger.error(f"保存指标失败: {e}")
            raise
    
    def _save_provenances_to_db(self, provenances: List[Dict]):
        """批量保存 provenance 到数据库"""
        try:
            result = self.repository.client.table('metric_provenance')\
                .insert(provenances)\
                .execute()
            
            logger.info(f"保存 metric provenance 成功: {len(provenances)} 条")
        except Exception as e:
            logger.error(f"保存 metric provenance 失败: {e}")
            raise
    
    def can_continue_on_error(self) -> bool:
        """允许继续处理其他 runs"""
        return True
