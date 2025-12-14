"""
Extract Step - 独立的结构化抽取步骤

P0-6 修正：失败 run 显式标记，避免幽灵 run
P0-7: 新增独立步骤，与 Research 完全解耦
"""

from typing import Any, Dict
import logging

from ymda.pipeline.steps.base_step import BaseStep
from ymda.utils.expected_fields_parser import ExpectedFieldsParser, ParsingError
from ymda.utils.registry_validator import RegistryValidator
from ymda.llm.extractor_agent import ExtractorAgent

logger = logging.getLogger(__name__)


class ExtractStep(BaseStep):
    """
    Extract Step - 结构化抽取
    
    职责:
    1. 解析 expected_fields → FieldSpec[]
    2. 验证 registry keys → RegistryEntry[]
    3. 调用 ExtractorAgent
    4. 返回 {structured, provenance}
    
    P0-6 修正：失败 run 显式标记 extraction_failed=true
    """
    
    def __init__(self, settings):
        super().__init__(settings)
        self.parser = ExpectedFieldsParser()
        self.validator = RegistryValidator(self.repository)
        self.extractor_agent = ExtractorAgent(settings)
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        执行抽取步骤
        
        输入: context with research_results (含 run_id, chunks)
        输出: context with extraction results
        
        失败处理:
        - expected_fields 为空 → status='failed', error='expected_fields_empty_after_parse'
        - registry missing keys → status='failed', error='registry_missing_keys: ...'
        - 抽取失败 → status='failed', error=原因
        - P0-6: 失败 run 显式标记 extraction_failed=true
        """
        research_results = context.get('research_results', [])
        
        for result in research_results:
            run_id = result.get('run_id')
            ymq = result.get('ymq', {})
            
            if not run_id:
                logger.warning("Result missing run_id, skipping")
                continue
            
            try:
                # 1. 解析 expected_fields
                logger.info(f"Run {run_id}: Parsing expected_fields")
                expected_fields = ymq.get('expected_fields')
                
                # P0-Fix: 如果 context 中没有 expected_fields，从数据库加载
                if not expected_fields:
                    logger.info(f"Run {run_id}: expected_fields not in context, loading from database")
                    # 从 research_run 获取 ymq_id
                    run_data = self.repository.client.table('research_run')\
                        .select('ymq_id')\
                        .eq('id', run_id)\
                        .single()\
                        .execute()
                    
                    if run_data.data:
                        ymq_id = run_data.data['ymq_id']
                        ymq_result = self.repository.client.table('ymq')\
                            .select('expected_fields')\
                            .eq('id', ymq_id)\
                            .single()\
                            .execute()
                        
                        if ymq_result.data:
                            expected_fields = ymq_result.data.get('expected_fields')
                            logger.info(f"Run {run_id}: Loaded expected_fields from ymq_id={ymq_id}")
                
                try:
                    field_specs = self.parser.parse(expected_fields)
                except ParsingError as e:
                    error_msg = str(e)
                    self._mark_failed(run_id, error_msg)
                    result['extraction_failed'] = True  # P0-6: 显式标记
                    logger.error(f"Run {run_id}: Parse failed - {error_msg}")
                    continue
                
                if not field_specs:
                    self._mark_failed(run_id, 'expected_fields_empty_after_parse')
                    result['extraction_failed'] = True  # P0-6: 显式标记
                    logger.error(f"Run {run_id}: No field specs after parsing")
                    continue
                
                logger.info(f"Run {run_id}: Parsed {len(field_specs)} field specs")
                
                # 2. 验证 registry
                logger.info(f"Run {run_id}: Validating registry")
                validation = self.validator.validate(field_specs)
                
                if validation.missing or validation.unsupported_types:
                    errors = []
                    if validation.missing:
                        errors.append(f"missing_keys: {','.join(validation.missing)}")
                    if validation.unsupported_types:
                        unsupported = [f"{k}({t})" for k, t in validation.unsupported_types]
                        errors.append(f"unsupported_types: {','.join(unsupported)}")
                    
                    error_msg = f"registry_validation_failed: {'; '.join(errors)}"
                    self._mark_failed(run_id, error_msg)
                    result['extraction_failed'] = True  # P0-6: 显式标记
                    logger.error(f"Run {run_id}: Registry validation failed - {error_msg}")
                    continue
                
                logger.info(f"Run {run_id}: {len(validation.matched)} keys validated")
                
                # 3. 获取 chunks
                chunks = self._get_chunks(run_id)
                if not chunks:
                    self._mark_failed(run_id, 'no_chunks_found')
                    result['extraction_failed'] = True
                    logger.error(f"Run {run_id}: No chunks found")
                    continue
                
                logger.info(f"Run {run_id}: Found {len(chunks)} chunks")
                
                # 4. 构建 flattened_fields (ExtractorAgent 期望的格式)
                flattened_fields = {}
                for field_spec, registry_entry in validation.matched:
                    flattened_fields[registry_entry.key] = {
                        'canonical_name': registry_entry.canonical_name,
                        'description': registry_entry.description,
                        'type': registry_entry.value_type,  # Fixed: use value_type (not type)
                        'unit': registry_entry.unit,
                        'required': field_spec.required
                    }
                
                # 5. 调用 ExtractorAgent
                logger.info(f"Run {run_id}: Starting extraction")
                extraction = self.extractor_agent.extract(
                    expected_fields=flattened_fields,
                    chunks=chunks
                )
                
                # 6. 保存到 context
                result['extraction'] = {
                    'structured': extraction.get('structured', {}),
                    'provenance': extraction.get('provenance', []),
                    'field_specs': field_specs,
                    'registry_entries': validation.matched
                }
                result['extraction_failed'] = False  # P0-6: 明确成功
                
                logger.info(f"Run {run_id}: Extraction succeeded - "
                           f"{len(extraction.get('structured', {}))} fields, "
                           f"{len(extraction.get('provenance', []))} provenance")
                
            except Exception as e:
                self._mark_failed(run_id, str(e))
                result['extraction_failed'] = True  # P0-6: 显式标记
                logger.error(f"Run {run_id}: Extract failed - {e}", exc_info=True)
        
        return context
    
    def _mark_failed(self, run_id: int, error_message: str):
        """标记 run 为失败状态"""
        try:
            self.repository.client.table('research_run')\
                .update({
                    'status': 'failed',
                    'error_message': error_message,
                    'is_latest': False  # P0-5: 失败 run 不能是 latest
                })\
                .eq('id', run_id)\
                .execute()
            
            logger.info(f"Run {run_id}: Marked as failed - {error_message}")
        except Exception as e:
            logger.error(f"Failed to mark run {run_id} as failed: {e}")
    
    def _get_chunks(self, run_id: int) -> list:
        """获取 run 的所有 chunks"""
        try:
            result = self.repository.client.table('research_chunk')\
                .select('chunk_uid, content')\
                .eq('research_run_id', run_id)\
                .execute()
            
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Failed to get chunks for run {run_id}: {e}")
            return []
    
    def can_continue_on_error(self) -> bool:
        """允许继续处理其他 runs"""
        return True
