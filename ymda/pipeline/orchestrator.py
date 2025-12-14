"""Pipeline 调度器"""

from typing import Any, Dict, List
from ymda.settings import Settings
from ymda.pipeline.steps.validate_step import ValidateStep
from ymda.pipeline.steps.preprocess_step import PreprocessStep
from ymda.pipeline.steps.registry_step import RegistryStep  # 新增
from ymda.pipeline.steps.research_step import ResearchStep
from ymda.pipeline.steps.chunk_step import ChunkStep  # 新增
from ymda.pipeline.steps.store_step import StoreStep
from ymda.pipeline.steps.quality_step import QualityStep
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class PipelineOrchestrator:
    """Pipeline 调度器 - 管理整个处理流程"""
    
    def __init__(self, settings: Settings):
        """初始化调度器"""
        self.settings = settings
        self.steps: List[Any] = []
        self._initialize_steps()
    
    def _initialize_steps(self):
        """初始化所有步骤（新版流程）"""
        self.steps = [
            ValidateStep(self.settings),
            PreprocessStep(self.settings),
            RegistryStep(self.settings),  # 新增: 生成metric_key_registry
            ResearchStep(self.settings),
            ChunkStep(self.settings),     # 新增: 切分为research_chunk
            StoreStep(self.settings),     # 重写: ExtractorAgent + Provenance
            QualityStep(self.settings),
        ]
        logger.info(f"Initialized {len(self.steps)} pipeline steps (新版架构)")
    
    def run(self, input_data: Dict[str, Any]) -> Dict[str, Any]:
        """运行整个 pipeline（已废弃，请使用逐步执行）"""
        context = {"input": input_data, "errors": []}
        
        for step in self.steps:
            try:
                logger.info(f"Running step: {step.__class__.__name__}")
                context = step.execute(context)
                if context.get("stop", False):
                    logger.warning("Pipeline stopped by step")
                    break
            except Exception as e:
                logger.error(f"Error in step {step.__class__.__name__}: {e}")
                context["errors"].append({
                    "step": step.__class__.__name__,
                    "error": str(e)
                })
                if not step.can_continue_on_error():
                    raise
        
        return context
    
    @property
    def step_names(self) -> List[str]:
        """获取所有步骤名称"""
        return [step.__class__.__name__ for step in self.steps]

