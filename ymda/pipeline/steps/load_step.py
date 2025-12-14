"""加载步骤"""

from typing import Any, Dict, List, Optional
from ymda.pipeline.steps.validate_step import BaseStep
from ymda.data.repository import get_repository
from ymda.settings import Settings
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class LoadStep(BaseStep):
    """加载步骤 - 从数据库加载YM和问题数据"""
    
    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.repository = None
        self._initialize_repository()
    
    def _initialize_repository(self):
        """初始化数据库仓储"""
        try:
            self.repository = get_repository(self.settings)
            if self.repository:
                logger.debug("LoadStep 数据库仓储初始化成功")
            else:
                logger.warning("LoadStep 数据库仓储未初始化，加载功能将不可用")
        except Exception as e:
            logger.warning(f"LoadStep 初始化数据库仓储失败: {e}")
            self.repository = None
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """执行加载"""
        logger.info("Loading data from database")
        
        if not self.repository:
            logger.warning("数据库未初始化，跳过加载步骤")
            return context
        
        # 1. 加载活跃的YM
        try:
            active_yms = self.repository.get_active_yms()
            ym_list = []
            ym_summaries = {}
            
            for ym in active_yms:
                # 映射数据库字段到 Pipeline上下文格式
                ym_id = ym.get('slug') or ym.get('ym_id') or str(ym.get('id'))
                
                ym_item = {
                    'id': ym.get('id'), # 数据库自增ID，用于外键
                    'ym_id': ym_id,     # slug
                    'name': ym.get('name', 'Unknown'),
                    'category': ym.get('category', 'unknown'),
                    'short_desc': ym.get('description', ''),
                }
                ym_list.append(ym_item)
                
                # 构建摘要
                if ym.get('description'):
                    ym_summaries[ym_id] = {
                        'summary': ym.get('description'),
                        'use_cases': [] # 数据库暂无 separate use_cases, 留空
                    }
            
            context['yml_list'] = ym_list
            context['ym_summaries'] = ym_summaries
            logger.info(f"已加载 {len(ym_list)} 个活跃YM")
            
        except Exception as e:
            logger.error(f"加载YM失败: {e}")
            raise
            
        # 2. 加载问题 (从 YMQ 表)
        try:
            all_questions = self.repository.get_all_questions()
            question_list = []
            
            for q in all_questions:
                # 映射字段: YMQ(key, name, expected_fields) -> Pipe(question_id, question_text)
                q_item = {
                    'id': q.get('id'),  # 数据库自增ID，用于外键
                    'question_id': q.get('key'),
                    'name': q.get('name'),  # 保留 name 字段用于 prompt
                    'description': q.get('description'),  # 保留 description 字段用于 prompt
                    'question_text': q.get('name'), # YMQ.name 是问题文本（向后兼容）
                    'type': 'text', # 默认为文本，YMQ表不存type
                    'expected_fields': q.get('expected_fields'), # 保留以供参考
                    'prompt_template': q.get('prompt_template')
                }
                question_list.append(q_item)
            
            context['question_list'] = question_list
            logger.info(f"已加载 {len(question_list)} 个问题")
            
        except Exception as e:
            logger.error(f"加载问题失败: {e}")
            raise
        
        # 标记数据已验证/预处理 (因为是从DB加载的)
        context['validated'] = True
        context['preprocessed'] = True
        
        return context
