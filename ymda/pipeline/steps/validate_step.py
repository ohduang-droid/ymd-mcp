"""验证步骤"""

from typing import Any, Dict, List, Optional
from abc import ABC, abstractmethod
from ymda.settings import Settings
from ymda.data.repository import get_repository, SupabaseRepository
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class ValidationError(Exception):
    """验证异常"""
    pass


class BaseStep(ABC):
    """步骤基类"""
    
    def __init__(self, settings: Settings):
        self.settings = settings
    
    @abstractmethod
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """执行步骤"""
        pass
    
    def can_continue_on_error(self) -> bool:
        """错误时是否继续执行"""
        return False


class YMLValidator:
    """YML数据验证器"""
    
    REQUIRED_FIELDS = ['ym_id', 'name']
    
    def validate(self, ym_data: Dict[str, Any]) -> List[str]:
        """验证YML数据，返回错误列表"""
        errors = []
        
        # 必填字段检查
        for field in self.REQUIRED_FIELDS:
            if field not in ym_data or not ym_data[field]:
                errors.append(f"缺少必填字段: {field}")
        
        # 数据类型检查
        if 'ym_id' in ym_data and not isinstance(ym_data['ym_id'], str):
            errors.append("ym_id必须是字符串类型")
        
        if 'name' in ym_data and not isinstance(ym_data['name'], str):
            errors.append("name必须是字符串类型")
        
        # 唯一性检查（在列表中检查）
        return errors


class YMQLValidator:
    """YMQL数据验证器"""
    
    REQUIRED_FIELDS = ['question_id', 'question_text', 'type']
    VALID_TYPES = ['text', 'number', 'boolean', 'enum', 'table']
    
    def validate(self, question_data: Dict[str, Any]) -> List[str]:
        """验证YMQL数据，返回错误列表"""
        errors = []
        
        # 必填字段检查
        for field in self.REQUIRED_FIELDS:
            if field not in question_data or not question_data[field]:
                errors.append(f"缺少必填字段: {field}")
        
        # 问题类型检查
        if 'type' in question_data:
            if question_data['type'] not in self.VALID_TYPES:
                errors.append(f"无效的问题类型: {question_data['type']}，有效类型: {self.VALID_TYPES}")
        
        # 数据类型检查
        if 'question_id' in question_data and not isinstance(question_data['question_id'], str):
            errors.append("question_id必须是字符串类型")
        
        if 'question_text' in question_data and not isinstance(question_data['question_text'], str):
            errors.append("question_text必须是字符串类型")
        
        return errors


class ValidateStep(BaseStep):
    """验证步骤 - 验证输入数据"""
    
    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.yml_validator = YMLValidator()
        self.ymql_validator = YMQLValidator()
        self.repository: Optional[SupabaseRepository] = None
        self._initialize_repository()
    
    def _initialize_repository(self):
        """初始化数据库仓储（使用单例）"""
        try:
            # 先检查配置
            if not self.settings.supabase_url or not self.settings.supabase_key:
                missing = []
                if not self.settings.supabase_url:
                    missing.append("SUPABASE_URL")
                if not self.settings.supabase_key:
                    missing.append("SUPABASE_KEY 或 SUPABASE_SERVICE_ROLE_KEY")
                logger.warning(
                    f"ValidateStep: Supabase 配置不完整，缺失: {', '.join(missing)}。"
                    f"数据库写入功能将被跳过。请检查 .env 文件或环境变量。"
                )
                self.repository = None
                return
            
            self.repository = get_repository(self.settings)
            if self.repository:
                logger.info("ValidateStep 数据库仓储初始化成功（使用单例）")
            else:
                logger.warning(
                    "ValidateStep: 数据库仓储未初始化，将跳过数据库写入。"
                    "可能原因：1) Supabase 配置不完整 2) 数据库连接失败 3) 网络问题"
                )
        except Exception as e:
            logger.error(f"ValidateStep 数据库仓储初始化失败: {e}，将跳过数据库写入")
            self.repository = None
    
    def _save_ym_to_database(self, ym_data: Dict[str, Any]) -> bool:
        """将单个YM数据保存到数据库（使用仓储的公共方法）"""
        if not self.repository:
            return False
        
        # 使用仓储的公共方法 upsert_ym_by_slug
        return self.repository.upsert_ym_by_slug(ym_data)
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """执行验证"""
        logger.info("Validating input data")
        input_data = context.get("input", {})
        
        if not input_data:
            raise ValidationError("Input data is empty")
        
        all_errors = []
        
        # 验证YML列表
        ym_list = input_data.get("yml_list", [])
        if not isinstance(ym_list, list):
            raise ValidationError("yml_list必须是列表类型")
        
        ym_ids = set()
        for idx, ym in enumerate(ym_list):
            errors = self.yml_validator.validate(ym)
            if errors:
                all_errors.extend([f"YML[{idx}]: {e}" for e in errors])
            
            # 唯一性检查
            if 'ym_id' in ym:
                if ym['ym_id'] in ym_ids:
                    all_errors.append(f"YML[{idx}]: ym_id重复: {ym['ym_id']}")
                ym_ids.add(ym['ym_id'])
        
        # 验证YMQL列表 - 支持两种格式:
        # 1. 顶层 question_list
        # 2. 嵌套在 yml_list[i].question_list 中
        question_list = []
        
        # 先获取顶层的 question_list
        top_level_questions = input_data.get("question_list", [])
        if not isinstance(top_level_questions, list):
            raise ValidationError("question_list必须是列表类型")
        question_list.extend(top_level_questions)
        
        # 再从每个 YM 中提取嵌套的 question_list
        for idx, ym in enumerate(ym_list):
            nested_questions = ym.get("question_list", [])
            if nested_questions:
                if not isinstance(nested_questions, list):
                    all_errors.append(f"YML[{idx}]: question_list必须是列表类型")
                    continue
                
                # 为嵌套问题添加 ym_id 引用
                ym_id = ym.get('ym_id')
                for question in nested_questions:
                    # 如果问题中没有 ym_id，添加它
                    if 'ym_id' not in question and ym_id:
                        question['ym_id'] = ym_id
                    question_list.append(question)
        
        logger.info(f"提取到 {len(question_list)} 个问题 (顶层: {len(top_level_questions)}, 嵌套: {len(question_list) - len(top_level_questions)})")
        
        # 验证所有问题
        question_ids = set()
        for idx, question in enumerate(question_list):
            errors = self.ymql_validator.validate(question)
            if errors:
                all_errors.extend([f"YMQL[{idx}]: {e}" for e in errors])
            
            # 唯一性检查
            if 'question_id' in question:
                if question['question_id'] in question_ids:
                    all_errors.append(f"YMQL[{idx}]: question_id重复: {question['question_id']}")
                question_ids.add(question['question_id'])
        
        if all_errors:
            error_msg = "验证失败:\n" + "\n".join(all_errors)
            logger.error(error_msg)
            raise ValidationError(error_msg)
        
        logger.info(f"验证通过: {len(ym_list)}个YM, {len(question_list)}个问题")
        
        # 将验证通过的yml_list写入数据库
        if self.repository:
            logger.info("开始将验证通过的YM数据写入数据库")
            saved_count = 0
            failed_count = 0
            
            for ym in ym_list:
                if self._save_ym_to_database(ym):
                    saved_count += 1
                else:
                    failed_count += 1
            
            logger.info(f"数据库写入完成: 成功 {saved_count} 个, 失败 {failed_count} 个")
        else:
            logger.info("数据库仓储未初始化，跳过数据库写入")
            saved_count = 0
            failed_count = 0
        
        # 构建输出结果，不包含 input 字段
        result = {
            "validated": True,
            "yml_list": ym_list,
            "question_list": question_list,
            "ym_saved_count": saved_count,
            "ym_failed_count": failed_count
        }
        
        return result

