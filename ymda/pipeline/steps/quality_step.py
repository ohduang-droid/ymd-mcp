"""质量检查步骤"""

from typing import Any, Dict, List
from ymda.pipeline.steps.validate_step import BaseStep
from ymda.data.db import Database
from ymda.data.repository import get_repository, SupabaseRepository
from ymda.settings import Settings
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class QualityChecker:
    """质量检查器（使用Supabase）"""
    
    def __init__(self, repository: SupabaseRepository, confidence_threshold: float = 0.6):
        self.repository = repository
        self.confidence_threshold = confidence_threshold
    
    def check_completeness(self, ym_id: str, question_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """检查完整性（使用Supabase查询）"""
        try:
            # 查询已回答的问题
            answered_questions = self.repository.client.table('ym_answer')\
                .select('question_id')\
                .eq('ym_id', ym_id)\
                .execute()
            
            answered_ids = {q['question_id'] for q in answered_questions.data}
            expected_ids = {q['question_id'] for q in question_list}
            
            missing = expected_ids - answered_ids
            
            return {
                "is_complete": len(missing) == 0,
                "missing_questions": list(missing),
                "completion_rate": len(answered_ids) / len(expected_ids) if expected_ids else 0
            }
        except Exception as e:
            logger.error(f"检查完整性失败: {e}")
            return {
                "is_complete": False,
                "missing_questions": [],
                "completion_rate": 0.0,
                "error": str(e)
            }
    
    def check_confidence_threshold(self, ym_id: str) -> Dict[str, Any]:
        """检查置信度阈值（使用Supabase查询）"""
        try:
            answers = self.repository.client.table('ym_answer')\
                .select('question_id, confidence_score, version')\
                .eq('ym_id', ym_id)\
                .execute()
            
            low_confidence = [
                {
                    "question_id": a['question_id'],
                    "confidence": float(a['confidence_score']) if a.get('confidence_score') else 0.0,
                    "version": a.get('version', 1)
                }
                for a in answers.data
                if a.get('confidence_score') and float(a['confidence_score']) < self.confidence_threshold
            ]
            
            return {
                "has_low_confidence": len(low_confidence) > 0,
                "low_confidence_answers": low_confidence
            }
        except Exception as e:
            logger.error(f"检查置信度失败: {e}")
            return {
                "has_low_confidence": False,
                "low_confidence_answers": [],
                "error": str(e)
            }
    
    def mark_anomalies(self, ym_id: str, question_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """标记异常"""
        anomalies = []
        
        # 检查完整性
        completeness = self.check_completeness(ym_id, question_list)
        if not completeness["is_complete"]:
            anomalies.append({
                "type": "incomplete",
                "details": completeness
            })
        
        # 检查置信度
        confidence = self.check_confidence_threshold(ym_id)
        if confidence["has_low_confidence"]:
            anomalies.append({
                "type": "low_confidence",
                "details": confidence
            })
        
        return anomalies


class QualityStep(BaseStep):
    """质量检查步骤 - 验证输出质量"""
    
    def __init__(self, settings: Settings):
        super().__init__(settings)
        self.db: Database = None
        self.repository: SupabaseRepository = None
        self.quality_checker: QualityChecker = None
        self._initialize()
    
    def _initialize(self):
        """初始化质量检查器（使用单例）"""
        try:
            # 使用统一的获取方法
            self.repository = get_repository(self.settings)
            if self.repository:
                self.db = self.repository.db
                self.quality_checker = QualityChecker(
                    self.repository, 
                    confidence_threshold=0.6
                )
                logger.info("质量检查步骤初始化成功（使用单例）")
            else:
                logger.warning("数据库仓储未初始化，质量检查功能将不可用")
        except Exception as e:
            logger.warning(f"质量检查步骤初始化失败（可能配置不完整）: {e}")
            # 不抛出异常，允许在配置不完整时继续
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """执行质量检查"""
        logger.info("Running quality check")
        
        if not self.quality_checker:
            logger.warning("质量检查器未初始化，跳过质量检查")
            context["quality_passed"] = False
            return context
        
        stored = context.get("stored", False)
        if not stored:
            logger.warning("数据未存储，跳过质量检查")
            context["quality_passed"] = False
            return context
        
        ym_list = context.get("yml_list", [])
        question_list = context.get("question_list", [])
        
        all_anomalies = {}
        quality_report = {}
        
        # 对每个YM进行质量检查
        for ym in ym_list:
            ym_id = ym.get("ym_id")
            if not ym_id:
                continue
            
            try:
                anomalies = self.quality_checker.mark_anomalies(ym_id, question_list)
                all_anomalies[ym_id] = anomalies
                
                # 生成质量报告
                completeness = self.quality_checker.check_completeness(ym_id, question_list)
                confidence = self.quality_checker.check_confidence_threshold(ym_id)
                
                quality_report[ym_id] = {
                    "completeness": completeness,
                    "confidence": confidence,
                    "anomalies": anomalies,
                    "passed": len(anomalies) == 0
                }
                
                if anomalies:
                    logger.warning(f"YM {ym_id} 存在质量问题: {len(anomalies)}个异常")
                else:
                    logger.info(f"YM {ym_id} 质量检查通过")
            except Exception as e:
                logger.error(f"质量检查失败: YM={ym_id}, {e}")
                quality_report[ym_id] = {
                    "error": str(e),
                    "passed": False
                }
        
        # 计算总体质量
        total_yms = len(quality_report)
        passed_yms = sum(1 for report in quality_report.values() if report.get("passed", False))
        overall_passed = passed_yms == total_yms if total_yms > 0 else False
        
        context["quality_report"] = quality_report
        context["quality_passed"] = overall_passed
        context["quality_summary"] = {
            "total_yms": total_yms,
            "passed_yms": passed_yms,
            "failed_yms": total_yms - passed_yms,
            "overall_passed": overall_passed
        }
        
        logger.info(f"质量检查完成: {passed_yms}/{total_yms}个YM通过")
        return context

