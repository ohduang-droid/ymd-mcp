"""预处理步骤 - 使用AI分析问题列表，生成expected_fields（prompt_template使用固定模板）"""

import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Tuple
from pydantic import BaseModel, Field, field_validator, ValidationError
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from ymda.pipeline.steps.validate_step import BaseStep
from ymda.settings import Settings
from ymda.data.repository import get_repository
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class ExtractionRule(BaseModel):
    """数据提取规则 (⚠️ DEPRECATED - 新架构不使用parsed_struct)"""
    field_name: str = Field(description="目标字段名")
    json_path: str = Field(description="从 parsed_struct 解析值的 JSONPath (DEPRECATED)")
    default_value: Optional[Any] = Field(default=None, description="默认值")


class ExpectedField(BaseModel):
    """Expected Field 模型"""
    key: str = Field(description="metric.key，必须只包含小写英文字母、数字和下划线，禁止使用空格、特殊字符、中文等。例如：health_regulation_standard")
    json_path: str = Field(description="从 parsed_struct 解析值的 JSONPath")
    type: str = Field(description="决定写 metric 的 value_numeric/text/json。必须是 'numeric'、'text' 或 'json' 之一，禁止使用 'number' 等其他值")
    description: str = Field(description="Agent/MCP 使用")
    query: List[str] = Field(description="Agent 自动生成 SQL 时使用的查询操作符数组。每个操作符必须是以下值之一：'=', '<', '>', '<=', '>=', 'between', 'in', 'like'。禁止使用 '=>'、'=<' 等其他值")
    example: Optional[Union[int, float, str, List[str]]] = Field(
        default=None, 
        description="用于 Agent 感知大概量级（推荐字段）。numeric类型用数字，text类型用字符串，json类型用字符串数组"
    )
    
    @field_validator('key', mode='before')
    @classmethod
    def validate_key(cls, v: str) -> str:
        """验证并清理 key 字段，确保只包含小写字母、数字和下划线"""
        if not isinstance(v, str):
            v = str(v)
        
        # 检查是否包含中文字符
        if re.search(r'[\u4e00-\u9fff]', v):
            # 如果包含中文，尝试转换为拼音或使用下划线替换
            # 这里先简单处理：将非ASCII字符替换为下划线
            logger.warning(f"key 字段包含中文字符，将进行清理: {v}")
        
        # 清理：只保留小写字母、数字和下划线
        cleaned = re.sub(r'[^a-z0-9_]', '_', v.lower())
        # 合并多个连续的下划线
        cleaned = re.sub(r'_+', '_', cleaned)
        # 去除首尾下划线
        cleaned = cleaned.strip('_')
        
        if not cleaned:
            raise ValueError(f"key 字段清理后为空，原始值: {v}")
        
        if cleaned != v:
            logger.warning(f"key 字段已自动清理: '{v}' -> '{cleaned}'")
        
        return cleaned
    
    @field_validator('type')
    @classmethod
    def validate_type(cls, v: str) -> str:
        # 兼容处理：将 'number' 自动转换为 'numeric'
        if v.lower() == 'number':
            v = 'numeric'
            logger.debug(f"type 字段值 'number' 已自动转换为 'numeric'")
        
        if v not in ['numeric', 'text', 'json']:
            raise ValueError(f"type 必须是 'numeric', 'text' 或 'json'，当前值: {v}")
        return v
    
    @field_validator('query')
    @classmethod
    def validate_query(cls, v: List[str]) -> List[str]:
        valid_ops = ['=', '<', '>', '<=', '>=', 'between', 'in', 'like']
        # 常见错误操作符的映射
        op_corrections = {
            '=>': '>=',
            '=<': '<=',
            '==': '=',
            '!=': '=',  # != 不支持，转换为 =
        }
        
        corrected_ops = []
        for op in v:
            # 尝试修正常见错误
            if op in op_corrections:
                corrected_op = op_corrections[op]
                logger.debug(f"query 操作符已自动修正: '{op}' -> '{corrected_op}'")
                op = corrected_op
            
            if op not in valid_ops:
                raise ValueError(f"query 操作符必须是 {valid_ops} 之一，当前值: {op}")
            corrected_ops.append(op)
        
        return corrected_ops


class ExpectedFields(BaseModel):
    """Expected Fields 模型"""
    fields: List[ExpectedField] = Field(description="字段定义列表")


class QuestionAnalysisOutput(BaseModel):
    """问题分析输出模型（AI生成，不包含prompt_template）"""
    name: str = Field(description="问题名称（简洁）")
    description: str = Field(description="问题描述（详细说明问题的目的和范围）")
    expected_fields: ExpectedFields = Field(description="字段定义对象")


class PreprocessStep(BaseStep):
    """预处理步骤 - 使用两步LLM流程生成expected_fields和prompt_template"""
    
    def __init__(self, settings: Settings):
        super().__init__(settings)
        self._initialize_llm()
        self._load_prompt_templates()  # Load both prompt templates
        self._initialize_repository()
        self._registry_cache = None  # Cache for metric_key_registry
    
    def _initialize_llm(self):
        """初始化LangChain LLM"""
        self.llm = ChatOpenAI(
            model="gpt-4.1-mini",
            temperature=0.3,
            api_key=self.settings.openai_api_key
        )
        
        # 使用Pydantic模型定义输出结构（LangChain 1.0+方式）
        self.output_model = QuestionAnalysisOutput
    
    def _load_prompt_templates(self):
        """加载两个Prompt模板：expected_fields生成 和 prompt_template生成"""
        try:
            prompts_dir = Path(__file__).parent.parent.parent / "llm" / "prompts"
            
            # 加载 Prompt #1: 生成 expected_fields
            prompt1_path = prompts_dir / "prompt_generate_expected_fields.md"
            if not prompt1_path.exists():
                raise FileNotFoundError(f"Prompt #1 模板不存在: {prompt1_path}")
            
            self.prompt_expected_fields = self._parse_prompt_file(prompt1_path)
            logger.debug(f"Prompt #1 加载成功: {prompt1_path}")
            
            # 加载 Prompt #2: 生成 prompt_template
            prompt2_path = prompts_dir / "prompt_generate_prompt_template.md"
            if not prompt2_path.exists():
                raise FileNotFoundError(f"Prompt #2 模板不存在: {prompt2_path}")
            
            self.prompt_template_generator = self._parse_prompt_file(prompt2_path)
            logger.debug(f"Prompt #2 加载成功: {prompt2_path}")
            
        except Exception as e:
            logger.error(f"加载Prompt模板失败: {e}")
            raise
    
    def _parse_prompt_file(self, file_path: Path) -> ChatPromptTemplate:
        """解析prompt文件，提取system和user消息"""
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 分离 system 和 user 部分
        parts = content.split('---')
        system_message = None
        user_message = None
        
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if part.startswith('system:'):
                system_message = part.replace('system:', '', 1).strip()
            elif part.startswith('user:'):
                user_message = part.replace('user:', '', 1).strip()
        
        if not system_message or not user_message:
            raise ValueError(f"Prompt文件格式错误: {file_path}")
        
        return ChatPromptTemplate.from_messages([
            ("system", system_message),
            ("human", user_message)
        ])
    
    def _initialize_repository(self):
        """初始化数据库仓储"""
        try:
            self.repository = get_repository(self.settings)
            if self.repository:
                logger.debug("数据库仓储初始化成功")
            else:
                logger.warning("数据库仓储未初始化，将跳过数据库写入")
        except Exception as e:
            logger.warning(f"初始化数据库仓储失败: {e}，将跳过数据库写入")
            self.repository = None
    
    def _load_metric_key_registry(self) -> List[Dict[str, Any]]:
        """
        从数据库加载 metric_key_registry
        
        使用缓存策略：第一次加载后缓存，避免重复查询
        
        Returns:
            List[Dict]: registry 列表
        """
        if self._registry_cache is not None:
            logger.debug("使用缓存的 metric_key_registry")
            return self._registry_cache
        
        if not self.repository:
            logger.warning("Repository 未初始化，无法加载 registry")
            return []
        
        try:
            registry = self.repository.list_all_registry_keys()
            self._registry_cache = registry
            logger.info(f"加载 metric_key_registry 成功: {len(registry)} 个 keys")
            return registry
        except Exception as e:
            logger.error(f"加载 metric_key_registry 失败: {e}")
            return []
    
    def _filter_registry_for_extraction(self, registry: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        过滤 registry，只保留适合结构化提取的字段
        
        规则:
        - 只保留 query_capability = "strong_structured" 或 "filter_only"
        - 排除 type = "text", "json", "note", "summary" 的字段
        
        Args:
            registry: 完整的 registry 列表
            
        Returns:
            List[Dict]: 过滤后的 registry
        """
        filtered = []
        excluded_types = {'text', 'json', 'note', 'summary'}
        
        for item in registry:
            # 检查 query_capability
            query_cap = item.get('query_capability', '')
            if query_cap not in ['strong_structured', 'filter_only']:
                continue
            
            # 检查 type (使用 value_type 字段)
            value_type = item.get('value_type', item.get('type', ''))
            if value_type in excluded_types:
                continue
            
            filtered.append(item)
        
        logger.debug(f"Registry 过滤: {len(registry)} -> {len(filtered)} 个可用字段")
        return filtered
    
    def _generate_expected_fields(
        self,
        ymq_name: str,
        ymq_description: str,
        registry: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        使用 Prompt #1 生成 expected_fields (use_fields 格式)
        
        Args:
            ymq_name: YMQ 名称
            ymq_description: YMQ 描述
            registry: 过滤后的 metric_key_registry
            
        Returns:
            Dict: {"use_fields": [...]}
        """
        # 格式化 registry 为可读文本
        registry_text = self._format_registry_for_prompt(registry)
        
        # 调用 LLM
        chain = self.prompt_expected_fields | self.llm
        response = chain.invoke({
            "ymq_name": ymq_name,
            "ymq_description": ymq_description,
            "metric_key_registry": registry_text
        })
        
        # 解析 JSON 响应
        content = response.content if hasattr(response, 'content') else str(response)
        logger.debug(f"LLM 原始响应 (前500字符): {content[:500]}")
        expected_fields = self._parse_json_response(content)
        
        # 验证格式 (支持两种格式的兼容性)
        if 'use_fields' not in expected_fields:
            # 兼容性处理：如果LLM返回了 expected_fields 而不是 use_fields
            if 'expected_fields' in expected_fields:
                logger.warning("LLM 返回了 'expected_fields' 而不是 'use_fields'，正在转换...")
                fields_data = expected_fields['expected_fields']
                
                # 如果是简单的key列表，转换为use_fields格式
                if isinstance(fields_data, list) and fields_data and isinstance(fields_data[0], str):
                    use_fields = [
                        {"key": key, "role": ["filter"], "required": True}
                        for key in fields_data
                    ]
                    expected_fields = {"use_fields": use_fields}
                    logger.info(f"已自动转换为 use_fields 格式: {len(use_fields)} 个字段")
                else:
                    # 如果已经是对象列表，直接重命名
                    expected_fields = {"use_fields": fields_data}
            else:
                logger.error(f"LLM 返回的完整 JSON: {expected_fields}")
                raise ValueError("LLM 输出缺少 use_fields 字段")
        
        # 验证所有 key 都在 registry 中
        self._validate_keys_in_registry(expected_fields['use_fields'], registry)
        
        logger.info(f"生成 expected_fields 成功: {len(expected_fields['use_fields'])} 个字段")
        return expected_fields
    
    def _generate_prompt_template(
        self,
        ymq_name: str,
        ymq_description: str,
        expected_fields: Dict[str, Any]
    ) -> str:
        """
        使用 Prompt #2 生成 prompt_template
        
        Args:
            ymq_name: YMQ 名称
            ymq_description: YMQ 描述
            expected_fields: 已生成的 expected_fields
            
        Returns:
            str: prompt_template 文本
        """
        # 格式化 expected_fields 为 JSON 字符串
        expected_fields_json = json.dumps(expected_fields, ensure_ascii=False, indent=2)
        
        # 调用 LLM
        chain = self.prompt_template_generator | self.llm
        response = chain.invoke({
            "ymq_name": ymq_name,
            "ymq_description": ymq_description,
            "expected_fields": expected_fields_json
        })
        
        # 提取文本
        prompt_template = response.content if hasattr(response, 'content') else str(response)
        
        # 验证必需的占位符
        required_placeholders = ['{{YM_NAME}}', '{{YM_DESC}}', '{{expected_fields}}']
        missing = [p for p in required_placeholders if p not in prompt_template]
        if missing:
            logger.warning(f"生成的 prompt_template 缺少占位符: {missing}")
        
        logger.info("生成 prompt_template 成功")
        return prompt_template.strip()
    
    def _format_registry_for_prompt(self, registry: List[Dict[str, Any]]) -> str:
        """格式化 registry 为 LLM 可读的文本"""
        lines = []
        for item in registry:
            key = item.get('key', '')
            canonical_name = item.get('canonical_name', '')
            value_type = item.get('value_type', item.get('type', ''))
            query_cap = item.get('query_capability', '')
            description = item.get('description', '')
            
            lines.append(f"- key: {key}")
            lines.append(f"  canonical_name: {canonical_name}")
            lines.append(f"  value_type: {value_type}")
            lines.append(f"  query_capability: {query_cap}")
            if description:
                lines.append(f"  description: {description}")
            lines.append("")  # 空行分隔
        
        return "\n".join(lines)
    
    def _parse_json_response(self, content: str) -> Dict[str, Any]:
        """解析 LLM 返回的 JSON（可能包含 markdown 代码块）"""
        # 尝试提取 JSON（可能在 markdown 代码块中）
        json_match = re.search(r'```(?:json)?\s*({.*?})\s*```', content, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            # 尝试直接解析
            json_str = content.strip()
        
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            logger.error(f"JSON 解析失败: {e}\n内容: {content[:500]}")
            raise ValueError(f"LLM 输出不是有效的 JSON: {e}")
    
    def _validate_keys_in_registry(
        self,
        use_fields: List[Dict[str, Any]],
        registry: List[Dict[str, Any]]
    ) -> None:
        """验证 use_fields 中的所有 key 都在 registry 中"""
        registry_keys = {item['key'] for item in registry}
        
        invalid_keys = []
        for field in use_fields:
            key = field.get('key', '')
            if key not in registry_keys:
                invalid_keys.append(key)
        
        if invalid_keys:
            raise ValueError(
                f"以下 keys 不在 metric_key_registry 中: {invalid_keys}\n"
                f"可用的 keys: {sorted(registry_keys)}"
            )

    def _generate_key(self, question_id: str, question_text: str) -> str:
        """
        生成唯一的 key
        
        Args:
            question_id: 问题ID
            question_text: 问题文本
            
        Returns:
            生成的 key，格式：yq_{category}_{subcategory}
        """
        # 从 question_id 提取基础标识
        base_id = question_id.replace('q_', '').replace('question_', '')
        
        # 从问题文本中提取关键词
        # 简化处理：基于问题文本的前几个关键词生成
        text_lower = question_text.lower()
        
        # 常见分类关键词
        category_map = {
            '财务': 'financial',
            '用户': 'user',
            '目标': 'target',
            '定价': 'pricing',
            '成本': 'cost',
            '收入': 'revenue',
            '市场': 'market',
            '技术': 'tech',
            '功能': 'function',
            '性能': 'performance'
        }
        
        # 尝试匹配分类
        category = 'general'
        for keyword, cat in category_map.items():
            if keyword in text_lower:
                category = cat
                break
        
        # 生成 key
        key = f"yq_{category}_{base_id}"
        
        # 确保 key 符合命名规范（小写、下划线）
        key = re.sub(r'[^a-z0-9_]', '_', key.lower())
        key = re.sub(r'_+', '_', key)  # 合并多个下划线
        key = key.strip('_')
        
        return key
    
    def _validate_expected_fields(self, expected_fields: Dict[str, Any]) -> bool:
        """
        验证 expected_fields 是否符合 use_fields 规范
        
        Args:
            expected_fields: expected_fields 字典
            
        Returns:
            是否符合规范
        """
        try:
            if 'use_fields' not in expected_fields:
                logger.error("expected_fields 缺少 'use_fields' 字段")
                return False
            
            use_fields = expected_fields['use_fields']
            if not isinstance(use_fields, list):
                logger.error("expected_fields.use_fields 必须是数组")
                return False
            
            if len(use_fields) == 0:
                logger.error("expected_fields.use_fields 不能为空")
                return False
            
            # 验证每个字段
            required_fields = ['key', 'role', 'required']
            for idx, field in enumerate(use_fields):
                if not isinstance(field, dict):
                    logger.error(f"expected_fields.use_fields[{idx}] 必须是对象")
                    return False
                
                # 检查必填字段
                if 'key' not in field:
                    logger.error(f"expected_fields.use_fields[{idx}] 缺少必填字段: key")
                    return False
                
                # 验证 role（可选，但如果存在必须是列表）
                if 'role' in field:
                    if not isinstance(field['role'], list):
                        logger.error(f"expected_fields.use_fields[{idx}].role 必须是数组")
                        return False
                
                # 验证 required（可选，但如果存在必须是布尔值）
                if 'required' in field:
                    if not isinstance(field['required'], bool):
                        logger.error(f"expected_fields.use_fields[{idx}].required 必须是布尔值")
                        return False
            
            return True
        except Exception as e:
            logger.error(f"验证 expected_fields 失败: {e}")
            return False
    
    def analyze_question(self, question: Dict[str, Any]) -> Dict[str, Any]:
        """
        分析问题，使用两步LLM流程生成 expected_fields 和 prompt_template
        
        Args:
            question: 问题字典，包含 question_id, question_text, type 等字段
            
        Returns:
            分析结果，包含 key, name, description, prompt_template, expected_fields
        """
        try:
            question_id = question.get('question_id', '')
            question_text = question.get('question_text', '')
            
            if not question_id:
                raise ValueError("问题缺少 question_id")
            if not question_text:
                raise ValueError("问题缺少 question_text")
            
            # Step 1: 加载并过滤 metric_key_registry
            logger.debug(f"开始分析问题: {question_id}")
            registry = self._load_metric_key_registry()
            if not registry:
                raise ValueError("无法加载 metric_key_registry")
            
            filtered_registry = self._filter_registry_for_extraction(registry)
            if not filtered_registry:
                raise ValueError("过滤后的 registry 为空，无可用字段")
            
            # Step 2: 使用 question_text 作为 ymq.name 和 description
            ymq_name = question_text
            ymq_description = question_text  # 可以后续优化为更详细的描述
            
            # Step 3: 调用 Prompt #1 生成 expected_fields
            logger.debug(f"调用 Prompt #1 生成 expected_fields...")
            expected_fields = self._generate_expected_fields(
                ymq_name=ymq_name,
                ymq_description=ymq_description,
                registry=filtered_registry
            )
            
            # Step 4: 调用 Prompt #2 生成 prompt_template
            logger.debug(f"调用 Prompt #2 生成 prompt_template...")
            prompt_template = self._generate_prompt_template(
                ymq_name=ymq_name,
                ymq_description=ymq_description,
                expected_fields=expected_fields
            )
            
            # Step 5: 生成 key
            key = self._generate_key(question_id, question_text)
            
            # Step 6: 构建返回结果
            analysis_result = {
                'key': key,
                'name': ymq_name,
                'description': ymq_description,
                'prompt_template': prompt_template,
                'expected_fields': expected_fields
            }
            
            logger.info(f"问题 {question_id} 分析完成，生成 {len(expected_fields.get('use_fields', []))} 个字段")
            return analysis_result
            
        except Exception as e:
            logger.error(f"分析问题失败: {e}")
            raise
    
    def _save_ymq_to_database(self, question_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        将处理后的问题数据写入数据库 ymq 表
        
        使用 upsert 逻辑：如果 key 已存在则更新，不存在则插入新记录
        
        Args:
            question_data: 处理后的问题数据，包含：
                - 分析结果：key, name, description, prompt_template, expected_fields
                - 原始问题：question_id, question_text, type, target_field, importance 等
            
        Returns:
            字典包含: {'success': bool, 'id': int | None, 'error': str | None}
        """
        if not self.repository:
            logger.warning("数据库仓储未初始化，跳过YMQ写入")
            return {'success': False, 'id': None, 'error': 'Repository not initialized'}
        
        try:
            key = question_data.get('key')
            if not key:
                logger.warning("问题数据缺少key，跳过数据库写入")
                return {'success': False, 'id': None, 'error': 'Missing key'}
            
            # 直接使用原始的 expected_fields，不添加 _meta 字段
            expected_fields = question_data.get('expected_fields', {})
            
            # ymq.name 必须使用 question_text
            question_text = question_data.get('question_text', '')
            if not question_text:
                logger.warning(f"问题数据缺少 question_text，无法设置 name，key={key}")
                question_text = question_data.get('name', '')  # 降级使用 AI 生成的 name
            
            ymq_data = {
                'key': key,
                'name': question_text,  # 使用 question_text 作为 name
                'description': question_data.get('description'),
                'prompt_template': question_data.get('prompt_template', ''),
                'expected_fields': expected_fields  # 直接使用原始的 expected_fields
            }
            
            # ⭐ 返回 upsert_ymq 的结果（包含 ID）
            result = self.repository.upsert_ymq(ymq_data)
            return {
                'success': result.get('success', False),
                'id': result.get('id'),
                'error': result.get('error')
            }
        except Exception as e:
            logger.error(f"保存YMQ到数据库失败: key={question_data.get('key', 'unknown')}, 错误={e}")
            return {'success': False, 'id': None, 'error': str(e)}
    
    def _process_single_question(self, question: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str], Optional[str]]:
        """
        处理单个问题（用于并发执行）
        
        Args:
            question: 问题字典
            
        Returns:
            (processed_question, question_id, error_message)
            - 成功: (processed_question, question_id, None)
            - 失败: (None, question_id, error_message)
        """
        question_id = question.get("question_id")
        if not question_id:
            return None, None, "缺少question_id"
        
        try:
            # 分析问题
            analysis_result = self.analyze_question(question)
            
            # 合并原始问题数据和分析结果
            processed_question = {
                **question,  # 保留原始字段
            }
            processed_question.update(analysis_result)
            
            # 步骤4: 保存到数据库并捕获 ID
            db_result = self._save_ymq_to_database(processed_question)
            if db_result['success']:
                # ⭐ 设置数据库 ID
                processed_question['db_id'] = db_result['id']
                processed_question['key'] = processed_question.get('key')
                logger.debug(f"✓ 问题 {question_id} 保存成功, db_id={db_result['id']}")
                return processed_question, question_id, None
            else:
                error_msg = f"问题 {question_id} 保存到数据库失败: {db_result.get('error', 'Unknown')}"
                logger.warning(error_msg)
                return processed_question, question_id, error_msg
                
        except Exception as e:
            error_msg = f"处理问题 {question_id} 失败: {e}"
            logger.error(error_msg)
            return None, question_id, str(e)
    
    def _save_failed_questions(self, failed_questions: List[Dict[str, Any]], output_file_path: Optional[str] = None) -> bool:
        """
        将失败的问题保存到 data-fail.json
        
        Args:
            failed_questions: 失败的问题列表
            output_file_path: 输出文件路径，如果为None则基于input_file_path生成
            
        Returns:
            成功返回 True，失败返回 False
        """
        if not failed_questions:
            return True
        
        try:
            # 构建失败数据
            fail_data = {
                "total_failed": len(failed_questions),
                "question_list": failed_questions
            }
            
            # 确定输出文件路径
            if not output_file_path:
                output_file_path = "data-fail.json"
            
            file_path = Path(output_file_path)
            
            # 写入文件
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(fail_data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"已保存 {len(failed_questions)} 个失败问题到: {output_file_path}")
            return True
        except Exception as e:
            logger.error(f"保存失败问题到文件失败: {e}")
            return False
    
    def _update_data_json_status(self, input_file_path: str, question_status_map: Dict[str, str]) -> bool:
        """
        更新data.json文件中问题的预处理状态
        
        Args:
            input_file_path: data.json文件路径
            question_status_map: 问题ID到状态的映射，状态为 'success' 或 'failed'
            
        Returns:
            成功返回 True，失败返回 False
        """
        try:
            file_path = Path(input_file_path)
            if not file_path.exists():
                logger.warning(f"文件不存在，无法更新状态: {input_file_path}")
                return False
            
            # 读取原始文件
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # 更新question_list中每个问题的状态
            if 'question_list' not in data:
                logger.warning("data.json中缺少question_list字段")
                return False
            
            updated_count = 0
            not_found_questions = []
            
            # 获取所有问题ID的集合，用于调试
            all_question_ids = {q.get('question_id') for q in data['question_list'] if q.get('question_id')}
            map_question_ids = set(question_status_map.keys())
            
            # 检查是否有问题ID在状态映射中但不在文件中
            missing_in_file = map_question_ids - all_question_ids
            if missing_in_file:
                logger.warning(f"以下问题ID在状态映射中但不在 data.json 中: {missing_in_file}")
            
            for question in data['question_list']:
                question_id = question.get('question_id')
                if question_id and question_id in question_status_map:
                    old_status = question.get('preprocess_status', '未设置')
                    question['preprocess_status'] = question_status_map[question_id]
                    updated_count += 1
                    if old_status != question_status_map[question_id]:
                        logger.debug(f"更新问题 {question_id} 状态: {old_status} -> {question_status_map[question_id]}")
                elif question_id:
                    # 问题ID存在但不在状态映射中（可能是跳过的没有question_id的问题）
                    not_found_questions.append(question_id)
            
            if not_found_questions:
                logger.warning(f"以下问题ID在 data.json 中但不在状态映射中（可能被跳过）: {not_found_questions[:10]}...")  # 只显示前10个
            
            # 写回文件
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.debug(f"已更新data.json中 {updated_count} 个问题的预处理状态")
            if updated_count != len(question_status_map):
                logger.warning(f"状态更新数量不匹配: 期望 {len(question_status_map)} 个，实际更新 {updated_count} 个")
            return True
        except Exception as e:
            logger.error(f"更新data.json状态失败: {e}")
            return False
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """执行预处理（并发处理，一次10个并发）"""
        logger.info("Preprocessing data - analyzing questions and generating expected_fields (并发处理，一次10个)")
        
        if not context.get("validated", False):
            raise ValueError("数据未通过验证，无法进行预处理")
        
        question_list = context.get("question_list", [])
        if not question_list:
            raise ValueError("question_list 不能为空")
        
        if not isinstance(question_list, list):
            raise ValueError("question_list 必须是列表类型")
        
        # 过滤出有question_id的问题
        valid_questions = [q for q in question_list if q.get("question_id")]
        skipped_count = len(question_list) - len(valid_questions)
        if skipped_count > 0:
            logger.warning(f"跳过 {skipped_count} 个没有question_id的问题")
        
        processed_questions = []
        failed_questions = []  # 保存失败的问题（包含错误信息）
        saved_count = 0
        failed_count = 0
        question_status_map = {}  # 跟踪每个问题的状态: question_id -> 'success' 或 'failed'
        
        # 使用线程池并发处理，一次处理10个
        max_workers = 10
        logger.info(f"开始并发处理 {len(valid_questions)} 个问题，并发数: {max_workers}")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交所有任务
            future_to_question = {
                executor.submit(self._process_single_question, question): question
                for question in valid_questions
            }
            
            # 收集结果
            completed = 0
            for future in as_completed(future_to_question):
                completed += 1
                question = future_to_question[future]
                question_id = question.get("question_id", "unknown")
                
                # 每10个问题输出一次进度
                if completed % 10 == 0 or completed == len(valid_questions):
                    logger.info(f"处理进度: {completed}/{len(valid_questions)} ({completed*100//len(valid_questions)}%)")
                
                try:
                    processed_question, result_question_id, error_message = future.result()
                    
                    if processed_question:
                        # 分析成功（无论保存是否成功，都保留在 context 中）
                        processed_questions.append(processed_question)
                        
                        if not error_message:
                            # 保存成功
                            saved_count += 1
                            question_status_map[question_id] = 'success'
                        else:
                            # 分析成功但保存失败
                            failed_count += 1
                            question_status_map[question_id] = 'failed'
                            logger.warning(f"问题 {question_id} 保存失败: {error_message}")
                            
                            # 保存失败的问题信息到 data-fail.json
                            failed_question = {
                                **question,  # 保留原始字段
                                "error": error_message,
                                "preprocess_status": "failed"
                            }
                            failed_questions.append(failed_question)
                    else:
                        # 分析失败
                        failed_count += 1
                        question_status_map[question_id] = 'failed'
                        
                        # 保存失败的问题信息
                        failed_question = {
                            **question,  # 保留原始字段
                            "error": error_message or "未知错误",
                            "preprocess_status": "failed"
                        }
                        failed_questions.append(failed_question)
                        logger.warning(f"问题 {question_id} 处理失败: {error_message}")
                        
                except Exception as e:
                    # 处理异常
                    failed_count += 1
                    question_status_map[question_id] = 'failed'
                    error_msg = f"处理异常: {str(e)}"
                    
                    failed_question = {
                        **question,
                        "error": error_msg,
                        "preprocess_status": "failed"
                    }
                    failed_questions.append(failed_question)
                    logger.error(f"问题 {question_id} 处理异常: {e}")
        
        context["question_list"] = processed_questions
        context["preprocessed"] = True
        context["ymq_saved_count"] = saved_count
        context["ymq_failed_count"] = failed_count
        context["question_status_map"] = question_status_map  # 保存状态映射，供后续步骤使用
        
        logger.info(f"预处理完成: 总计 {len(valid_questions)} 个，成功 {saved_count} 个，失败 {failed_count} 个")
        
        # 保存失败的问题到 data-fail.json
        if failed_questions:
            input_file_path = context.get("input_file_path")
            if input_file_path:
                # 基于输入文件路径生成失败文件路径
                input_path = Path(input_file_path)
                fail_file_path = input_path.parent / "data-fail.json"
            else:
                fail_file_path = "data-fail.json"
            
            self._save_failed_questions(failed_questions, str(fail_file_path))
        
        # 如果context中有原始输入文件路径，更新data.json状态
        input_file_path = context.get("input_file_path")
        if input_file_path and question_status_map:
            result = self._update_data_json_status(input_file_path, question_status_map)
            if not result:
                logger.error(f"更新 data.json 状态失败")
        
        return context
