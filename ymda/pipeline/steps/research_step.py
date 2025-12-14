"""研究步骤"""

import re
import json
import time
import asyncio  # 新增：用于同步调用异步方法
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import datetime
from langchain_core.prompts import PromptTemplate
from langchain_openai import ChatOpenAI
from ymda.pipeline.steps.validate_step import BaseStep
from ymda.llm.deep_research_agent import Deep_ResearchAgent
from ymda.data.models import ResearchRun  # 新增
from ymda.data.repository import get_repository  # 新增
from ymda.settings import Settings
from ymda.utils.logger import get_logger
from ymda.utils.schema_utils import flatten_expected_fields

logger = get_logger(__name__)


class ResearchStep(BaseStep):
    """研究步骤 - 使用 Deep_Research (LangGraph) 进行深度研究"""
    
    LANGUAGE_REQUIREMENT_TAG = "[LANGUAGE REQUIREMENT]"
    LANGUAGE_REQUIREMENT_BLOCK = (
        "[LANGUAGE REQUIREMENT]\n"
        "You MUST deliver the entire research report, structured outputs, provenance evidence_text, and any explanations strictly in English. "
        "If your sources are not in English, summarize and translate them into English while preserving product names, numbers, and units."
    )
    ENGLISH_RETRY_BLOCK = (
        "[ENGLISH_ONLY_RETRY]\n"
        "The previous answer was not fully in English. Rewrite the whole report, structured data, and provenance strictly in English. "
        "Translate all content into English while keeping key terms intact."
    )
    NON_ENGLISH_PATTERN = re.compile(r'[\u4e00-\u9fff\u3040-\u30ff\uac00-\ud7af]')
    RATE_LIMIT_HINT_PATTERN = re.compile(r'try again in ([0-9.]+)s', re.IGNORECASE)
    RATE_LIMIT_MAX_RETRIES = 3
    RATE_LIMIT_INITIAL_DELAY = 8.0
    
    def __init__(self, settings: Settings):
        super().__init__(settings)
        # 使用 OpenAI API for Deep_Research
        api_key = settings.openai_api_key
        
        self.deep_research_client = Deep_ResearchAgent(
            api_key=api_key,
            model="gpt-4.1-mini"
        )
        self._load_research_prompt()
        self.post_structure_llm = ChatOpenAI(
            model="gpt-4.1-mini",
            temperature=0,
            api_key=api_key
        )
    
    def _load_research_prompt(self):
        """加载研究Prompt模板"""
        try:
            # 这里的路径假设是从项目根目录运行或者按照相对路径找到
            prompt_template_path = Path(__file__).parent.parent.parent / "llm" / "prompts" / "research.md"
            
            if not prompt_template_path.exists():
                logger.warning(f"研究Prompt模板文件不存在: {prompt_template_path}，使用默认模板")
                template_content = """请对以下问题进行研究。

## Yield Machine 信息
- 名称: {ym_name}
- 类别: {ym_category}
- 摘要: {ym_summary}
- 使用场景: {use_cases}

## 问题信息
- 问题: {question_text}
- 目标字段: {target_field}

## 要求
请基于权威来源（科技媒体、行业报告、官方文档等）回答该问题。"""
            else:
                with open(prompt_template_path, 'r', encoding='utf-8') as f:
                    template_content = f.read()
            
            # 构建Prompt模板
            self.research_prompt = PromptTemplate(
                template=template_content,
                input_variables=["ym_name", "ym_summary", "ym_category", "use_cases", 
                               "question_text", "question_type", "target_field"]
            )
        except Exception as e:
            logger.error(f"加载研究Prompt模板失败: {e}")
            raise
    
    def _ensure_language_requirement(self, query: str) -> str:
        """在查询末尾附加英文输出要求"""
        safe_query = (query or "").strip()
        if not safe_query:
            safe_query = "Research the specified Yield Machine question."
        
        if self.LANGUAGE_REQUIREMENT_TAG.lower() in safe_query.lower():
            return safe_query
        
        return f"{safe_query}\n\n{self.LANGUAGE_REQUIREMENT_BLOCK}"
    
    def _append_retry_instruction(self, query: str) -> str:
        """为重试请求附加更强的英文约束"""
        base_query = (query or "").rstrip()
        return f"{base_query}\n\n{self.ENGLISH_RETRY_BLOCK}"
    
    def _update_input_payload(self, run_id: int, repository, query: str):
        """将最终使用的查询写回 research_run"""
        try:
            repository.client.table('research_run')\
                .update({'input_payload': {'query': query}})\
                .eq('id', run_id)\
                .execute()
        except Exception as e:
            logger.warning(f"更新 run {run_id} input_payload 失败: {e}")
    
    def _execute_research_once(self, query: str, schema_wrapper: Dict[str, Any]) -> Dict[str, Any]:
        """同步执行一次 Deep Research"""
        return asyncio.run(
            self.deep_research_client.research(
                query=query,
                json_schema=schema_wrapper
            )
        )
    
    def _error_text(self, error: Exception) -> str:
        """提取错误文本"""
        parts = []
        for attr in ('message', 'body'):
            value = getattr(error, attr, None)
            if value:
                parts.append(str(value))
        if hasattr(error, 'args') and error.args:
            parts.extend(str(arg) for arg in error.args)
        if not parts:
            parts.append(str(error))
        return " | ".join(part for part in parts if part)
    
    def _parse_retry_after(self, text: str) -> Optional[float]:
        match = self.RATE_LIMIT_HINT_PATTERN.search(text)
        if match:
            try:
                return max(float(match.group(1)), 0)
            except ValueError:
                return None
        return None
    
    def _should_retry_rate_limit(self, error: Exception) -> tuple[bool, Optional[float]]:
        text = self._error_text(error)
        lowered = text.lower()
        code = getattr(error, 'code', '')
        if isinstance(code, str):
            code = code.lower()
        if 'rate limit' in lowered or 'rate_limit' in lowered or code == 'rate_limit_exceeded':
            retry_after = self._parse_retry_after(text)
            return True, retry_after
        return False, None
    
    def _run_deep_research_with_retry(self, query: str, schema_wrapper: Dict[str, Any]) -> Dict[str, Any]:
        """执行deep research，命中429时自动重试"""
        delay = self.RATE_LIMIT_INITIAL_DELAY
        for attempt in range(1, self.RATE_LIMIT_MAX_RETRIES + 1):
            try:
                return self._execute_research_once(query, schema_wrapper)
            except Exception as err:
                should_retry, retry_after = self._should_retry_rate_limit(err)
                is_last_attempt = attempt >= self.RATE_LIMIT_MAX_RETRIES
                if not should_retry or is_last_attempt:
                    raise
                
                wait_seconds = retry_after or delay
                logger.warning(
                    f"Deep Research hit OpenAI rate limit (attempt {attempt}/{self.RATE_LIMIT_MAX_RETRIES}); "
                    f"sleeping {wait_seconds:.2f}s before retry"
                )
                time.sleep(wait_seconds)
                delay *= 2
    
    def _contains_non_english(self, text: Optional[str]) -> bool:
        """检测文本中是否包含常见的非英文字符（中、日、韩）"""
        if not text:
            return False
        return bool(self.NON_ENGLISH_PATTERN.search(text))
    
    def _collect_strings(self, value: Any):
        """递归遍历结构，提取所有字符串值"""
        if isinstance(value, str):
            yield value
        elif isinstance(value, dict):
            for v in value.values():
                yield from self._collect_strings(v)
        elif isinstance(value, list):
            for item in value:
                yield from self._collect_strings(item)
    
    def _is_english_output(self, raw_answer: str, structured_answer: Optional[Dict[str, Any]]) -> bool:
        """判断原文及结构化内容是否全部为英文"""
        if self._contains_non_english(raw_answer):
            return False
        
        if structured_answer:
            for text in self._collect_strings(structured_answer):
                if self._contains_non_english(text):
                    return False
        
        return True
    
    def _format_field_definitions(self, flattened: Dict[str, Dict[str, Any]]) -> str:
        """将平铺字段定义转为JSON文本供LLM参考"""
        serialized = []
        for key, field in flattened.items():
            serialized.append({
                "key": key,
                "canonical_name": field.get("canonical_name", key),
                "description": field.get("description", ""),
                "type": field.get("type", "text"),
                "unit": field.get("unit"),
                "required": field.get("required", True)
            })
        return json.dumps(serialized, ensure_ascii=False, indent=2)
    
    def _load_registry_definitions(self, use_fields: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """从registry加载use_fields定义"""
        if not isinstance(use_fields, list) or not use_fields:
            raise ValueError("expected_fields.use_fields 不能为空")
        
        repository = get_repository(self.settings)
        if not repository:
            raise ValueError("Repository 未初始化，无法加载 registry 定义")
        
        keys = []
        required_map = {}
        for idx, field in enumerate(use_fields):
            if not isinstance(field, dict):
                raise ValueError(f"use_fields[{idx}] 必须是对象")
            key = field.get('key')
            if not key:
                raise ValueError(f"use_fields[{idx}] 缺少 key")
            keys.append(key)
            required_map[key] = bool(field.get('required', False))
        
        result = repository.client.table('metric_key_registry')\
            .select('key, canonical_name, description, value_type, unit')\
            .in_('key', keys)\
            .execute()
        
        registry_map = {row['key']: row for row in (result.data or []) if row.get('key')}
        missing = [key for key in keys if key not in registry_map]
        if missing:
            raise ValueError(f"use_fields 包含未注册字段: {missing}")
        
        flattened = {}
        for key in keys:
            row = registry_map[key]
            flattened[key] = {
                "canonical_name": row.get("canonical_name", key),
                "description": row.get("description", ""),
                "type": row.get("value_type", "text"),
                "unit": row.get("unit"),
                "required": required_map.get(key, False)
            }
        return flattened
    
    def _resolve_expected_fields(self, question: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """统一加载字段定义（树状或use_fields）"""
        expected_fields = question.get('expected_fields')
        if not expected_fields:
            raise ValueError("expected_fields 缺失，无法生成 structured 输出")
        
        if isinstance(expected_fields, dict) and "use_fields" in expected_fields:
            return self._load_registry_definitions(expected_fields["use_fields"])
        
        try:
            flattened = flatten_expected_fields(expected_fields)
        except Exception as exc:
            raise ValueError(f"expected_fields 无法展开: {exc}") from exc
        if not flattened:
            raise ValueError("expected_fields 展开失败，无法生成 structured 输出")
        return flattened
    
    def _generate_structured_output(self, report_text: str, question: Dict[str, Any]) -> Dict[str, Any]:
        """使用LLM将最终报告转换为 structured + provenance"""
        flattened = self._resolve_expected_fields(question)
        
        field_def_json = self._format_field_definitions(flattened)
        
        prompt = f"""
You are an information extraction expert. Read the research report and extract the required fields.

## Field Definitions
{field_def_json}

## Output Requirements
- Return ONLY valid JSON without code fences.
- JSON schema:
{{
  "structured": {{
    "<field_key>": <value>
  }},
  "provenance": [
    {{
      "fields": ["<field_key>", "..."],
      "evidence_text": "Verbatim English sentences from the report that justify the value."
    }}
  ]
}}
- For type="range": output {{"min": <number>, "max": <number>}} (numbers only).
- For type="number": output a number (no units or strings).
- For type="boolean": output true or false.
- For type="enum" or "text": output strings exactly as phrased in the report.
- Every field listed above must appear in `structured`. Do not invent fields.
- Each field must have at least one provenance entry referencing verbatim sentences from the report (no paraphrasing, no translation).
- evidence_text must come directly from the report; quote the minimal sentences needed.
- All required fields must be extracted. If truly unavailable, do not invent values; the run should be considered failed.
- Do not add commentary outside the JSON object.

## Research Report
{report_text}
"""
        response = self.post_structure_llm.invoke(prompt)
        raw_content = getattr(response, "content", response)
        if isinstance(raw_content, list):
            raw_content = "\n".join(
                part.get("text", "") for part in raw_content if isinstance(part, dict)
            )
        try:
            parsed = json.loads(raw_content)
        except Exception as e:
            logger.error(f"解析 structured 输出失败: {e}")
            raise ValueError("LLM 未返回有效JSON结构")
        
        structured = parsed.get("structured") or {}
        provenance = parsed.get("provenance") or []
        
        required_missing = [
            key for key, field in flattened.items()
            if field.get("required", True) and key not in structured
        ]
        if required_missing:
            raise ValueError(f"缺少必填字段结构化结果: {required_missing}")
        
        if not provenance:
            raise ValueError("生成的 provenance 为空")
        
        # 规范化 provenance 中的字段
        normalized_prov = []
        for entry in provenance:
            fields = entry.get("fields") or []
            evidence_text = entry.get("evidence_text", "").strip()
            valid_fields = [f for f in fields if f in flattened]
            if not valid_fields or not evidence_text:
                continue
            normalized_prov.append({
                "fields": valid_fields,
                "evidence_text": evidence_text
            })
        
        if not normalized_prov:
            raise ValueError("provenance 中没有有效条目")
        
        return {
            "structured": structured,
            "provenance": normalized_prov
        }
    
    def _get_schema_from_expected_fields(self, expected_fields_dsl: Dict[str, Any]) -> Dict[str, Any]:
        """从 DB DSL 生成 Perplexity 兼容的 JSON Schema"""
        fields = expected_fields_dsl.get('fields', [])
        
        # 构建 structured 部分的 properties
        structured_props = {}
        structured_required = []
        
        for field in fields:
            key = field.get('key')
            f_type = field.get('type')
            desc = field.get('description', '')
            
            json_type = "string"
            if f_type in ["numeric", "number", "float", "int"]:
                json_type = "number"
            elif f_type == "boolean":
                json_type = "boolean"
            elif f_type in ["json", "array", "object"]:
                # 对复杂类型简单处理为 object 或 array，或保持宽泛
                json_type = "object" 
            
            structured_props[key] = {
                "type": json_type,
                "description": desc
            }
            structured_required.append(key)
            
        # 构建完整 Schema (包含 structured 和 provenance)
        schema = {
            "name": "research_result",
            "schema": {
                "type": "object",
                "properties": {
                    "structured": {
                        "type": "object",
                        "properties": structured_props,
                        "required": structured_required,
                        "additionalProperties": False
                    },
                    "provenance": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "fields": {
                                    "type": "array",
                                    "items": {"type": "string"}
                                },
                            "evidence_text": {
                                    "type": "string",
                                    "description": "50-100 words, semantic dense, self-contained explanation for vector retrieval."
                                },
                                "evidence_sources": {
                                    "type": "array",
                                    "items": {"type": "string"}
                                }
                            },
                            "required": ["fields", "evidence_text", "evidence_sources"],
                            "additionalProperties": False
                        }
                    }
                },
                "required": ["structured", "provenance"],
                "additionalProperties": False
            }
        }
        return schema

    def _get_schema_for_question(self, question: Dict[str, Any]) -> Dict[str, Any]:
        """根据问题类型生成 JSON Schema"""
        
        # 1. 优先使用 expected_fields DSL
        expected_fields = question.get('expected_fields')
        if expected_fields and isinstance(expected_fields, dict) and 'fields' in expected_fields:
            return self._get_schema_from_expected_fields(expected_fields)
            
        # 2. 回退到旧逻辑 (Fallback)
        q_type = question.get('type', 'text')
        
        base_schema = {
            "name": "research_result",
            "schema": {
                "type": "object",
                "properties": {
                    "explanation": { "type": "string", "description": "对答案的详细解释和上下文" },
                    "confidence": { "type": "string", "enum": ["high", "medium", "low"] }
                },
                "required": ["explanation", "confidence"]
            }
        }
        
        properties = base_schema["schema"]["properties"]
        required = base_schema["schema"]["required"]
        
        if q_type == 'number':
            properties["value"] = { "type": "number", "description": "提取的数值" }
            properties["unit"] = { "type": "string", "description": "数值的单位" }
            required.extend(["value", "unit"])
        elif q_type == 'boolean':
            properties["value"] = { "type": "boolean", "description": "是/否结论" }
            required.append("value")
        elif q_type == 'enum':
            properties["value"] = { "type": "string", "description": "选定的枚举值" }
            required.append("value")
        else: # text or table
            properties["answer"] = { "type": "string", "description": "详细的文本答案" }
            required.append("answer")
            
        return base_schema

    def build_research_query(self, ym: Dict[str, Any], ym_summary: Dict[str, Any], question: Dict[str, Any]) -> str:
        """构建研究查询 - 优先使用 ymq.prompt_template，带兼容性检查
        
        优先级:
        1. 使用 question['prompt_template'] (如果存在且非空)
           - 检查是否包含必要占位符
           - 如果缺少占位符，自动补充产品上下文
        2. 回退到默认查询格式
        """
        try:
            # 提取基本信息（确保不是 None）
            ym_name = ym.get('name') or 'N/A'
            ym_desc = ym.get('description') or ym_summary.get("summary") or ""
            ym_category = ym.get('category') or ''
            question_text = question.get('question_text', '')
            
            # ⭐ 优先使用 ymq.prompt_template
            prompt_template = question.get('prompt_template', '').strip()
            
            if prompt_template:
                # 使用数据库中的 prompt_template
                logger.info(f"✓ 使用 ymq.prompt_template (长度: {len(prompt_template)} 字符)")
                
                # ⭐ 兼容性检查：是否包含必要的占位符
                has_ym_name = '{{YM_NAME}}' in prompt_template
                has_ym_desc = '{{YM_DESC}}' in prompt_template
                has_expected_fields = '{{expected_fields}}' in prompt_template
                
                missing_placeholders = []
                if not has_ym_name:
                    missing_placeholders.append('{{YM_NAME}}')
                if not has_ym_desc:
                    missing_placeholders.append('{{YM_DESC}}')
                if not has_expected_fields:
                    missing_placeholders.append('{{expected_fields}}')
                
                # 如果缺少占位符，自动补充产品上下文
                if missing_placeholders:
                    logger.warning(f"⚠️ prompt_template 缺少占位符: {missing_placeholders}，自动补充产品上下文")
                    
                    # 构建补充的上下文信息
                    context_prefix = "# 产品信息\n\n"
                    
                    if not has_ym_name:
                        if ym_category:
                            context_prefix += f"**产品名称**: {ym_name} ({ym_category})\n\n"
                        else:
                            context_prefix += f"**产品名称**: {ym_name}\n\n"
                    
                    if not has_ym_desc and ym_desc:
                        context_prefix += f"**产品描述**: {ym_desc}\n\n"
                    
                    context_prefix += "---\n\n# 研究任务\n\n"
                    
                    # 将上下文前置到 prompt_template
                    prompt_template = context_prefix + prompt_template
                    logger.info(f"✓ 已自动补充产品上下文，新长度: {len(prompt_template)} 字符")
                
                # 替换占位符（如果存在）- 确保替换值不是 None
                query = prompt_template
                query = query.replace('{{YM_NAME}}', ym_name)
                query = query.replace('{{YM_DESC}}', ym_desc)
                
                # 替换 expected_fields 占位符
                expected_fields = question.get('expected_fields', {})
                if expected_fields:
                    expected_fields_json = json.dumps(expected_fields, ensure_ascii=False, indent=2)
                    query = query.replace('{{expected_fields}}', expected_fields_json)
                
                logger.debug(f"使用自定义 prompt_template，最终查询长度: {len(query)} 字符")
                return self._ensure_language_requirement(query)
            
            # ⭐ 回退：使用默认查询格式
            logger.info("⚠️ ymq.prompt_template 为空，使用默认查询格式")
            
            # 构建简洁的查询
            # 格式：关于 [产品名称] ([类别])，请研究：[问题]
            if ym_category:
                query = f"关于 {ym_name} ({ym_category})，请研究：{question_text}"
            else:
                query = f"关于 {ym_name}，请研究：{question_text}"
            
            # 如果有摘要，可以添加简短的背景
            if ym_desc and len(ym_desc) < 200:
                query += f"\n\n背景信息：{ym_desc}"
            
            logger.debug(f"构建的查询（默认格式）: {query[:100]}...")
            return self._ensure_language_requirement(query)
            
        except Exception as e:
            logger.error(f"构建研究查询失败: {e}")
            import traceback
            traceback.print_exc()
            # 最终回退到最简单的查询
            return self._ensure_language_requirement(question.get('question_text', ''))
    
    def deep_research(
        self,
        ym: Dict[str, Any],
        ym_summary: Dict[str, Any],
        question: Dict[str, Any],
        forced_run_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """执行完整的深度研究流程（新版：创建research_run）"""
        run_id = forced_run_id
        
        try:
            # 0. 创建 ResearchRun 记录 (新增)
            
            repository = get_repository(self.settings)
            
            ym_db_id = ym.get('id')
            ymq_db_id = question.get('db_id') or question.get('id')
            
            if repository and ymq_db_id is None:
                question_key = question.get('question_id') or question.get('key')
                if question_key:
                    try:
                        db_lookup = repository.client.table('ymq')\
                            .select('id')\
                            .eq('key', question_key)\
                            .limit(1)\
                            .execute()
                        if db_lookup.data:
                            ymq_db_id = db_lookup.data[0]['id']
                            question['db_id'] = ymq_db_id
                            logger.info(f"🔎 从数据库加载 question_id={question_key} 的 db_id={ymq_db_id}")
                    except Exception as e:
                        logger.warning(f"根据 question_id 查询 ymq.id 失败: {e}")
            
            if run_id and repository:
                try:
                    repository.client.table('research_run')\
                        .update({
                            'status': 'running',
                            'error_message': None,
                            'is_latest': False,
                            'raw_output': {},
                            'input_payload': {},
                            'model_name': self.deep_research_client.model,
                            'updated_at': datetime.now().isoformat()
                        })\
                        .eq('id', run_id)\
                        .execute()
                    logger.info(f"🔁 复用指定 ResearchRun: run_id={run_id}, ymq_db_id={ymq_db_id}")
                except Exception as e:
                    logger.error(f"更新指定 ResearchRun 失败: {e}")
                    run_id = None
            
            if run_id is None and repository and ym_db_id and ymq_db_id:
                # ⭐ 使用 db_id (数据库ID) 而不是逻辑 id
                existing_run = None
                try:
                    existing_run = repository.client.table('research_run')\
                        .select('id')\
                        .eq('ym_id', ym_db_id)\
                        .eq('ymq_id', ymq_db_id)\
                        .order('created_at', desc=True)\
                        .limit(1)\
                        .execute()
                except Exception as e:
                    logger.warning(f"查询现有 ResearchRun 失败: {e}")
                
                existing_data = None
                if existing_run and existing_run.data:
                    existing_data = existing_run.data[0]
                
                if existing_data:
                    run_id = existing_data.get('id')
                    try:
                        repository.client.table('research_run')\
                            .update({
                                'status': 'running',
                                'error_message': None,
                                'is_latest': False,
                                'raw_output': {},
                                'input_payload': {},
                                'model_name': self.deep_research_client.model,
                                'updated_at': datetime.now().isoformat()
                            })\
                            .eq('id', run_id)\
                            .execute()
                        logger.info(f"🔁 复用 ResearchRun: run_id={run_id}, ymq_db_id={ymq_db_id}")
                    except Exception as e:
                        logger.error(f"复用现有 ResearchRun 失败: {e}")
                        run_id = None
                else:
                    run = ResearchRun(
                        ym_id=ym.get('id'),
                        ymq_id=ymq_db_id,  # ⭐ 使用数据库 ID
                        model_name=self.deep_research_client.model,
                        input_payload={},  # 稍后填充
                        raw_output={},     # 稍后填充
                        status='running',
                        is_latest=False
                    )
                    
                    try:
                        saved_run = repository.save_research_run(run)
                        run_id = saved_run.get('id')
                        logger.info(f"✅ 创建 ResearchRun: run_id={run_id}, ymq_db_id={ymq_db_id}")
                    except Exception as e:
                        logger.error(f"创建 ResearchRun 失败: {e}")
                        # 继续执行，但没有run_id
            else:
                if not question.get('db_id'):
                    logger.warning(f"Question 缺少 db_id，无法创建 ResearchRun: question_id={question.get('question_id')}")
            
            # 1. 构建研究查询
            query = self.build_research_query(ym, ym_summary, question)
            
            # 更新 input_payload
            if run_id and repository:
                self._update_input_payload(run_id, repository, query)
            
            # 2. 生成 Schema
            schema_wrapper = self._get_schema_for_question(question)
            
            # 3. 执行研究
            logger.info(f"开始研究: YM={ym.get('ym_id')}, Question={question.get('question_id')}")
            logger.info(f"Research Query:\n{query}")
            
            result = self._run_deep_research_with_retry(query, schema_wrapper)
            
            # Extract - Deep_ResearchAgent 返回的字段名称不同
            raw_answer = result.get('raw_answer_text', '')  # Deep_Research 使用 raw_answer_text
            structured_answer = result.get('structured_answer', {})
            citations = result.get('citations', [])
            
            if not self._is_english_output(raw_answer, structured_answer):
                logger.warning("检测到非英文研究结果，附加强制指令后重试一次")
                retry_query = self._append_retry_instruction(query)
                if run_id and repository:
                    self._update_input_payload(run_id, repository, retry_query)
                result = self._run_deep_research_with_retry(retry_query, schema_wrapper)
                raw_answer = result.get('raw_answer_text', '')
                structured_answer = result.get('structured_answer', {})
                citations = result.get('citations', [])
                query = retry_query
                
                if not self._is_english_output(raw_answer, structured_answer):
                    logger.error("深度研究重试后仍包含非英文内容")
                    raise ValueError("Deep Research output must be in English but is not.")
            
            structured_block = self._generate_structured_output(raw_answer, question)
            
            # Log full raw output for debugging
            logger.info(f"Research Result Raw:\n{raw_answer}")
            logger.info(f"Research Result Structured:\n{json.dumps(structured_block, ensure_ascii=False, indent=2)}")
            
            # Log Usage / Cost
            usage = result.get('usage', {})
            logger.info(f"Deep_Research Usage (Cost): {json.dumps(usage)}")
            
            
            # 4. 保存 raw_output (新增)
            if run_id and repository:
                try:
                    repository.client.table('research_run')\
                        .update({
                            'raw_output': {
                                'full_response': raw_answer,
                                'structured_answer': structured_block,  # ✅ 修复：添加structured_answer
                                'citations': citations
                            },
                            'input_payload': {'query': query}
                        })\
                        .eq('id', run_id)\
                        .execute()
                    logger.debug(f"✅ 保存 raw_output 到 run_id={run_id}")
                except Exception as e:
                    logger.error(f"保存 raw_output 失败: {e}")
            
            # 5. 返回结果 (包含run_id)
            final_result = {
                'raw_answer_text': raw_answer,
                'structured_answer': structured_block,
                'citations': citations,
                'run_id': run_id,  # ⭐ 关键：传递run_id
                'research_metadata': {
                    'model_used': self.deep_research_client.model,
                    'timestamp': datetime.now().isoformat(),
                    'usage': result.get('usage')
                }
            }
            
            return final_result
            
        except Exception as e:
            logger.error(f"深度研究失败: {e}")
            raise
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """执行研究"""
        logger.info("Running research step with incremental saving")
        
        if not context.get("preprocessed", False):
            raise ValueError("数据未完成预处理，无法进行研究")
        
        ym_list = context.get("yml_list", [])
        question_list = context.get("question_list", [])
        ym_summaries = context.get("ym_summaries", {})
        
        research_results = []
        force_run_map = context.get("force_run_id_map") or {}
        
        # 循环处理所有YM和问题的组合
        for ym in ym_list:
            ym_id = ym.get("ym_id")
            ym_summary = ym_summaries.get(ym_id)
            
            if not ym_summary:
                # 为了鲁棒性，如果没有摘要，尝试只用名称
                ym_summary = {"summary": "No summary available"}
                # logger.warning(f"YM {ym_id} 没有摘要")
            
            for question in question_list:
                question_id = question.get("question_id")
                
                identifier = question.get("question_id") or question.get("key") or str(question.get("id"))
                forced_run_id = force_run_map.get(identifier)
                
                try:
                    logger.info(f"处理组合: YM={ym_id}, Question={question_id}")
                    answer = self.deep_research(ym, ym_summary, question, forced_run_id=forced_run_id)
                    
                    result = {
                        'ym_id': ym_id,
                        'ym_db_id': ym.get('id'), # Pass DB ID for Foreign Key
                        'question_id': question_id,
                        'ymq_db_id': question.get('id'), # Pass DB ID for Foreign Key
                        'answer': answer,
                        'run_id': answer.get('run_id')  # ⭐ 提升 run_id 到顶层
                    }
                    
                    research_results.append(result)
                    logger.info(f"研究完成: YM={ym_id}, Question={question_id}")
                    
                except Exception as e:
                    logger.error(f"研究失败: YM={ym_id}, Question={question_id}: {e}")
                    continue
        
        context["research_results"] = research_results
        logger.info(f"研究步骤完成: {len(research_results)}个结果")
        return context
