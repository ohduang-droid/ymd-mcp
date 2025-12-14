"""Extractor Agent - 从研究切片中抽取结构化数据

该Agent负责:
1. 读取 research_chunk 切片
2. 根据 expected_fields 抽取结构化数据
3. 为每个字段绑定证据来源 (chunk_uid + quote)
4. 返回 {structured, provenance} 格式

关键原则:
- LLM只抽取原始值 (value_raw)，不做单位换算
- 每个字段必须关联到具体的chunk作为证据
- 支持降级到raw_output的简单解析
"""

import json
import time
from typing import Dict, Any, List, Optional
from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate
from ymda.settings import Settings
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class ExtractorAgent:
    """结构化数据抽取Agent (chunk-grounded)"""
    
    def __init__(self, settings: Settings):
        """初始化Extractor Agent
        
        Args:
            settings: 全局配置
        """
        self.settings = settings
        self.llm = ChatOpenAI(
            model="gpt-4o",
            temperature=0,
            api_key=settings.openai_api_key
        )
        
        # 重试配置
        self.max_retries = 3
        self.retry_delay = 2  # seconds
        
        logger.debug("ExtractorAgent 初始化成功")
    
    def _build_extraction_prompt(
        self,
        flattened_fields: Dict[str, Dict[str, Any]],
        chunks: List[Dict[str, Any]]
    ) -> str:
        """构建抽取Prompt（新版：使用平铺schema）
        
        P0-2 修正：明确英文原样规则
        
        Args:
            flattened_fields: 平铺的字段映射 {key: field_def}
            chunks: List[{chunk_uid, content}]
            
        Returns:
            格式化的Prompt文本
        """
        # 构建字段列表，包含type信息用于格式指导
        fields_list = []
        for key, field_def in flattened_fields.items():
            field_entry = {
                "key": key,
                "canonical_name": field_def.get("canonical_name"),
                "description": field_def.get("description"),
                "type": field_def.get("type"),  # ⭐ 保留type用于格式指导
                "required": field_def.get("required", True)
            }
            # 只在有unit时添加，用于提示但不强制转换
            if field_def.get("unit"):
                field_entry["unit_hint"] = field_def.get("unit")
            fields_list.append(field_entry)
        
        schema_for_llm = {"fields": fields_list}
        fields_json = json.dumps(schema_for_llm, ensure_ascii=False, indent=2)
        
        # 格式化chunks
        chunks_text = ""
        for chunk in chunks:
            chunks_text += f"\n[{chunk['chunk_uid']}]\n{chunk['content']}\n"
        
        prompt = f"""You are a precise data extraction expert.

## Task
Extract structured data from the provided text chunks according to the schema.

## Expected Fields Schema
{fields_json}

## Text Chunks
{chunks_text}

## Critical Requirements

### 🔴 P0-1: Type-Specific Output Formats

**IMPORTANT**: The value format MUST match the field's `type`:

1. **type="range"**: Output as JSON object with min/max
   ```json
   {{"min": 15000, "max": 25000}}
   ```
   - Extract BOTH minimum and maximum values from the text
   - If only one value mentioned, use it for both min and max
   - Values should be numbers (extract from text like "$20k" → 20000)

2. **type="number"**: Output as a single number
   ```json
   12000
   ```

3. **type="text"**: Output as string (preserve original language)
   ```json
   "original text from report"
   ```

4. **type="boolean"**: Output as true/false
   ```json
   true
   ```

5. **type="enum"**: Output as string matching one of the allowed values
   ```json
   "option_value"
   ```

**unit_hint** (if provided): This is a HINT about expected units, but:
- Do NOT convert units
- Extract the numeric value in whatever unit appears in the text
- Example: If text says "¥20万" and unit_hint is "USD", extract {{"min": 200000, "max": 200000}} (the CNY value)

### 🔴 P0-2: Original Language - No Translation

**IMPORTANT**: Extracted values MUST be in the SAME LANGUAGE as they appear in the report.

Examples:
- ✅ CORRECT: If report says "Operator" (English), extract "Operator"  
- ❌ WRONG: Do NOT translate to Chinese "运营方"
- ✅ CORRECT: If report says "$20k", extract numeric value 20000 for range type
- ❌ WRONG: Do NOT keep as string "$20k" for range type
- ✅ CORRECT: For text type, extract the EXACT original expression from the text
- ❌ WRONG: Do NOT convert, translate, or rewrite in ANY language

**Rule**: For text types, copy values EXACTLY as written. For numeric/range types, extract the numeric value.

### Other Requirements

1. **Extract Original Expressions**: Extract values according to their type.
   - For range/number types: Extract numeric values (convert "20k" → 20000)
   - For text types: Keep exact original text
   - Do NOT perform unit conversion between different currencies/units
   - Example: If text says "20k USD", extract 20000 for number type
   - Example: If text says "12 months", extract "12 months" for text type
   
2. **Evidence Binding**: For each field, find the most relevant chunk_uid as evidence
   - quote should be a verbatim excerpt from the chunk that directly supports the field
   - Prefer concise quotes; length is flexible, do NOT pad quotes unnecessarily
   - If multiple chunks mention it, choose the most explicit one
   
3. **Relevance Assessment** (Optional): Set relevance (0.0-1.0) based on evidence clarity
   - 0.9-1.0: Directly and explicitly supports the metric
   - 0.7-0.8: Strongly related but requires inference
   - 0.5-0.6: Moderately related or partially supports
   
4. **Missing Fields**: If a field has no evidence in chunks:
   - Do NOT include the key in "structured"
   - Do NOT output null values
   - Simply omit the field entirely

## Output Format (Strict JSON)
{{
  "structured": {{
    "key1": {{"min": 15000, "max": 25000}},  // for range type
    "key2": 12000,  // for number type
    "key3": "original text"  // for text type
  }},
  "provenance": [
    {{
      "fields": ["key1"],
      "chunk_uid": "rr_123_chunk_0001",
      "quote": "verbatim quote from chunk...",
      "reasoning": "why this quote supports the field",
      "relevance": 0.9
    }}
  ]
}}

## Important Reminders
- Output MUST be valid JSON
- Keys in structured MUST exactly match keys in expected_fields
- ⭐ **Each key in structured MUST have corresponding entry in provenance** (mandatory)
- provenance cannot be empty (unless structured is also empty)
- **Missing fields**: Do NOT output keys with null values; omit them entirely
- Do NOT add any explanatory text outside JSON
- **P0-2: Values MUST be in original report language - NO translation, NO conversion**

## Example
If structured has 2 fields, provenance must have at least 1 entry covering those fields.
If a field has no evidence, do not include it in structured at all.
"""
        return prompt
    
    def _validate_extraction(
        self,
        extraction: Dict[str, Any],
        expected_fields: Dict[str, Any]
    ) -> bool:
        """验证抽取结果的格式
        
        P0-3 修正：支持新 provenance 格式 {fields[], chunk_uid, quote, reasoning}
        
        Args:
            extraction: LLM返回的抽取结果
            expected_fields: 期望的字段定义
            
        Returns:
            是否有效
        """
        # 检查必须字段
        if "structured" not in extraction or "provenance" not in extraction:
            logger.warning("抽取结果缺少必须字段 (structured/provenance)")
            return False
        
        # 检查类型
        if not isinstance(extraction["structured"], dict):
            logger.warning("structured 不是dict类型")
            return False
        
        if not isinstance(extraction["provenance"], list):
            logger.warning("provenance 不是list类型")
            return False
        
        # ⭐ 兼容性修改：允许 structured 和 provenance 都为空（表示没有找到数据）
        # 这是合法的情况，不应该视为格式错误
        if extraction["structured"] and not extraction["provenance"]:
            logger.warning("⚠️  provenance 为空，但 structured 有数据，这不符合要求")
            logger.warning("   每个 structured 字段都应该有对应的 provenance 条目")
            return False
        
        # 如果都为空，也是合法的（表示没有找到相关数据）
        if not extraction["structured"] and not extraction["provenance"]:
            logger.info("✓ structured 和 provenance 都为空（未找到相关数据）")
            return True # 如果都为空，则视为有效，直接返回
        
        # P0-3: 检查provenance格式（新格式）
        for idx, prov in enumerate(extraction["provenance"]):
            if not isinstance(prov, dict):
                logger.warning(f"provenance[{idx}] 不是dict")
                return False
            
            # 新格式必需字段: fields, chunk_uid, quote
            # 可选字段: reasoning, relevance
            required_keys = ["fields", "chunk_uid", "quote"]
            for key in required_keys:
                if key not in prov:
                    logger.warning(f"provenance[{idx}] 缺少 {key}")
                    return False
            
            # fields 必须是数组
            if not isinstance(prov["fields"], list):
                logger.warning(f"provenance[{idx}].fields 不是list")
                return False
            
            # quote 不能为空
            if not prov.get("quote", "").strip():
                logger.warning(f"provenance[{idx}].quote 为空")
                return False
        
        logger.debug("抽取结果验证通过")
        return True
    
    def _simple_fallback_extraction(
        self,
        raw_output: str,
        expected_fields: Dict[str, Any]
    ) -> Dict[str, Any]:
        """降级方案: 简单的正则匹配抽取
        
        当LLM抽取失败时使用
        
        Args:
            raw_output: 原始LLM响应
            expected_fields: 期望字段
            
        Returns:
            简化版抽取结果
        """
        logger.warning("使用降级抽取方案 (simple fallback)")
        
        # 非常简单的实现，实际可以更复杂
        return {
            "structured": {},
            "provenance": []
        }
    
    def extract(
        self,
        expected_fields: Dict[str, Dict[str, Any]],  # ⭐ 改为 expected_fields
        chunks: List[Dict[str, Any]],
        raw_output: Optional[str] = None
    ) -> Dict[str, Any]:
        """执行结构化数据抽取（新版：使用flattened schema）
        
        Args:
            expected_fields: 平铺的字段映射 {key: field_def}
            chunks: 文本切片 List[{chunk_uid, content}]
            raw_output: 原始LLM响应 (用于fallback)
            
        Returns:
            {
              "structured": {key: value_raw},
              "provenance": [{key, chunk_uid, quote, relevance}]
            }
        """
        if not chunks:
            logger.warning("没有chunks可用于抽取，返回空结果")
            return {"structured": {}, "provenance": []}
        
        # 构建Prompt（传递expected_fields）
        prompt_text = self._build_extraction_prompt(expected_fields, chunks)
        
        # 重试机制
        for attempt in range(self.max_retries):
            try:
                logger.info(f"执行抽取 (尝试 {attempt + 1}/{self.max_retries})")
                
                # 调用LLM (使用JSON mode)
                messages = [
                    {"role": "system", "content": "你是一个精确的数据抽取专家。只返回JSON格式的结果。"},
                    {"role": "user", "content": prompt_text}
                ]
                
                response = self.llm.invoke(
                    messages,
                    response_format={"type": "json_object"}
                )
                
                # 解析JSON
                extraction = json.loads(response.content)
                
                # ⭐ 调试：如果 structured 为空，记录完整响应
                if not extraction.get('structured'):
                    logger.warning(f"⚠️ LLM 返回了空的 structured 数据")
                    logger.warning(f"完整响应内容: {response.content[:1000]}")
                
                # 验证格式
                if self._validate_extraction(extraction, expected_fields):
                    logger.info(f"抽取成功: {len(extraction['structured'])} 个字段")
                    
                    # ⭐ 兼容性：如果 structured 为空但格式正确，也算成功（避免无限重试）
                    if not extraction.get('structured'):
                        logger.warning(f"⚠️ 抽取结果为空，可能是报告中没有相关数据")
                        logger.warning(f"期望的字段: {list(expected_fields.keys())}")
                    
                    return extraction
                else:
                    logger.warning(f"抽取结果验证失败，重试...")
                    if attempt < self.max_retries - 1:
                        time.sleep(self.retry_delay)
                        continue
                
            except json.JSONDecodeError as e:
                logger.error(f"JSON解析失败: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                
            except Exception as e:
                logger.error(f"抽取失败: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
        
        # 所有重试都失败，使用降级方案
        logger.error("所有抽取尝试都失败，使用降级方案")
        if raw_output:
            return self._simple_fallback_extraction(raw_output, expected_fields)
        else:
            return {"structured": {}, "provenance": []}
    
    def extract_with_validation(
        self,
        expected_fields: Dict[str, Any],
        chunks: List[Dict[str, Any]],
        raw_output: Optional[str] = None,
        validate_against_registry: bool = False
    ) -> Dict[str, Any]:
        """带额外验证的抽取 (可选)
        
        Args:
            expected_fields: 字段定义
            chunks: 文本切片
            raw_output: 原始响应
            validate_against_registry: 是否验证key在registry中
            
        Returns:
            抽取结果
        """
        extraction = self.extract(expected_fields, chunks, raw_output)
        
        if validate_against_registry:
            # TODO: 可以添加registry key验证
            pass
        
        return extraction
