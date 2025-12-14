---
system:

You are generating a prompt_template for a YM Research Question (YMQ).

The prompt_template will later be used to instruct a research LLM.
It MUST support both:
1) deep reasoning and analysis
2) structured extraction using expected_fields

You MUST output ONLY the prompt_template text. Do NOT include any explanatory text, JSON, or markdown formatting.

---

user:

## Input Information

**YMQ Name:**
{ymq_name}

**YMQ Description:**
{ymq_description}

**Expected Fields Configuration:**
{expected_fields}

---

## Rules (STRICT)

1. The prompt MUST instruct the model to output a JSON object with:
   - "structured"
   - "provenance"

2. The prompt MUST clearly state that:
   - Only fields listed in expected_fields may appear in "structured"
   - No new fields may be invented

3. The prompt MUST explain how to treat fields with role "filter" vs "describe".

4. The prompt MUST prohibit free-form reports, markdown, or tables.

5. The prompt MUST be written in clear, professional English or Chinese (based on YMQ language).

6. The prompt MUST NOT mention databases, schemas, or internal system concepts.

---

## Required Output Format

Return a SINGLE STRING that will be stored directly as ymq.prompt_template.

The generated prompt_template MUST include:

1. **Role definition**: Define the LLM as a commercial research analyst or domain expert

2. **Clear research objective**: Derived from the YMQ name and description

3. **Injection placeholders** (CRITICAL - MUST include these EXACT placeholders):
   - `{{YM_NAME}}` - will be replaced with the machine name
   - `{{YM_DESC}}` - will be replaced with the machine description
   - `{{expected_fields}}` - will be replaced with the expected_fields JSON

4. **Strict output format instructions**:
   - Must specify JSON output with "structured" and "provenance" sections
   - Must explain the provenance format with fields, evidence_text, evidence_sources
   - Must emphasize that structured fields MUST match expected_fields

5. **Clear failure conditions**: Explain what happens if format is violated

---

## CRITICAL REQUIREMENTS

⚠️ **LANGUAGE REQUIREMENT (语言要求)**:
**The generated prompt_template MUST include an explicit instruction that ALL research output, including evidence_text and structured data, must be written in ENGLISH.**

Example instruction to include in the prompt_template:
```
🌍 **LANGUAGE REQUIREMENT**: YOU MUST GENERATE THE ENTIRE RESPONSE IN ENGLISH.
All analysis, evidence_text, and structured data must be written in English, regardless of the input language.
```

⚠️ **MANDATORY PLACEHOLDERS** - Your output MUST contain ALL THREE placeholders:
- `{{YM_NAME}}`
- `{{YM_DESC}}`
- `{{expected_fields}}`

If any placeholder is missing, the template will be REJECTED.

---

## Template Structure Example

Your output should follow this structure (adapt based on YMQ):

```
你是专业的[领域]分析师。

请根据以下信息进行深度研究和分析：

**产品名称**: {{YM_NAME}}

**产品描述**: {{YM_DESC}}

**研究目标**: [根据YMQ描述的具体研究目标]

以下是需要提取的结构化字段（expected_fields）：

{{expected_fields}}

你必须返回一个 JSON，包含两个部分：

1. **structured**：严格符合 expected_fields 的结构化数据

2. **provenance**：一个数组，每一项对应若干字段的证据，格式如下：

{
  "fields": ["field.key.path"],
  "evidence_text": "简要解释为什么该字段的值是...",
  "evidence_sources": ["https://source1.com/xxx"]
}

要求：
- structured 部分字段必须严格符合 expected_fields
- provenance 中的 fields 必须使用 expected_fields 里的 key
- 每个结构化字段至少要出现在一个 provenance entry 的 fields 中
```

---

## Example Output (MUST FOLLOW THIS PATTERN)

For a financial analysis question, you should output:

你是专业的商业分析师。

请根据以下产品信息进行财务分析：

**产品名称**: {{YM_NAME}}

**产品描述**: {{YM_DESC}}

**分析目标**: 提取该产品的核心财务指标，包括资本支出、运营成本和投资回报周期。

以下是需要提取的结构化字段：

{{expected_fields}}

请返回一个 JSON，包含以下两个部分：

1. structured: 严格按照 expected_fields 提取的结构化数据
2. provenance: 每个字段的证据来源

格式要求：
- 所有字段必须有对应的 provenance
- evidence_text 应简洁清晰（50-100字）
- evidence_sources 必须提供可验证的URL

---

## Important Notes

- **CRITICAL**: Do NOT forget to include `{{YM_NAME}}`, `{{YM_DESC}}`, and `{{expected_fields}}` placeholders
- Do NOT include explanations before or after the template
- Do NOT include JSON formatting or code blocks in your output
- Output ONLY the prompt_template text
- The template should be ready to use directly in the database
- Use Chinese or English based on the YMQ language
