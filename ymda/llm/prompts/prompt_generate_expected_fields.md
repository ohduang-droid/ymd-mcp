---
system:

You are designing the expected_fields configuration for a YM Research Question (YMQ).

Your task is to select a SMALL and PRECISE set of metric keys from metric_key_registry that are appropriate for STRUCTURED EXTRACTION for this specific YMQ.

You MUST output ONLY valid JSON. Do NOT include any explanatory text, markdown formatting, or code blocks.

---

user:

## Input Information

**YMQ Name:**
{ymq_name}

**YMQ Description:**
{ymq_description}

**Available Metric Keys (from metric_key_registry):**

{metric_key_registry}

---

## Rules (STRICT)

1. You may ONLY select keys that exist in the metric_key_registry provided above.
2. You may ONLY select keys with query_capability = "strong_structured" or "filter_only".
3. Do NOT invent new keys.
4. Do NOT include text, json, note, or summary fields.
5. Keep the number of selected fields SMALL (ideally 3–8 fields).
6. Select ONLY fields that are directly relevant to answering the YMQ.
7. Prefer "strong_structured" fields over "filter_only" when possible.

---

## Output Format (CRITICAL)

You MUST output a JSON object with EXACTLY this structure. Do NOT add any other fields:

{{
  "use_fields": [
    {{
      "key": "field_name_from_registry",
      "role": ["filter"],
      "required": true
    }}
  ]
}}

The top-level key MUST be "use_fields" (not "expected_fields" or anything else).
Each item in the array MUST have: key, role (array), and required (boolean).

---

## Additional Guidance

- Use "required": true ONLY for fields that are absolutely essential to answering the YMQ.
- Use "required": false for optional but helpful fields.
- The "role" field should be ["filter"] for most fields.
- Do NOT include any explanatory text before or after the JSON.
- Do NOT wrap the JSON in markdown code blocks (no ```).
- Output ONLY the raw JSON object starting with {{ and ending with }}.

---

## Example

For a question about "该产品的财务基础信息是什么？包括CAPEX、OPEX和投资回报周期", output:

{{
  "use_fields": [
    {{
      "key": "financial.capex.total",
      "role": ["filter"],
      "required": true
    }},
    {{
      "key": "financial.opex_monthly.total",
      "role": ["filter"],
      "required": true
    }},
    {{
      "key": "financial.payback_months.base",
      "role": ["filter"],
      "required": false
    }}
  ]
}}
