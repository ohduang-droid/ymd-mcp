请对以下问题进行研究。

## Yield Machine 信息
- 名称: {ym_name}
- 类别: {ym_category}
- 摘要: {ym_summary}
- 使用场景: {use_cases}

## 问题信息
- 问题: {question_text}
- 目标字段: {target_field}

## 输出要求 (Output Requirements)

**🌍 LANGUAGE REQUIREMENT (语言要求):**
**YOU MUST GENERATE THE ENTIRE REPORT IN ENGLISH.**
All analysis, evidence_text, and structured data must be written in English, regardless of the input language.

你必须基于权威来源进行回答，并将结果格式化为特定的 JSON 结构 (见 JSON Schema)。

### 特别注意：Evidence Text (证据文本)
对于 `provenance` 中的 `evidence_text` 字段，你不仅要提取原文，还必须对其进行**语义优化**，以便于后续的**向量检索 (Vector Embedding)**。

**`evidence_text` 编写准则：**
1.  **独立性 (Self-Contained)**: 这段文本必须是自解释的。即使脱离了上下文，也能清晰表达核心事实。不要使用代词（如"它"、"该值"），而要使用完整的实体名称或明确的主语。
2.  **语义密度 (Semantically Dense)**: 去除无关的废话和过渡词。每句话都应该包含高密度的信息量。
3.  **核心论点明确**: 必须直接解释**为什么**得到这个数值或结论。
4.  **字数限制**: 这里的目标不是长篇大论，也不是极其简短。请控制在 **50-100 个字 (Words) 或 80-150 个中文字符** 左右。这恰好通过一个 Embedding 切片能涵盖的最佳长度。
5.  **格式**: 纯文本，不要 markdown 格式。

**Bad Example (太简单/依赖上下文):**
> "因为它使用了最新的技术，所以成本是 $20k。来源见链接。"

**Good Example (优化后):**
> "NailBot 的总资本支出 (CAPEX) 约为 $20,000，这主要是由于其集成了高精度的计算机视觉模块和专门的机械臂组件，导致硬件成本占比较高。相比传统美甲机，其自动化程度显著提升了初始投资门槛。"

请严格遵循以上准则生成 `evidence_text`。
