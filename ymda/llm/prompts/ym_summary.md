# YM 摘要生成 Prompt

请理解以下 Yield Machine 的定义，并生成一个简洁准确的总结。

YM JSON:
{ym_json}

要求：
1. 总结这个机器是什么（核心功能）
2. 主要使用场景
3. 目标用户群体
4. 确认是否有不清楚的地方

请以JSON格式返回结果，包含以下字段：
- summary: 简洁总结（200字内）
- core_function: 核心功能描述
- use_cases: 使用场景列表
- target_users: 用户群体
- confirmation: 是否理解清晰（布尔值）
- unclear_points: 不清楚的地方列表

