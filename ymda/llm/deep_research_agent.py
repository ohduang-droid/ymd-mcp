"""Deep_Research Agent 适配器

将 LangGraph 5 阶段深度研究流程封装为与 DeepResearchClient 兼容的接口
"""

import re
import asyncio
from typing import Any, Dict, List, Optional
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from ymda.utils.logger import get_logger

logger = get_logger(__name__)

# OpenAI 价格表（USD / 1M tokens）- 2024年12月最新
OPENAI_PRICING = {
    # GPT-5 系列
    'gpt-5.2': {'input': 1.75, 'output': 14.00},
    'gpt-5.1': {'input': 1.25, 'output': 10.00},
    'gpt-5': {'input': 1.25, 'output': 10.00},
    'gpt-5-mini': {'input': 0.25, 'output': 2.00},
    'gpt-5-nano': {'input': 0.05, 'output': 0.40},
    'gpt-5.2-chat-latest': {'input': 1.75, 'output': 14.00},
    'gpt-5.1-chat-latest': {'input': 1.25, 'output': 10.00},
    'gpt-5-chat-latest': {'input': 1.25, 'output': 10.00},
    'gpt-5.1-codex-max': {'input': 1.25, 'output': 10.00},
    'gpt-5.1-codex': {'input': 1.25, 'output': 10.00},
    'gpt-5-codex': {'input': 1.25, 'output': 10.00},
    'gpt-5.2-pro': {'input': 21.00, 'output': 168.00},
    'gpt-5-pro': {'input': 15.00, 'output': 120.00},
    
    # GPT-4.1 系列
    'gpt-4.1': {'input': 2.00, 'output': 8.00},
    'gpt-4.1-mini': {'input': 0.40, 'output': 1.60},  # ✅ 当前使用
    'gpt-4.1-nano': {'input': 0.10, 'output': 0.40},
    
    # GPT-4o 系列
    'gpt-4o': {'input': 2.50, 'output': 10.00},
    'gpt-4o-2024-05-13': {'input': 5.00, 'output': 15.00},
    'gpt-4o-mini': {'input': 0.15, 'output': 0.60},
    
    # Realtime 系列
    'gpt-realtime': {'input': 4.00, 'output': 16.00},
    'gpt-realtime-mini': {'input': 0.60, 'output': 2.40},
    'gpt-4o-realtime-preview': {'input': 5.00, 'output': 20.00},
    'gpt-4o-mini-realtime-preview': {'input': 0.60, 'output': 2.40},
    
    # Audio 系列
    'gpt-audio': {'input': 2.50, 'output': 10.00},
    'gpt-audio-mini': {'input': 0.60, 'output': 2.40},
    'gpt-4o-audio-preview': {'input': 2.50, 'output': 10.00},
    'gpt-4o-mini-audio-preview': {'input': 0.15, 'output': 0.60},
    
    # o 系列（推理模型）
    'o1': {'input': 15.00, 'output': 60.00},
    'o1-pro': {'input': 150.00, 'output': 600.00},
    'o1-mini': {'input': 1.10, 'output': 4.40},
    'o3': {'input': 2.00, 'output': 8.00},
    'o3-pro': {'input': 20.00, 'output': 80.00},
    'o3-deep-research': {'input': 10.00, 'output': 40.00},
    'o3-mini': {'input': 1.10, 'output': 4.40},
    'o4-mini': {'input': 1.10, 'output': 4.40},
    'o4-mini-deep-research': {'input': 2.00, 'output': 8.00},
    
    # Codex 系列
    'gpt-5.1-codex-mini': {'input': 0.25, 'output': 2.00},
    'codex-mini-latest': {'input': 1.50, 'output': 6.00},
    
    # Search 系列
    'gpt-5-search-api': {'input': 1.25, 'output': 10.00},
    'gpt-4o-mini-search-preview': {'input': 0.15, 'output': 0.60},
    'gpt-4o-search-preview': {'input': 2.50, 'output': 10.00},
    
    # 其他
    'computer-use-preview': {'input': 3.00, 'output': 12.00},
    'gpt-image-1': {'input': 5.00, 'output': 0.00},  # 图像模型无 output
    'gpt-image-1-mini': {'input': 2.00, 'output': 0.00},
}


class Deep_ResearchAgent:
    """使用 LangGraph 5阶段流程的深度研究 Agent
    
    兼容的接口设计，与 DeepResearchClient 保持一致
    """
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4.1-mini"):
        """初始化 Agent
        
        Args:
            api_key: OpenAI API 密钥
            model: 使用的模型（默认 gpt-4.1-mini）
        """
        self.api_key = api_key
        self.model = model
        
        # 设置环境变量（LangChain 会读取）
        if api_key:
            import os
            os.environ["OPENAI_API_KEY"] = api_key
        
        # 编译 Lang Graph
        from ymda.deep_research.agent_full import deep_researcher_builder
        
        checkpointer = InMemorySaver()
        self.agent = deep_researcher_builder.compile(checkpointer=checkpointer)
        
        logger.info(f"Deep_ResearchAgent 初始化完成: model={model}")
    
    def _extract_citations_from_report(self, final_report: str) -> List[str]:
        """从报告中提取引用来源（支持中英文标题）
        
        Args:
            final_report: 完整的研究报告文本
            
        Returns:
            List of citation URLs
        """
        citations = []
        
        try:
            # ⭐ 更新正则表达式，支持中文和英文标题
            # 匹配: Sources, 参考资料, References, 引用来源, Bibliography 等
            sources_pattern = r'###+?\s*(Sources?|参考资料|References?|引用来源|Bibliography)\s*\n(.*?)(?:\n##|$)'
            match = re.search(sources_pattern, final_report, re.DOTALL | re.IGNORECASE)
            
            if match:
                # ⭐ 使用 group(2) 获取标题后的内容
                sources_section = match.group(2)
                
                # 提取所有 URL（支持 HTTP 和 HTTPS）
                url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+[^\s.,;:!?<>"{}|\\^`\[\])]'
                urls = re.findall(url_pattern, sources_section)
                
                # 去重并保持顺序
                seen = set()
                citations = []
                for url in urls:
                    if url not in seen:
                        seen.add(url)
                        citations.append(url)
                
                logger.debug(f"✓ 从报告中提取了 {len(citations)} 个引用URL")
            else:
                logger.warning(f"⚠️ 报告中未找到 Sources 部分（尝试匹配: Sources/参考资料/References）")
        except Exception as e:
            logger.error(f"✗ 提取 citations 失败: {e}")
        
        return citations
    
    def _calculate_cost(self, usage_by_model: Dict[str, Any]) -> float:
        """计算总成本（基于最新 OpenAI 价格）
        
        Args:
            usage_by_model: 按模型的 token 使用统计
            
        Returns:
            总成本（USD）
        """
        total_cost = 0.0
        
        try:
            for model_name, usage in usage_by_model.items():
                # Deep_Research 使用逻辑名称（model, creative_model等）
                # 我们需要映射到实际使用的 OpenAI 模型
                # 所有的 Deep_Research 模型都使用同一个模型（self.model），默认是 gpt-4.1-mini
                
                actual_model = self.model  # 使用初始化时指定的模型
                
                if actual_model in OPENAI_PRICING:
                    price = OPENAI_PRICING[actual_model]
                    
                    # 获取 token 数
                    if hasattr(usage, 'prompt_tokens'):
                        prompt_tokens = usage.prompt_tokens
                        completion_tokens = usage.completion_tokens
                    else:
                        prompt_tokens = usage.get('prompt_tokens', 0)
                        completion_tokens = usage.get('completion_tokens', 0)
                    
                    # 计算成本
                    input_cost = (prompt_tokens / 1_000_000) * price['input']
                    output_cost = (completion_tokens / 1_000_000) * price['output']
                    model_cost = input_cost + output_cost
                    total_cost += model_cost
                    
                    logger.debug(f"{model_name} (实际:{actual_model}): ${input_cost:.4f} (input) + ${output_cost:.4f} (output) = ${model_cost:.4f}")
                else:
                    logger.warning(f"未知模型价格: {actual_model}，无法计算成本")
        except Exception as e:
            logger.error(f"计算成本失败: {e}")
        
        return total_cost
    
    async def research(
        self, 
        query: str, 
        json_schema: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """执行深度研究
        
        Args:
            query: 研究查询（将作为 HumanMessage 传入）
            json_schema: 期望的输出结构（Deep_Research 不使用，保持接口兼容）
            **kwargs: 其他参数
                - thread_id: 可选的线程ID
                - recursion_limit: 递归限制（默认50）
                
        Returns:
            {
                'raw_answer_text': str,      # final_report 的内容
                'structured_answer': {},      # 空字典（Deep_Research 不生成结构化数据）
                'citations': List[str],       # 从报告中提取的 URL
                'usage': Dict,                # token 统计和成本
                'status': 'completed'
            }
        """
        from ymda.deep_research.token_stats import get_token_stats, reset_token_stats
        import uuid
        
        try:
            # 重置 token 统计（确保每次研究独立统计）
            reset_token_stats()
            
            # 生成唯一的 thread_id
            thread_id = kwargs.get('thread_id', str(uuid.uuid4()))
            
            # 配置
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "recursion_limit": kwargs.get("recursion_limit", 50)
                }
            }
            
            logger.info(f"开始 Deep_Research: query=[{query[:100]}...], thread_id={thread_id}")
            
            # 执行 LangGraph 流程
            result = await self.agent.ainvoke(
                {"messages": [HumanMessage(content=query)]},
                config=config
            )
            
            # 提取最终报告
            final_report = result.get("final_report", "")
            
            # 提取 notes（备用）
            notes = result.get("notes", [])
            
            # 从报告中提取 citations
            citations = self._extract_citations_from_report(final_report)
            
            # 获取 Token 统计
            stats = get_token_stats()
            total_usage = stats.get_total_usage()
            usage_by_model = stats.get_usage_by_model()
            
            # 计算成本
            total_cost = self._calculate_cost(usage_by_model)
            
            # 格式化 usage
            usage_data = {
                'prompt_tokens': total_usage.prompt_tokens,
                'completion_tokens': total_usage.completion_tokens,
                'total_tokens': total_usage.total_tokens,
                'total_cost_usd': round(total_cost, 4),
                'models': {
                    model: {
                        'prompt_tokens': usage.prompt_tokens,
                        'completion_tokens': usage.completion_tokens,
                        'total_tokens': usage.total_tokens,
                    }
                    for model, usage in usage_by_model.items()
                }
            }
            
            logger.info(f"Deep_Research 完成: tokens={total_usage.total_tokens}, cost=${total_cost:.4f}, citations={len(citations)}")
            
            return {
                'raw_answer_text': final_report,
                'structured_answer': {},  # Deep_Research 不生成结构化数据
                'citations': citations,   # 从报告中提取的 URL
                'usage': usage_data,
                'status': 'completed'
            }
            
        except Exception as e:
            logger.error(f"Deep_Research 执行失败: {e}", exc_info=True)
            raise
