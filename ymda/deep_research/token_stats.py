"""Token 统计工具模块.

该模块提供模型执行时的 token 使用统计功能，包括：
- 跟踪每次模型调用的 token 使用情况
- 累积统计信息
- 生成统计报告
"""

from typing import Dict, Optional, Any
from dataclasses import dataclass, field
from collections import defaultdict
import threading


@dataclass
class TokenUsage:
    """单次模型调用的 token 使用情况."""
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    
    def __add__(self, other: "TokenUsage") -> "TokenUsage":
        """合并两个 TokenUsage 对象."""
        return TokenUsage(
            prompt_tokens=self.prompt_tokens + other.prompt_tokens,
            completion_tokens=self.completion_tokens + other.completion_tokens,
            total_tokens=self.total_tokens + other.total_tokens
        )


class TokenStats:
    """Token 使用统计管理器.
    
    线程安全的 token 统计工具，用于跟踪和累积所有模型调用的 token 使用情况。
    """
    
    def __init__(self):
        """初始化统计管理器."""
        self._lock = threading.Lock()
        self._total_usage = TokenUsage()
        self._usage_by_model: Dict[str, TokenUsage] = defaultdict(TokenUsage)
        self._usage_by_function: Dict[str, TokenUsage] = defaultdict(TokenUsage)
        self._call_count = 0
        self._call_count_by_model: Dict[str, int] = defaultdict(int)
        self._call_count_by_function: Dict[str, int] = defaultdict(int)
    
    def record_usage(
        self,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        total_tokens: Optional[int] = None,
        model_name: Optional[str] = None,
        function_name: Optional[str] = None
    ):
        """记录一次模型调用的 token 使用情况.
        
        Args:
            prompt_tokens: 输入 token 数量
            completion_tokens: 输出 token 数量
            total_tokens: 总 token 数量（如果提供则使用，否则计算）
            model_name: 模型名称（可选）
            function_name: 函数名称（可选）
        """
        if total_tokens is None:
            total_tokens = prompt_tokens + completion_tokens
        
        usage = TokenUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens
        )
        
        with self._lock:
            self._total_usage += usage
            self._call_count += 1
            
            if model_name:
                self._usage_by_model[model_name] += usage
                self._call_count_by_model[model_name] += 1
            
            if function_name:
                self._usage_by_function[function_name] += usage
                self._call_count_by_function[function_name] += 1
    
    def extract_usage_from_response(
        self,
        response: Any,
        model_name: Optional[str] = None,
        function_name: Optional[str] = None
    ) -> TokenUsage:
        """从模型响应中提取 token 使用信息并记录.
        
        Args:
            response: 模型响应对象（通常有 response_metadata 属性）
            model_name: 模型名称（可选）
            function_name: 函数名称（可选）
            
        Returns:
            TokenUsage 对象
        """
        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0
        
        # 尝试从 response_metadata 中提取 token 信息
        if hasattr(response, 'response_metadata'):
            metadata = response.response_metadata
            if metadata:
                # OpenAI 格式
                if 'token_usage' in metadata:
                    token_usage = metadata['token_usage']
                    prompt_tokens = token_usage.get('prompt_tokens', 0)
                    completion_tokens = token_usage.get('completion_tokens', 0)
                    total_tokens = token_usage.get('total_tokens', 0)
                # Anthropic 格式
                elif 'usage' in metadata:
                    usage = metadata['usage']
                    prompt_tokens = usage.get('input_tokens', 0)
                    completion_tokens = usage.get('output_tokens', 0)
                    total_tokens = prompt_tokens + completion_tokens
        
        # 记录使用情况
        self.record_usage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens if total_tokens > 0 else None,
            model_name=model_name,
            function_name=function_name
        )
        
        return TokenUsage(
            prompt_tokens=prompt_tokens,
            completion_tokens=completion_tokens,
            total_tokens=total_tokens
        )
    
    def get_total_usage(self) -> TokenUsage:
        """获取总 token 使用情况."""
        with self._lock:
            return TokenUsage(
                prompt_tokens=self._total_usage.prompt_tokens,
                completion_tokens=self._total_usage.completion_tokens,
                total_tokens=self._total_usage.total_tokens
            )
    
    def get_usage_by_model(self) -> Dict[str, TokenUsage]:
        """按模型获取 token 使用情况."""
        with self._lock:
            return {
                model: TokenUsage(
                    prompt_tokens=usage.prompt_tokens,
                    completion_tokens=usage.completion_tokens,
                    total_tokens=usage.total_tokens
                )
                for model, usage in self._usage_by_model.items()
            }
    
    def get_usage_by_function(self) -> Dict[str, TokenUsage]:
        """按函数获取 token 使用情况."""
        with self._lock:
            return {
                func: TokenUsage(
                    prompt_tokens=usage.prompt_tokens,
                    completion_tokens=usage.completion_tokens,
                    total_tokens=usage.total_tokens
                )
                for func, usage in self._usage_by_function.items()
            }
    
    def get_call_count(self) -> int:
        """获取总调用次数."""
        with self._lock:
            return self._call_count
    
    def get_call_count_by_model(self) -> Dict[str, int]:
        """按模型获取调用次数."""
        with self._lock:
            return dict(self._call_count_by_model)
    
    def get_call_count_by_function(self) -> Dict[str, int]:
        """按函数获取调用次数."""
        with self._lock:
            return dict(self._call_count_by_function)
    
    def reset(self):
        """重置所有统计信息."""
        with self._lock:
            self._total_usage = TokenUsage()
            self._usage_by_model.clear()
            self._usage_by_function.clear()
            self._call_count = 0
            self._call_count_by_model.clear()
            self._call_count_by_function.clear()
    
    def get_summary(self) -> str:
        """生成统计摘要报告.
        
        Returns:
            格式化的统计报告字符串
        """
        with self._lock:
            lines = []
            lines.append("=" * 60)
            lines.append("Token 使用统计报告")
            lines.append("=" * 60)
            lines.append("")
            
            # 总体统计
            lines.append(f"总调用次数: {self._call_count}")
            lines.append(f"总 Token 使用:")
            lines.append(f"  输入 tokens: {self._total_usage.prompt_tokens:,}")
            lines.append(f"  输出 tokens: {self._total_usage.completion_tokens:,}")
            lines.append(f"  总计 tokens: {self._total_usage.total_tokens:,}")
            lines.append("")
            
            # 按模型统计
            if self._usage_by_model:
                lines.append("按模型统计:")
                for model, usage in sorted(self._usage_by_model.items()):
                    count = self._call_count_by_model[model]
                    lines.append(f"  {model}:")
                    lines.append(f"    调用次数: {count}")
                    lines.append(f"    输入 tokens: {usage.prompt_tokens:,}")
                    lines.append(f"    输出 tokens: {usage.completion_tokens:,}")
                    lines.append(f"    总计 tokens: {usage.total_tokens:,}")
                lines.append("")
            
            # 按函数统计
            if self._usage_by_function:
                lines.append("按函数统计:")
                for func, usage in sorted(self._usage_by_function.items()):
                    count = self._call_count_by_function[func]
                    lines.append(f"  {func}:")
                    lines.append(f"    调用次数: {count}")
                    lines.append(f"    输入 tokens: {usage.prompt_tokens:,}")
                    lines.append(f"    输出 tokens: {usage.completion_tokens:,}")
                    lines.append(f"    总计 tokens: {usage.total_tokens:,}")
                lines.append("")
            
            lines.append("=" * 60)
            return "\n".join(lines)
    
    def print_summary(self):
        """打印统计摘要报告."""
        print(self.get_summary())


# 全局统计实例
_global_stats = TokenStats()


def get_token_stats() -> TokenStats:
    """获取全局 token 统计实例."""
    return _global_stats


def reset_token_stats():
    """重置全局 token 统计."""
    _global_stats.reset()

