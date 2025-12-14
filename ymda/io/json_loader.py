"""JSON 加载器"""

import json
from pathlib import Path
from typing import Any, Dict, List
from ymda.utils.logger import get_logger

logger = get_logger(__name__)


class JSONLoader:
    """JSON 文件加载器"""
    
    @staticmethod
    def load(file_path: str) -> Dict[str, Any]:
        """加载 JSON 文件"""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            logger.info(f"Loaded JSON file: {file_path}")
            return data
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {e}")
            raise
    
    @staticmethod
    def load_multiple(file_paths: List[str]) -> List[Dict[str, Any]]:
        """加载多个 JSON 文件"""
        results = []
        for file_path in file_paths:
            try:
                data = JSONLoader.load(file_path)
                results.append(data)
            except Exception as e:
                logger.warning(f"Failed to load {file_path}: {e}")
        return results

