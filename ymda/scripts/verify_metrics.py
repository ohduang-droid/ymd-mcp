#!/usr/bin/env python3
"""验证 metric 表数据"""

import os
import sys
import json
from pathlib import Path

# 添加项目路径
# 添加项目路径
sys.path.insert(0, str(Path(__file__).parents[2]))

from dotenv import load_dotenv
load_dotenv()

from ymda.data.db import get_database

def verify_metrics():
    """验证最新的 metric 记录"""
    db = get_database()
    client = db.get_client()
    
    # 获取最新的 research_run
    response = client.table('research_run')\
        .select('id, ym_id, ymq_id, created_at')\
        .order('created_at', desc=True)\
        .limit(1)\
        .execute()
    
    if not response.data:
        print("❌ 没有找到 research_run 记录")
        return
    
    run = response.data[0]
    print(f"✅ 最新 ResearchRun ID: {run['id']}")
    print(f"   YM ID: {run['ym_id']}, YMQ ID: {run['ymq_id']}")
    print(f"   创建时间: {run['created_at']}\n")
    
    # 获取对应的 metrics
    metrics_response = client.table('metric')\
        .select('*')\
        .eq('research_run_id', run['id'])\
        .execute()
    
    if not metrics_response.data:
        print("❌ 没有找到 metric 记录")
        return
    
    print(f"✅ 找到 {len(metrics_response.data)} 条 Metric 记录:\n")
    
    for i, metric in enumerate(metrics_response.data, 1):
        print(f"--- Metric {i} ---")
        print(f"Key: {metric['key']}")
        
        # 打印值
        if metric.get('value_numeric') is not None:
            print(f"Value (Numeric): {metric['value_numeric']}")
        elif metric.get('value_text'):
            print(f"Value (Text): {metric['value_text'][:100]}...")
        elif metric.get('value_json'):
            print(f"Value (JSON): {json.dumps(metric['value_json'], ensure_ascii=False)}")
        
        # 打印证据
        if metric.get('evidence_text'):
            print(f"Evidence Text: {metric['evidence_text'][:150]}...")
        else:
            print("⚠️  Evidence Text: MISSING")
            
        if metric.get('evidence_sources'):
            print(f"Evidence Sources: {len(metric['evidence_sources'])} URLs")
            for url in metric['evidence_sources'][:2]:
                print(f"  - {url}")
        else:
            print("⚠️  Evidence Sources: MISSING")
        
        print()

if __name__ == '__main__':
    verify_metrics()
