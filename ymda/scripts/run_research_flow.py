"""
执行研究流程脚本 (Load -> Research with Incremental Save)

此脚本用于连接 Step 2 (数据存储) 和 Step 3/4 (研究与结果存储)。
它从数据库加载数据，执行深度研究，并在每次研究完成后立即存储结果。
"""

import sys
import argparse
from typing import Dict, Any
from dotenv import load_dotenv

import os
# Ensure project root is in path (../../ relative to this script)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from ymda.settings import Settings
from ymda.pipeline.steps import LoadStep, ResearchStep
from ymda.utils.logger import get_logger

# 加载环境变量
load_dotenv()
logger = get_logger("research_flow")


def main():
    parser = argparse.ArgumentParser(description="运行深度研究流程")
    parser.add_argument("--ym-id", help="指定要研究的 YM Slug (例如: automatic-nail-art-machine)")
    parser.add_argument("--ym-db-id", type=int, help="指定要研究的 YM 数据库ID (例如: 1)")
    parser.add_argument("--question-id", help="指定要研究的问题 Key (例如: yq_pricing_model)")
    parser.add_argument("--question-db-id", type=int, help="指定要研究的问题 数据库ID (例如: 1)")
    parser.add_argument("--limit", type=int, default=1, help="限制处理的YM数量，默认1个以节省Token")
    
    args = parser.parse_args()
    
    settings = Settings()
    
    # 1. 初始上下文
    context = {}
    
    # 2. 执行步骤
    steps = [
        LoadStep(settings),
        ResearchStep(settings),
        # StoreStep 已集成到 ResearchStep 中，数据会在研究完成后立即保存
    ]
    
    try:
        # LoadStep
        logger.info(">>> 步骤 1: 加载数据 (LoadStep)")
        context = steps[0].execute(context)
        
        # 过滤数据 (如果在命令行指定了过滤条件)
        if args.ym_db_id:
            logger.info(f"过滤: 仅保留 YM DB ID = {args.ym_db_id}")
            context['yml_list'] = [ym for ym in context.get('yml_list', []) if ym.get('id') == args.ym_db_id]
        elif args.ym_id:
            logger.info(f"过滤: 仅保留 YM Slug = {args.ym_id}")
            context['yml_list'] = [ym for ym in context.get('yml_list', []) if ym.get('ym_id') == args.ym_id]
        
        # 如果没有指定 ym_id 但指定了 limit，则截取
        elif args.limit > 0:
            logger.info(f"限制: 仅处理前 {args.limit} 个 YM")
            context['yml_list'] = context.get('yml_list', [])[:args.limit]
            
        if args.question_db_id:
            logger.info(f"过滤: 仅保留 Question DB ID = {args.question_db_id}")
            context['question_list'] = [q for q in context.get('question_list', []) if q.get('id') == args.question_db_id]
        elif args.question_id:
            logger.info(f"过滤: 仅保留 Question Key = {args.question_id}")
            context['question_list'] = [q for q in context.get('question_list', []) if q.get('question_id') == args.question_id]
            
        if not context.get('yml_list'):
            logger.warning("没有可处理的 YM 数据，退出")
            return
            
        if not context.get('question_list'):
            logger.warning("没有可处理的问题数据，退出")
            return

        # ResearchStep (现在包含增量保存)
        logger.info(f">>> 步骤 2: 深度研究 + 增量保存 (ResearchStep)")
        logger.info(f"待处理: {len(context['yml_list'])} YMs × {len(context['question_list'])} Questions = {len(context['yml_list']) * len(context['question_list'])} 组合")
        context = steps[1].execute(context)
        
        # 输出统计 (数据已在 ResearchStep 中保存)
        results = context.get('research_results', [])
        stored_count = context.get('stored_count', 0)
        
        logger.info("=" * 60)
        logger.info("✅ 流程完成!")
        logger.info(f"   📊 生成结果: {len(results)} 个")
        logger.info(f"   💾 已保存到数据库: {stored_count} 个")
        if results:
            success_rate = 100 * stored_count / len(results)
            logger.info(f"   📈 保存成功率: {stored_count}/{len(results)} ({success_rate:.1f}%)")
        logger.info("=" * 60)
        
    except KeyboardInterrupt:
        logger.warning("\n⚠️ 用户中断执行 (Ctrl+C)")
        stored_count = context.get('stored_count', 0)
        if stored_count > 0:
            logger.info(f"ℹ️ 已保存 {stored_count} 条结果到数据库（中断前）")
        logger.info("提示: 已完成的研究结果已安全保存到数据库")
    except Exception as e:
        logger.error(f"流程执行失败: {e}")
        stored_count = context.get('stored_count', 0)
        if stored_count > 0:
            logger.info(f"ℹ️ 已保存 {stored_count} 条结果到数据库（失败前）")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
