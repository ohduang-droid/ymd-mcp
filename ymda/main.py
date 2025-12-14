"""主入口 - 运行 pipeline"""

import sys
import json
import argparse
from pathlib import Path
from ymda.pipeline.orchestrator import PipelineOrchestrator
from ymda.io.json_loader import JSONLoader
from ymda.settings import Settings
from ymda.utils.logger import get_logger

logger = get_logger(__name__)

# 步骤名称映射
STEP_NAMES = {
    "validate": "ValidateStep",
    "preprocess": "PreprocessStep",
    "research": "ResearchStep",
    "store": "StoreStep",
    "quality": "QualityStep",
}


def save_intermediate_result(result: dict, step_name: str, output_dir: Path = None):
    """保存中间结果"""
    if output_dir is None:
        output_dir = Path("step_results")
    output_dir.mkdir(exist_ok=True)
    
    # 将步骤名称转换为小写，去掉 "step" 后缀
    step_file_name = step_name.lower().replace("step", "")
    output_file = output_dir / f"{step_file_name}.json"
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False, indent=2, default=str)
    logger.info(f"步骤结果已保存到: {output_file}")


def print_step_summary(result: dict, step_name: str):
    """打印步骤摘要"""
    print("\n" + "="*60)
    print(f"步骤完成: {step_name}")
    print("="*60)
    
    if step_name == "ValidateStep":
        print(f"✓ 验证通过: {len(result.get('yml_list', []))} 个YM, {len(result.get('question_list', []))} 个问题")
    
    elif step_name == "PreprocessStep":
        summaries = result.get("ym_summaries", {})
        print(f"✓ 预处理完成: {len(summaries)} 个YM已生成摘要")
        for ym_id, summary in summaries.items():
            print(f"  - {ym_id}: {summary.get('summary', '')[:50]}...")
    
    elif step_name == "ResearchStep":
        research_results = result.get("research_results", [])
        print(f"✓ 研究完成: {len(research_results)} 个结果")
        for res in research_results[:3]:  # 只显示前3个
            print(f"  - YM={res.get('ym_id')}, Question={res.get('question_id')}")
    
    elif step_name == "StoreStep":
        stored_count = result.get("stored_count", 0)
        print(f"✓ 存储完成: {stored_count} 条记录")
    
    elif step_name == "QualityStep":
        quality_summary = result.get("quality_summary", {})
        print(f"✓ 质量检查完成")
        print(f"  - 通过: {quality_summary.get('passed_yms', 0)}/{quality_summary.get('total_yms', 0)} 个YM")
    
    print("="*60 + "\n")


def main():
    """主函数：初始化并运行 pipeline"""
    parser = argparse.ArgumentParser(description="YMDA Pipeline - Yield Machine Database Application")
    parser.add_argument("--yml", type=str, help="YML JSON文件路径")
    parser.add_argument("--ymql", type=str, help="YMQL JSON文件路径")
    parser.add_argument("--input", type=str, help="包含yml_list和question_list的JSON文件路径")
    parser.add_argument("--step", type=str, choices=["validate", "preprocess", "research", "store", "quality"], 
                       help="执行到指定步骤后停止")
    parser.add_argument("--interactive", action="store_true", help="交互模式：每步执行后询问是否继续")
    parser.add_argument("--save-intermediate", action="store_true", help="保存每步的中间结果")
    parser.add_argument("--output-dir", type=str, default="output", help="中间结果输出目录")
    
    args = parser.parse_args()
    
    settings = Settings()
    orchestrator = PipelineOrchestrator(settings)
    
    # 加载输入数据
    input_data = {}
    
    if args.input:
        # 从单个文件加载
        data = JSONLoader.load(args.input)
        input_data = {
            "yml_list": data.get("yml_list", []),
            "question_list": data.get("question_list", [])
        }
    elif args.yml and args.ymql:
        # 从两个文件加载
        yml_data = JSONLoader.load(args.yml)
        ymql_data = JSONLoader.load(args.ymql)
        
        # 处理不同的JSON格式
        if isinstance(yml_data, list):
            input_data["yml_list"] = yml_data
        elif isinstance(yml_data, dict) and "yml_list" in yml_data:
            input_data["yml_list"] = yml_data["yml_list"]
        else:
            logger.error("YML文件格式不正确，应为列表或包含yml_list的字典")
            sys.exit(1)
        
        if isinstance(ymql_data, list):
            input_data["question_list"] = ymql_data
        elif isinstance(ymql_data, dict) and "question_list" in ymql_data:
            input_data["question_list"] = ymql_data["question_list"]
        else:
            logger.error("YMQL文件格式不正确，应为列表或包含question_list的字典")
            sys.exit(1)
    else:
        logger.error("请提供输入文件：使用 --input 或 --yml 和 --ymql")
        parser.print_help()
        sys.exit(1)
    
    # 运行pipeline
    try:
        logger.info("开始运行Pipeline")
        context = {"input": input_data, "errors": []}
        
        # 保存原始输入文件路径到context，供后续步骤使用（如更新data.json状态）
        if args.input:
            context["input_file_path"] = args.input
        
        output_dir = Path(args.output_dir) if args.save_intermediate else None
        
        # 确定停止步骤
        stop_step = None
        if args.step:
            stop_step = STEP_NAMES[args.step]
        
        # 逐步执行
        for step in orchestrator.steps:
            step_name = step.__class__.__name__
            try:
                logger.info(f"运行步骤: {step_name}")
                context = step.execute(context)
                
                # 打印步骤摘要
                print_step_summary(context, step_name)
                
                # 自动保存每一步的执行结果到 step_results 目录
                step_results_dir = Path("step_results")
                save_intermediate_result(context, step_name, step_results_dir)
                
                # 如果指定了 --save-intermediate，也保存到指定目录
                if args.save_intermediate and output_dir:
                    save_intermediate_result(context, step_name.lower().replace("step", ""), output_dir)
                
                # 检查是否到达停止步骤
                if stop_step and step_name == stop_step:
                    logger.info(f"已执行到指定步骤: {step_name}")
                    break
                
                # 交互模式：询问是否继续
                if args.interactive:
                    if step_name != orchestrator.steps[-1].__class__.__name__:
                        next_step = orchestrator.steps[orchestrator.steps.index(step) + 1].__class__.__name__
                        response = input(f"是否继续执行下一步 ({next_step})? [Y/n]: ").strip().lower()
                        if response in ['n', 'no']:
                            logger.info("用户选择停止执行")
                            break
                
                if context.get("stop", False):
                    logger.warning("Pipeline stopped by step")
                    break
                    
            except Exception as e:
                logger.error(f"步骤 {step_name} 执行失败: {e}")
                # 确保 errors 数组存在
                if "errors" not in context:
                    context["errors"] = []
                context["errors"].append({
                    "step": step_name,
                    "error": str(e)
                })
                if not step.can_continue_on_error():
                    raise
        
        # 输出最终结果摘要
        logger.info("\n" + "="*60)
        logger.info("Pipeline执行完成")
        logger.info("="*60)
        logger.info(f"处理了 {len(context.get('yml_list', []))} 个YM")
        logger.info(f"处理了 {len(context.get('question_list', []))} 个问题")
        logger.info(f"生成了 {len(context.get('research_results', []))} 个研究结果")
        
        if context.get("quality_passed"):
            logger.info("质量检查通过")
        elif "quality_passed" in context:
            logger.warning("质量检查未通过，请查看质量报告")
        
        if context.get("errors"):
            logger.warning(f"执行过程中出现 {len(context['errors'])} 个错误")
            for error in context["errors"]:
                logger.warning(f"  - {error.get('step')}: {error.get('error')}")
        
        # 保存最终结果到 step_results 目录
        step_results_dir = Path("step_results")
        save_intermediate_result(context, "final", step_results_dir)
        
        # 如果指定了 --save-intermediate，也保存到指定目录
        if args.save_intermediate and output_dir:
            save_intermediate_result(context, "final", output_dir)
        
        return context
    except Exception as e:
        logger.error(f"Pipeline执行失败: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

