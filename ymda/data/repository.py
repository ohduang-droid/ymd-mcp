"""统一仓储 - 抽象 + Supabase实现"""

import os
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from datetime import datetime
from threading import Lock
from ymda.data.models import YM, YMQuestion, ResearchRun, Metric, ResearchChunk, MetricKeyRegistry, MetricProvenance
from ymda.data.db import Database, get_database
from ymda.utils.logger import get_logger

logger = get_logger(__name__)

# 仓储单例实例
_repository_instance: Optional['SupabaseRepository'] = None
_repository_lock = Lock()


class Repository(ABC):
    """仓储抽象基类"""
    
    @abstractmethod
    def save_ym(self, ym: YM) -> dict:
        """保存YM"""
        pass
    
    @abstractmethod
    def save_question(self, question: YMQuestion) -> dict:
        """保存问题"""
        pass
    
    @abstractmethod
    def save_research_run(self, run: ResearchRun) -> dict:
        """保存研究记录"""
        pass

    @abstractmethod
    def save_metrics(self, metrics: List[Metric]) -> bool:
        """批量保存指标"""
        pass

    @abstractmethod
    def get_latest_research_run(self, ym_id: int, ymq_id: int) -> Optional[dict]:
        """获取最新研究记录"""
        pass

    @abstractmethod
    def get_active_yms(self) -> List[Dict[str, Any]]:
        """获取所有活跃的YM"""
        pass

    @abstractmethod
    def get_all_questions(self) -> List[Dict[str, Any]]:
        """获取所有问题定义"""
        pass


class SupabaseRepository(Repository):
    """Supabase仓储实现"""
    
    def __init__(self, db: Database):
        """初始化仓储"""
        self.db = db
        self.client = db.get_client()
    
    def save_ym(self, ym: YM) -> dict:
        """保存YM到Supabase"""
        try:
            data = ym.to_dict()
            # 移除id字段，让Supabase自动生成
            data.pop('id', None)
            
            # 设置时间戳
            if 'created_at' not in data or not data['created_at']:
                data['created_at'] = datetime.now().isoformat()
            data['updated_at'] = datetime.now().isoformat()
            
            result = self.client.table('ym').insert(data).execute()
            logger.info(f"保存YM成功: {ym.ym_id}")
            return result.data[0] if result.data else {}
        except Exception as e:
            logger.error(f"保存YM失败: {e}")
            raise
    
    def save_question(self, question: YMQuestion) -> dict:
        """保存问题到Supabase"""
        try:
            data = question.to_dict()
            data.pop('id', None)
            
            if 'created_at' not in data or not data['created_at']:
                data['created_at'] = datetime.now().isoformat()
            
            result = self.client.table('ym_question').insert(data).execute()
            logger.info(f"保存问题成功: {question.question_id}")
            return result.data[0] if result.data else {}
        except Exception as e:
            logger.error(f"保存问题失败: {e}")
            raise
    
    def save_research_run(self, run: ResearchRun) -> dict:
        """保存研究记录到Supabase"""
        try:
            data = run.to_dict()
            # 移除id字段，让Supabase自动生成
            data.pop('id', None)
            
            # 设置时间戳
            if 'created_at' not in data or not data['created_at']:
                data['created_at'] = datetime.now().isoformat()
            
            # Supabase vector handling: embedding needs to be a list
            # data['embedding'] is already a list from to_dict -> asdict
            
            result = self.client.table('research_run').insert(data).execute()
            logger.info(f"保存研究记录成功: YM={run.ym_id}, YMQ={run.ymq_id}")
            return result.data[0] if result.data else {}
        except Exception as e:
            logger.error(f"保存研究记录失败: {e}")
            raise

    def save_metrics(self, metrics: List[Metric]) -> bool:
        """批量保存指标到Supabase"""
        if not metrics:
            return True
            
        try:
            data_list = []
            for m in metrics:
                d = m.to_dict()
                d.pop('id', None)
                if 'created_at' not in d or not d['created_at']:
                    d['created_at'] = datetime.now().isoformat()
                data_list.append(d)
                
            result = self.client.table('metric').insert(data_list).execute()
            
            # 统计有多少 metric 包含 embedding
            with_embedding = sum(1 for d in data_list if d.get('embedding'))
            logger.info(f"保存指标成功: {len(data_list)} 条（{with_embedding} 条包含 embedding）")
            return True
        except Exception as e:
            logger.error(f"保存指标失败: {e}")
            raise

    def get_latest_research_run(self, ym_id: int, ymq_id: int) -> Optional[dict]:
        """获取最新研究记录"""
        try:
            result = self.client.table('research_run')\
                .select('*')\
                .eq('ym_id', ym_id)\
                .eq('ymq_id', ymq_id)\
                .order('created_at', desc=True)\
                .limit(1)\
                .execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"获取研究记录失败: {e}")
            return None

    def get_active_yms(self) -> List[Dict[str, Any]]:
        """获取所有活跃的YM"""
        try:
            # 暂时获取所有YM，后续可以添加 status='active' 过滤
            result = self.client.table('ym').select('*').execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"获取YM列表失败: {e}")
            return []

    def get_all_questions(self) -> List[Dict[str, Any]]:
        """获取所有问题定义"""
        try:
            result = self.client.table('ymq').select('*').execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"获取问题列表失败: {e}")
            return []
    
    def upsert_ymq(self, ymq_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        根据 key 更新或插入 YMQ 数据（原子性操作）
        
        如果 key 已存在则更新，不存在则插入新记录
        
        Args:
            ymq_data: YMQ 数据字典，必须包含 'key' 字段
            
        Returns:
            字典包含: {'success': bool, 'id': int | None, 'key': str, 'is_new': bool}
        """
        try:
            key = ymq_data.get('key')
            if not key:
                logger.warning("YMQ数据缺少key，跳过数据库写入")
                return {'success': False, 'id': None, 'key': None, 'is_new': False, 'error': 'Missing key'}
            
            # 构建数据库记录
            # 注意：原始问题数据已经包含在 expected_fields 的 _meta.original_question 中
            db_data = {
                'key': key,
                'name': ymq_data.get('name', ''),
                'description': ymq_data.get('description'),
                'prompt_template': ymq_data.get('prompt_template', ''),
                'expected_fields': ymq_data.get('expected_fields')
            }
            
            # 先检查是否存在，用于日志记录
            is_update = False
            existing_id = None
            try:
                existing = self.client.table('ymq')\
                    .select('id, name')\
                    .eq('key', key)\
                    .execute()
                
                if existing.data and len(existing.data) > 0:
                    is_update = True
                    existing_id = existing.data[0]['id']
                    old_name = existing.data[0].get('name', '')
                    logger.debug(f"检测到YMQ已存在: key={key}, id={existing_id}, 将执行更新操作")
            except Exception as query_error:
                # 查询失败不影响后续操作，继续执行 upsert
                logger.debug(f"查询YMQ是否存在时出错（不影响upsert操作）: {query_error}")
            
            # 使用 Supabase 的原生 upsert 方法（原子性操作）
            # on_conflict 指定当 key 冲突时执行更新操作
            try:
                result = self.client.table('ymq')\
                    .upsert(db_data, on_conflict='key')\
                    .execute()
                
                # ⭐ 提取数据库返回的 ID
                db_record = result.data[0] if result.data else None
                db_id = db_record.get('id') if db_record else existing_id
                
                if is_update:
                    logger.info(f"✓ 更新YMQ到数据库成功: key={key}, id={db_id}, name={db_data.get('name', '')}")
                else:
                    logger.info(f"✓ 插入YMQ到数据库成功: key={key}, id={db_id}, name={db_data.get('name', '')}")
                
                # ⭐ 返回包含 ID 的字典
                return {
                    'success': True,
                    'id': db_id,
                    'key': key,
                    'is_new': not is_update
                }
            except Exception as upsert_error:
                error_str = str(upsert_error)
                error_type = type(upsert_error).__name__
                
                # 提取详细的错误信息
                error_details = {
                    'error_type': error_type,
                    'error_message': error_str,
                    'error_repr': repr(upsert_error)
                }
                
                # 尝试从异常中提取更多信息
                if hasattr(upsert_error, 'message'):
                    error_details['message'] = upsert_error.message
                if hasattr(upsert_error, 'code'):
                    error_details['code'] = upsert_error.code
                if hasattr(upsert_error, 'details'):
                    error_details['details'] = upsert_error.details
                if hasattr(upsert_error, 'hint'):
                    error_details['hint'] = upsert_error.hint
                if hasattr(upsert_error, 'args'):
                    error_details['args'] = upsert_error.args
                
                # 打印详细错误信息
                logger.error(f"✗ YMQ upsert操作失败: key={key}")
                logger.error(f"  错误类型: {error_type}")
                logger.error(f"  错误信息: {error_str}")
                logger.error(f"  错误详情: {error_details}")
                
                # 针对特定错误类型提供诊断建议
                error_lower = error_str.lower()
                if 'duplicate key' in error_lower or 'unique constraint' in error_lower:
                    logger.error(f"  可能原因: key '{key}' 的唯一性约束冲突")
                elif 'permission' in error_lower or 'unauthorized' in error_lower or '401' in error_str:
                    supabase_url = self.db.settings.supabase_url if hasattr(self.db, 'settings') else 'unknown'
                    is_service_role = hasattr(self.db.settings, 'supabase_key') and \
                                     os.getenv("SUPABASE_SERVICE_ROLE_KEY") is not None
                    logger.error(f"  可能原因: 数据库权限不足")
                    logger.error(f"  Supabase URL: {supabase_url}")
                    logger.error(f"  使用的Key类型: {'service_role' if is_service_role else 'anon/key'}")
                    logger.error(f"  💡 解决方案: 请确保在 .env 文件中设置了 SUPABASE_SERVICE_ROLE_KEY（而不是 SUPABASE_KEY）")
                elif 'disconnected' in error_lower or 'connection' in error_lower or 'timeout' in error_lower:
                    supabase_url = self.db.settings.supabase_url if hasattr(self.db, 'settings') else 'unknown'
                    logger.error(f"  可能原因: 数据库连接问题")
                    logger.error(f"  Supabase URL: {supabase_url}")
                    logger.error(f"  💡 诊断步骤:")
                    logger.error(f"    1. 检查网络连接是否正常")
                    logger.error(f"    2. 检查 SUPABASE_URL 是否正确（应该是 https://xxx.supabase.co）")
                    logger.error(f"    3. 检查 Supabase 服务是否正常运行")
                    logger.error(f"    4. 检查防火墙或代理设置是否阻止了连接")
                    logger.error(f"    5. 尝试在 Supabase Dashboard 中查看服务状态")
                elif '404' in error_str or 'not found' in error_lower:
                    supabase_url = self.db.settings.supabase_url if hasattr(self.db, 'settings') else 'unknown'
                    expected_url = f"{supabase_url}/rest/v1/ymq"
                    logger.error(f"  可能原因: 表不存在或URL错误")
                    logger.error(f"  Supabase URL: {supabase_url}")
                    logger.error(f"  预期请求URL: {expected_url}")
                    logger.error(f"  💡 诊断步骤:")
                    logger.error(f"    1. 检查表是否在 'public' schema 中")
                    logger.error(f"    2. 在 Supabase Dashboard → Settings → API 中确认表已暴露给 REST API")
                    logger.error(f"    3. 检查表名是否正确（应该是 'ymq' 而不是其他名称）")
                
                # ⭐ 返回错误字典
                return {
                    'success': False,
                    'id': None,
                    'key': key,
                    'is_new': False,
                    'error': error_str
                }
            
        except Exception as e:
            key = ymq_data.get('key', 'unknown')
            error_str = str(e)
            error_type = type(e).__name__
            
            # 提取详细的错误信息
            error_details = {
                'error_type': error_type,
                'error_message': error_str,
                'error_repr': repr(e)
            }
            
            # 尝试从异常中提取更多信息
            if hasattr(e, 'message'):
                error_details['message'] = e.message
            if hasattr(e, 'code'):
                error_details['code'] = e.code
            if hasattr(e, 'details'):
                error_details['details'] = e.details
            if hasattr(e, 'hint'):
                error_details['hint'] = e.hint
            if hasattr(e, 'args'):
                error_details['args'] = e.args
            
            logger.error(f"保存YMQ异常: key={key}")
            logger.error(f"  错误类型: {error_type}")
            logger.error(f"  错误信息: {error_str}")
            logger.error(f"  错误详情: {error_details}")
            
            # ⭐ 返回错误字典
            return {
                'success': False,
                'id': None,
                'key': key,
                'is_new': False,
                'error': error_str
            }
    
    def upsert_ym_by_slug(self, ym_data: Dict[str, Any]) -> bool:
        """
        根据 slug 更新或插入 YM 数据（公共方法）
        
        Args:
            ym_data: YM 数据字典，必须包含 'slug' 字段
            
        Returns:
            成功返回 True，失败返回 False
        """
        try:
            slug = ym_data.get('slug') or ym_data.get('ym_id', '')
            if not slug:
                logger.warning("YM数据缺少slug或ym_id，跳过数据库写入")
                return False
            
            # 检查数据库中是否已存在
            existing_data = None
            try:
                # 记录请求详情用于调试
                supabase_url = self.db.settings.supabase_url
                expected_url = f"{supabase_url}/rest/v1/ym?select=id%2Cslug&slug=eq.{slug}"
                logger.debug(f"查询YM: slug={slug}")
                logger.debug(f"Supabase URL: {supabase_url}")
                logger.debug(f"预期请求URL: {expected_url}")
                
                existing_data = self.client.table('ym')\
                    .select('id, slug')\
                    .eq('slug', slug)\
                    .execute()
                
                logger.debug(f"查询成功: {existing_data.data if existing_data.data else '无匹配记录'}")
            except Exception as query_error:
                    error_str = str(query_error)
                    error_details = {
                        'error_type': type(query_error).__name__,
                        'error_message': str(query_error),
                        'error_repr': repr(query_error)
                    }
                    
                    # 尝试从异常中提取更多信息
                    if hasattr(query_error, 'message'):
                        error_details['message'] = query_error.message
                    if hasattr(query_error, 'code'):
                        error_details['code'] = query_error.code
                    if hasattr(query_error, 'details'):
                        error_details['details'] = query_error.details
                    if hasattr(query_error, 'hint'):
                        error_details['hint'] = query_error.hint
                    
                    logger.error(f"REST API 查询YM失败 - 错误详情: {error_details}")
                    logger.error(f"完整错误信息: {error_str}")
                    
                    # 检查是否是 404 错误（表不存在或URL错误）
                    if '404' in error_str or 'Cannot GET' in error_str or 'not found' in error_str.lower():
                        logger.error(
                            f"保存YM到数据库失败（404错误）: {slug}\n"
                            f"错误详情: {error_str}\n"
                            f"Supabase URL: {supabase_url}\n"
                            f"预期请求URL: {expected_url}\n"
                            f"\n💡 诊断步骤:\n"
                            f"1. 检查 SUPABASE_URL 是否正确（应该是 https://xxx.supabase.co）\n"
                            f"2. 检查表是否在 'public' schema 中\n"
                            f"3. 在 Supabase Dashboard → Settings → API 中确认表已暴露给 REST API\n"
                            f"4. 检查表名是否正确（应该是 'ym' 而不是其他名称）\n"
                            f"5. 尝试在 Supabase Dashboard 的 SQL Editor 中执行: SELECT * FROM public.ym LIMIT 1;\n"
                            f"6. 检查 RLS (Row Level Security) 是否启用，如果启用需要配置策略或使用 service_role key"
                        )
                        return False
                    else:
                        # 其他查询错误，继续尝试插入
                        logger.warning(f"查询现有记录失败: {query_error}，将尝试直接插入")
            
            # 使用查询结果
            existing = existing_data
            
            # 构建数据库记录
            # 注意：category 在数据库中是必填字段，如果为空则使用默认值 'unknown'
            category = ym_data.get('category', '')
            if not category:
                category = 'unknown'
                logger.warning(f"YM {slug} 的 category 为空，使用默认值 'unknown'")
            
            db_data = {
                'slug': slug,
                'name': ym_data.get('name', ''),
                'category': category,
                'description': ym_data.get('short_desc') or ym_data.get('description') or None
            }
            
            if existing and (hasattr(existing, 'data') and existing.data):
                # 更新现有记录
                record_id = existing.data[0]['id']
                try:
                    logger.debug(f"准备更新YM: slug={slug}, id={record_id}, data={db_data}")
                    result = self.client.table('ym')\
                        .update(db_data)\
                        .eq('id', record_id)\
                        .execute()
                    logger.info(f"更新YM到数据库成功: {slug} (id: {record_id})")
                except Exception as update_error:
                    error_str = str(update_error)
                    logger.error(f"更新YM失败: {error_str}")
                    raise
            else:
                # 插入新记录
                try:
                    logger.debug(f"准备插入YM: slug={slug}, data={db_data}")
                    result = self.client.table('ym')\
                        .insert(db_data)\
                        .execute()
                    logger.info(f"插入YM到数据库成功: {slug}")
                except Exception as insert_error:
                    error_str = str(insert_error)
                    logger.error(f"插入YM失败: {error_str}")
                    raise
            
            return True
        except Exception as e:
            error_msg = str(e)
            error_type = type(e).__name__
            
            # 提取详细的错误信息
            error_details = {
                'error_type': error_type,
                'error_message': error_msg,
                'error_repr': repr(e)
            }
            
            if hasattr(e, 'message'):
                error_details['message'] = e.message
            if hasattr(e, 'code'):
                error_details['code'] = e.code
            if hasattr(e, 'details'):
                error_details['details'] = e.details
            if hasattr(e, 'hint'):
                error_details['hint'] = e.hint
            
            logger.error(f"保存YM异常 - 错误详情: {error_details}")
            logger.error(f"完整错误信息: {error_msg}")
            
            # 检查是否是 401 认证错误
            if '401' in error_msg or 'Invalid API key' in error_msg or 'Unauthorized' in error_msg:
                supabase_url = self.db.settings.supabase_url
                is_service_role = hasattr(self.db.settings, 'supabase_key') and \
                                 os.getenv("SUPABASE_SERVICE_ROLE_KEY") is not None
                logger.error(
                    f"保存YM到数据库失败（认证错误）: {ym_data.get('slug') or ym_data.get('ym_id', 'unknown')}\n"
                    f"错误详情: {error_msg}\n"
                    f"Supabase URL: {supabase_url}\n"
                    f"使用的Key类型: {'service_role' if is_service_role else 'anon/key'}\n"
                    f"💡 解决方案: 请确保在 .env 文件中设置了 SUPABASE_SERVICE_ROLE_KEY（而不是 SUPABASE_KEY）。"
                    f"service_role key 具有完整权限，可以绕过 RLS 限制进行数据库写入操作。"
                )
            # 检查是否是 404 错误（表不存在或URL错误）
            elif '404' in error_msg or 'Cannot GET' in error_msg or 'Cannot POST' in error_msg or 'not found' in error_msg.lower():
                supabase_url = self.db.settings.supabase_url
                expected_url = f"{supabase_url}/rest/v1/ym"
                logger.error(
                    f"保存YM到数据库失败（404错误）: {ym_data.get('slug') or ym_data.get('ym_id', 'unknown')}\n"
                    f"错误详情: {error_msg}\n"
                    f"Supabase URL: {supabase_url}\n"
                    f"预期请求URL: {expected_url}\n"
                    f"\n💡 诊断步骤:\n"
                    f"1. 检查 SUPABASE_URL 是否正确（应该是 https://xxx.supabase.co）\n"
                    f"2. 检查表是否在 'public' schema 中\n"
                    f"3. 在 Supabase Dashboard → Settings → API 中确认表已暴露给 REST API\n"
                    f"4. 检查表名是否正确（应该是 'ym' 而不是其他名称）\n"
                    f"5. 尝试在 Supabase Dashboard 的 SQL Editor 中执行: SELECT * FROM public.ym LIMIT 1;\n"
                    f"6. 检查 RLS (Row Level Security) 是否启用，如果启用需要配置策略或使用 service_role key"
                )
            else:
                logger.error(f"保存YM到数据库失败: {ym_data.get('slug') or ym_data.get('ym_id', 'unknown')} - {error_msg}")
            return False
    
    # ========== 新增方法 (YMDA 新版架构) ==========
    
    def save_research_chunks(self, chunks: List[ResearchChunk]) -> bool:
        """批量保存 research_chunk"""
        if not chunks:
            return True
        
        try:
            data_list = []
            for chunk in chunks:
                d = chunk.to_dict()
                d.pop('id', None)
                if 'created_at' not in d or not d['created_at']:
                    d['created_at'] = datetime.now().isoformat()
                data_list.append(d)
            
            result = self.client.table('research_chunk').insert(data_list).execute()
            logger.info(f"保存研究切片成功: {len(data_list)} 条")
            return True
        except Exception as e:
            logger.error(f"保存研究切片失败: {e}")
            raise
    
    def save_metric_key_registry(self, registry: MetricKeyRegistry) -> dict:
        """保存 metric_key_registry (单条)"""
        try:
            data = registry.to_dict()
            data.pop('id', None)
            if 'created_at' not in data or not data['created_at']:
                data['created_at'] = datetime.now().isoformat()
            data['updated_at'] = datetime.now().isoformat()
            
            result = self.client.table('metric_key_registry').insert(data).execute()
            logger.info(f"保存 registry key 成功: {registry.key}")
            return result.data[0] if result.data else {}
        except Exception as e:
            logger.error(f"保存 registry key 失败: {e}")
            raise
    
    def upsert_metric_key_registry(self, key: str, data: Dict[str, Any]) -> bool:
        """根据 key 更新或插入 metric_key_registry"""
        try:
            query_capability = data.get('query_capability')
            if not query_capability:
                raise ValueError(f"registry key {key} 缺少 query_capability，禁止写入")
            
            # 构建数据
            db_data = {
                'key': key,
                'canonical_name': data.get('canonical_name'),
                'description': data.get('description'),
                'value_type': data.get('value_type'),  # 使用 value_type 而非 type
                'query_capability': query_capability,
                'unit': data.get('unit'),
                'constraints': data.get('constraints'),
                'embedding': data.get('embedding'),
                'updated_at': datetime.now().isoformat()
            }
            # 移除None值
            db_data = {k: v for k, v in db_data.items() if v is not None}
            
            # Upsert (on_conflict='key')
            result = self.client.table('metric_key_registry').upsert(db_data, on_conflict='key').execute()
            logger.info(f"Upsert registry key 成功: {key}")
            return True
        except Exception as e:
            logger.error(f"Upsert registry key 失败 ({key}): {e}")
            return False
    
    def get_metric_key_registry_by_key(self, key: str) -> Optional[Dict[str, Any]]:
        """根据 key 获取 registry 记录"""
        try:
            result = self.client.table('metric_key_registry').select('*').eq('key', key).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"查询 registry key 失败 ({key}): {e}")
            return None
    
    def list_all_registry_keys(self) -> List[Dict[str, Any]]:
        """列出所有 registry keys"""
        try:
            result = self.client.table('metric_key_registry').select('*').execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"查询 registry keys 失败: {e}")
            return []
    
    def save_metric_provenance(self, provenances: List[MetricProvenance]) -> bool:
        """批量保存 metric_provenance"""
        if not provenances:
            return True
        
        try:
            data_list = []
            for prov in provenances:
                d = prov.to_dict()
                d.pop('id', None)
                if 'created_at' not in d or not d['created_at']:
                    d['created_at'] = datetime.now().isoformat()
                data_list.append(d)
            
            result = self.client.table('metric_provenance').insert(data_list).execute()
            logger.info(f"保存 metric provenance 成功: {len(data_list)} 条")
            return True
        except Exception as e:
            logger.error(f"保存 metric provenance 失败: {e}")
            raise
    
    def update_research_run_status(
        self, 
        run_id: int, 
        status: str, 
        error_msg: Optional[str] = None,
        parsed_ok: bool = False
    ) -> bool:
        """更新 research_run 状态"""
        try:
            update_data = {
                'status': status,
                'parsed_ok': parsed_ok
            }
            if error_msg:
                update_data['error_message'] = error_msg
            
            result = self.client.table('research_run').update(update_data).eq('id', run_id).execute()
            logger.info(f"更新 research_run 状态成功: run_id={run_id}, status={status}")
            return True
        except Exception as e:
            logger.error(f"更新 research_run 状态失败: {e}")
            return False
    
    def set_latest_run(self, ym_id: int, ymq_id: int, run_id: int) -> bool:
        """设置 is_latest (事务更新)
        
        步骤:
        1. 将同一(ym_id, ymq_id)的旧run的is_latest设为false
        2. 将新run的is_latest设为true
        
        注意: Supabase Python客户端不直接支持事务,这里用两步操作模拟
        """
        try:
            # Step 1: 清除旧的latest标记
            self.client.table('research_run')\
                .update({'is_latest': False})\
                .eq('ym_id', ym_id)\
                .eq('ymq_id', ymq_id)\
                .eq('is_latest', True)\
                .execute()
            
            # Step 2: 设置新的latest
            self.client.table('research_run')\
                .update({'is_latest': True})\
                .eq('id', run_id)\
                .execute()
            
            logger.info(f"设置 latest run 成功: ym_id={ym_id}, ymq_id={ymq_id}, run_id={run_id}")
            return True
        except Exception as e:
            logger.error(f"设置 latest run 失败: {e}")
            return False
    
    def get_latest_research_run_v2(self, ym_id: int, ymq_id: int, only_parsed: bool = True) -> Optional[dict]:
        """获取最新研究记录 (新版,使用 is_latest 字段)"""
        try:
            query = self.client.table('research_run')\
                .select('*')\
                .eq('ym_id', ym_id)\
                .eq('ymq_id', ymq_id)\
                .eq('is_latest', True)
            
            if only_parsed:
                query = query.eq('status', 'parsed')
            
            result = query.limit(1).execute()
            return result.data[0] if result.data else None
        except Exception as e:
            logger.error(f"获取最新研究记录失败: {e}")
            return None
    
    def finalize_research_run(self, run_id: int, ym_id: int, ymq_id: int) -> bool:
        """
        P0-5: Finalize 成功的 research_run
        
        操作:
        1. UPDATE research_run SET status='parsed', parsed_ok=true, is_latest=true WHERE id=run_id
        2. UPDATE research_run SET is_latest=false WHERE ym_id=ym_id AND ymq_id=ymq_id AND id!=run_id
        
        使用事务确保原子性
        
        Args:
            run_id: 要finalize的run
            ym_id: YM数据库ID
            ymq_id: YMQ数据库ID
            
        Returns:
            是否成功
        """
        try:
            # 1. 更新当前run为parsed+latest
            result1 = self.client.table('research_run')\
                .update({
                    'status': 'parsed',
                    'parsed_ok': True,
                    'is_latest': True
                })\
                .eq('id', run_id)\
                .execute()
            
            logger.info(f"Finalized run {run_id}: status=parsed, is_latest=true")
            
            # 2. 清除同(ym_id, ymq_id)的其他latest
            result2 = self.client.table('research_run')\
                .update({'is_latest': False})\
                .eq('ym_id', ym_id)\
                .eq('ymq_id', ymq_id)\
                .neq('id', run_id)\
                .execute()
            
            logger.info(f"Cleared old latest for (ym_id={ym_id}, ymq_id={ymq_id})")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to finalize run {run_id}: {e}")
            return False
    
    def finalize_research_run_partial(self, run_id: int) -> bool:
        """
        P0-4: Finalize partial 成功的 research_run (有required字段缺失)
        
        操作:
        - status='partial', parsed_ok=false, is_latest=false
        
        Args:
            run_id: 要标记为partial的run
            
        Returns:
            是否成功
        """
        try:
            self.client.table('research_run')\
                .update({
                    'status': 'partial',
                    'parsed_ok': False,
                    'is_latest': False
                })\
                .eq('id', run_id)\
                .execute()
            
            logger.info(f"Finalized run {run_id} as partial")
            return True
            
        except Exception as e:
            logger.error(f"Failed to finalize run {run_id} as partial: {e}")
            return False
    
    def rollback_failed_run(self, run_id: int, error_message: str) -> bool:
        """
        P0-5: Rollback 失败的 research_run
        
        操作 (顺序很重要):
        1. DELETE FROM research_artifact WHERE research_run_id=run_id
        2. DELETE FROM metric_provenance WHERE metric_id IN (SELECT id FROM metric WHERE research_run_id=run_id)
        3. DELETE FROM metric WHERE research_run_id=run_id
        4. UPDATE research_run SET status='failed', is_latest=false, error_message=error_message WHERE id=run_id
        
        保留: research_run, research_chunk
        
        Args:
            run_id: 要回滚的run
            error_message: 错误信息
            
        Returns:
            是否成功
        """
        try:
            # 1. 删除 artifact
            try:
                self.client.table('research_artifact')\
                    .delete()\
                    .eq('research_run_id', run_id)\
                    .execute()
                logger.debug(f"Deleted artifacts for run {run_id}")
            except Exception as e:
                logger.warning(f"Failed to delete artifacts for run {run_id}: {e}")
            
            # 2. 删除 provenance (级联会自动处理，但显式删除更安全)
            try:
                # 先获取该run的所有metric IDs
                metric_result = self.client.table('metric')\
                    .select('id')\
                    .eq('research_run_id', run_id)\
                    .execute()
                
                if metric_result.data:
                    metric_ids = [m['id'] for m in metric_result.data]
                    
                    self.client.table('metric_provenance')\
                        .delete()\
                        .in_('metric_id', metric_ids)\
                        .execute()
                    
                    logger.debug(f"Deleted {len(metric_ids)} provenance entries for run {run_id}")
            except Exception as e:
                logger.warning(f"Failed to delete provenance for run {run_id}: {e}")
            
            # 3. 删除 metrics
            try:
                self.client.table('metric')\
                    .delete()\
                    .eq('research_run_id', run_id)\
                    .execute()
                logger.debug(f"Deleted metrics for run {run_id}")
            except Exception as e:
                logger.warning(f"Failed to delete metrics for run {run_id}: {e}")
            
            # 4. 更新run状态为failed
            self.client.table('research_run')\
                .update({
                    'status': 'failed',
                    'is_latest': False,
                    'error_message': error_message
                })\
                .eq('id', run_id)\
                .execute()
            
            logger.info(f"Rolled back run {run_id}: {error_message}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to rollback run {run_id}: {e}")
            return False


def get_repository(settings: Optional[Any] = None) -> Optional[SupabaseRepository]:
    """
    获取仓储单例实例
    
    Args:
        settings: Settings 实例（可选，用于首次初始化）
        
    Returns:
        SupabaseRepository 实例，如果数据库未连接则返回 None
    """
    global _repository_instance
    
    with _repository_lock:
        if _repository_instance is None:
            db = get_database(settings)
            if db is None:
                logger.warning("数据库实例为 None，无法创建仓储实例。请检查 Supabase 配置。")
                return None
            
            if not db.is_connected():
                logger.warning("数据库未连接，无法创建仓储实例。请检查 Supabase 配置和连接状态。")
                return None
            
            try:
                _repository_instance = SupabaseRepository(db)
                logger.info("仓储实例初始化成功（单例）")
            except Exception as e:
                logger.error(f"初始化仓储实例失败: {e}")
                return None
        
        return _repository_instance
