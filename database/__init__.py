import os
import redis
import logging
import pymysql
import psycopg2
import paramiko

from psycopg2 import extras
from typing import Dict, Tuple, List, Optional, Union, Any
from sshtunnel import SSHTunnelForwarder, BaseSSHTunnelForwarderError


# 设置日志
logging.basicConfig(level=logging.INFO, format='[%(asctime)s][%(name)s] [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

class SSHTunnelManager:
    """SSH隧道管理基类"""
    
    def __init__(self, ssh_config: Dict[str, Union[str, int]]):
        self.ssh_config = ssh_config
        self.tunnel: Optional[SSHTunnelForwarder] = None
        
    def _setup_ssh_tunnel(self, remote_host: str, remote_port: int) -> Tuple[str, int]:
        """建立SSH隧道并返回本地绑定地址"""
        pkey = None
        if self.ssh_config.get('ssh_pkey_path'):
            pkey = paramiko.RSAKey.from_private_key_file(
                self.ssh_config['ssh_pkey_path'],
                password=self.ssh_config.get('ssh_pkey_password')
            )
        
        self.tunnel = SSHTunnelForwarder(
            ssh_address_or_host=(self.ssh_config['ssh_host'], self.ssh_config['ssh_port']),
            ssh_username=self.ssh_config['ssh_username'],
            ssh_password=self.ssh_config['ssh_password'],
            ssh_pkey=pkey,
            remote_bind_address=(remote_host, remote_port)
        )
        self.tunnel.start()
        logger.info(f"SSH隧道已建立，本地端口: {self.tunnel.local_bind_port}")
        return '127.0.0.1', self.tunnel.local_bind_port
    
    def _close_ssh_tunnel(self):
        """关闭SSH隧道"""
        if self.tunnel and self.tunnel.is_active:
            try:
                self.tunnel.stop()
                logger.info("SSH隧道已关闭")
            except Exception as e:
                logger.exception(f"关闭SSH隧道失败: {str(e)}")
            self.tunnel = None


class RedisConnector(SSHTunnelManager):
    """Redis 连接器"""
    
    def __init__(self, config: Dict[str, Union[str, int]], use_ssh: bool = False):
        # 验证必要配置
        if use_ssh and not all(k in config for k in ['ssh_host', 'ssh_username']):
            raise ValueError("SSH模式必须提供 ssh_host 和 ssh_username")
        
        if not config.get("redis_host"):
            raise ValueError("必须提供redis_host配置")
            
        super().__init__(config)
        self.use_ssh = use_ssh
        self.redis_config = {
            'host': config["redis_host"],
            'port': int(config.get("redis_port", 6379)),
            'db': int(config.get("redis_db", 0)),
            'password': config.get("redis_password"),
            'decode_responses': True
        }
        self.redis_conn: Optional[redis.Redis] = None
        self.is_connected = False
    
    def connect(self):
        """建立Redis连接"""
        if self.is_connected:
            logger.warning("Redis连接已存在")
            return
            
        try:
            if self.use_ssh:
                local_host, local_port = self._setup_ssh_tunnel(
                    self.redis_config['host'], 
                    self.redis_config['port']
                )
                self.redis_config.update({
                    'host': local_host,
                    'port': local_port
                })

            self.redis_conn = redis.Redis(**self.redis_config)
            
            if not self.redis_conn.ping():
                raise ConnectionError("Redis连接测试失败")
                
            self.is_connected = True
            logger.info("Redis连接成功")
            
        except (redis.RedisError, BaseSSHTunnelForwarderError, paramiko.ssh_exception.SSHException) as e:
            logger.exception(f"连接失败: {type(e).__name__}: {str(e)}")
            self.disconnect()
            raise
    
    def disconnect(self):
        """断开连接并清理资源"""
        if self.redis_conn:
            try:
                self.redis_conn.close()
                logger.info("Redis连接已关闭")
            except Exception as e:
                logger.exception(f"关闭Redis连接失败: {str(e)}")
                
            self.redis_conn = None
            
        if self.use_ssh:
            self._close_ssh_tunnel()
            
        self.is_connected = False
        
    def __enter__(self):
        self.connect()
        return self.redis_conn  # 直接返回redis连接对象

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()


class DBConnector(SSHTunnelManager):
    """数据库连接器"""
    
    def __init__(self, config: Dict[str, Union[str, int]], use_ssh: bool = False):
        # 验证必要配置
        required = ['db_type', 'db_host', 'db_port', 'db_user', 'db_name']
        if any(field not in config for field in required):
            raise ValueError(f"缺少必要配置项: {required}")
        
        if use_ssh and not all(k in config for k in ['ssh_host', 'ssh_username']):
            raise ValueError("SSH模式必须提供 ssh_host 和 ssh_username")
            
        super().__init__(config)
        self.use_ssh = use_ssh
        self.db_config = {
            'type': config['db_type'].lower(),
            'host': config['db_host'],
            'port': int(config['db_port']),
            'user': config['db_user'],
            'password': config.get('db_password'),
            'name': config['db_name']
        }
        self.conn: Optional[Union[pymysql.connections.Connection, psycopg2.extensions.connection]] = None
        self.is_connected = False
    
    def connect(self):
        """建立数据库连接"""
        if self.is_connected:
            logger.warning("数据库连接已存在")
            return
            
        try:
            if self.use_ssh:
                local_host, local_port = self._setup_ssh_tunnel(
                    self.db_config['host'], 
                    self.db_config['port']
                )
            else:
                local_host, local_port = self.db_config['host'], self.db_config['port']

            if self.db_config['type'] == "mysql":
                self.conn = pymysql.connect(
                    host=local_host,
                    port=local_port,
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    database=self.db_config['name'],
                    charset='utf8mb4',
                    cursorclass=pymysql.cursors.DictCursor
                )
                
            elif self.db_config['type'] == "postgresql":
                self.conn = psycopg2.connect(
                    host=local_host,
                    port=local_port,
                    user=self.db_config['user'],
                    password=self.db_config['password'],
                    dbname=self.db_config['name'],
                    cursor_factory=extras.DictCursor  # 使用DictCursor自动转换结果
                )
                self.conn.autocommit = False
                
            else:
                raise ValueError(f"不支持的数据库类型: {self.db_config['type']}")
            
            # 测试连接
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                
            self.is_connected = True
            logger.info(f"{self.db_config['type'].upper()}数据库连接成功")
            
        except (pymysql.Error, psycopg2.Error, ValueError) as e:
            logger.exception(f"连接失败: {type(e).__name__}: {str(e)}")
            self.disconnect()
            raise
        
    def disconnect(self):
        """断开连接并清理资源"""
        if self.conn:
            try:
                if self.db_config['type'] == "mysql":
                    if self.conn.open:
                        self.conn.close()
                elif self.db_config['type'] == "postgresql":
                    if not self.conn.closed:
                        self.conn.close()
                        
                logger.info(f"{self.db_config['type'].upper()}数据库连接已关闭")
            except Exception as e:
                logger.exception(f"关闭{self.db_config['type'].upper()}数据库连接失败: {str(e)}")
                
            self.conn = None
            
        if self.tunnel and self.tunnel.is_active:
            try:
                self.tunnel.stop()
                logger.info("SSH隧道已关闭")
            except Exception as e:
                logger.exception(f"关闭SSH隧道失败: {str(e)}")
                
            self.tunnel = None
            
        self.is_connected = False
    
    def execute_query(self, sql: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
        """执行查询语句"""
        if not self.is_connected or not self.conn:
            raise ConnectionError("数据库未连接")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                return cursor.fetchall()
                
        except (pymysql.Error, psycopg2.Error) as e:
            logger.exception(f"查询执行失败: {type(e).__name__}: {str(e)}\nSQL: {sql}")
            
            if self.conn:
                self.conn.rollback()
            raise
    
    def execute_update(self, sql: str, params: Optional[Tuple] = None) -> int:
        """执行更新操作"""
        if not self.is_connected or not self.conn:
            raise ConnectionError("数据库未连接")
        
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql, params or ())
                row_count = cursor.rowcount
                self.conn.commit()
                return row_count
                
        except (pymysql.Error, psycopg2.Error) as e:
            logger.exception(f"更新执行失败: {type(e).__name__}: {str(e)}\nSQL: {sql}")
            
            if self.conn:
                self.conn.rollback()
            raise
    
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    @property
    def connection(self):
        """获取原生数据库连接对象"""
        return self.conn


if __name__ == '__main__':
    use_ssh = True
    config = {
        "ssh_host": "",
        "ssh_port": 22,
        "ssh_username": "",
        "ssh_password": None,
        "ssh_pkey_path": os.path.join(os.path.expanduser("~/.ssh"), "id_rsa"),
        "ssh_pkey_password": None,
    }

    redis_config = {
        "redis_host": "",
        "redis_port": 6379,
        "redis_db": 0,
        "redis_password": "",
    }
    redis_config.update(config)
    logger.info(f"Redis配置: {redis_config}")

    gw_config = {
        "db_type": "mysql",  # 可选: "mysql" 或 "postgresql"
        "db_host": "",
        "db_port": 3306,
        "db_user": "root",
        "db_password": "",
        "db_name": "",
    }
    gw_config.update(config)
    logger.info(f"网关数据库配置: {gw_config}")

    with RedisConnector(redis_config, use_ssh) as redis_conn:
        rank_key = f""
        rank_data = redis_conn.zrevrange(rank_key, 0, -1, withscores=True)
        
        logger.info(f"rank_data: {rank_data}")
    
    with DBConnector(gw_config, use_ssh) as db_conn:
        sql = ""
        res = db_conn.execute_query(sql)
        
        logger.info(f"查询结果: {res}")
