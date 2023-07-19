import pymysql


class MySQL:
    def __init__(self, host, user, password, database, port=3306, charset='utf8'):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port
        self.charset = charset
        self.conn = None
        self.cursor = None

    def connect(self):
        # 连接数据库
        self.conn = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            charset=self.charset
        )
        # 创建游标对象
        self.cursor = self.conn.cursor()

    def close(self):
        # 关闭游标和连接
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def execute(self, sql, params=None):
        try:
            # 连接数据库
            self.connect()
            # 执行SQL语句
            self.cursor.execute(sql, params)
            # 提交事务
            self.conn.commit()
            # 返回执行结果
            if sql.strip().lower().startswith('insert into'):
                # 对于插入语句，直接返回id
                result = self.cursor.lastrowid
            else:
                # 对于其他，直接返回查询结果
                result = self.cursor.fetchall()
            return result
        except Exception as e:
            # 回滚事务
            self.conn.rollback()
            raise e
        finally:
            # 关闭游标和连接
            self.close()

    def fetchall(self, sql, params=None):
        try:
            # 连接数据库
            self.connect()
            self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
            # 执行SQL语句
            self.cursor.execute(sql, params)
            # 获取所有结果
            result = self.cursor.fetchall()
            return result
        except Exception as e:
            raise e
        finally:
            # 关闭游标和连接
            self.close()

    def fetchone(self, sql, params=None):
        try:
            # 连接数据库
            self.connect()
            self.cursor = self.conn.cursor(cursor=pymysql.cursors.DictCursor)
            # 执行SQL语句
            self.cursor.execute(sql, params)
            # 获取一条结果
            result = self.cursor.fetchone()
            return result
        except Exception as e:
            raise e
        finally:
            # 关闭游标和连接
            self.close()
