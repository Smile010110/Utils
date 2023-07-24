import threading

from .database import MySQL

# 定义一个线程本地存储
thread_local = threading.local()


def get_mysql_connection():
    # 获取线程本地存储中的数据库连接，如果没有则创建一个新连接
    if not hasattr(thread_local, "mysql_connection"):
        thread_local.mysql_connection = MySQL(
            host='localhost',
            user='root',
            password='wahtwstm..',
            database='stock_daily_data'
        )
    return thread_local.mysql_connection


mysql = MySQL(host='localhost', user='root', password='wahtwstm..', database='stock_daily_data')