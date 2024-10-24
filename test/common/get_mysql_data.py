import pymysql
from pymysql.constants import CLIENT
# import os
# from common.read_data import data
from pymysql import converters,FIELD_TYPE
# from setting import DB_HOST,DB_PORT,DB_USER,DB_PASSWORD,DB_NAME
from common.data import db_host,db_port,db_name,db_user,db_password

pymysql.install_as_MySQLdb()

#数据格式转换
conv = converters.conversions
# conv[FIELD_TYPE.NEWDECIMAL] = float  # convert decimals to float
conv[FIELD_TYPE.DATE] = str  # convert dates to strings
conv[FIELD_TYPE.TIMESTAMP] = str  # convert dates to strings
conv[FIELD_TYPE.DATETIME] = str  # convert dates to strings
conv[FIELD_TYPE.TIME] = str  # convert dates to strings


class MysqlDb:
    """初始化地址"""
    def __init__(self):
        self.host = db_host
        self.port = db_port
        self.user = db_user
        self.password = db_password
        self.db_name = db_name
        self.cursorclass = pymysql.cursors.DictCursor
        self.client_flag = CLIENT.MULTI_STATEMENTS

    def select_db(self, sql):
        """查询
            :return:
            tuple[tuple[Any, ...], ...]
        """
        connection = pymysql.connect(host=self.host,
                                     port=self.port,
                                     user=self.user,
                                     password=self.password,
                                     database=self.db_name,
                                     cursorclass=self.cursorclass)

        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                data = cursor.fetchall()
                return data

    def select_db_value_desc(self, sql):
        """查询
            :return:
            data:tuple[tuple[Any, ...], ...]
            des_list: list[str]
        """
        connection = pymysql.connect(host=self.host,
                                     port=self.port,
                                     user=self.user,
                                     password=self.password,
                                     database=self.db_name,
                                     cursorclass=self.cursorclass)

        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql.encode('utf-8'))
                data = cursor.fetchall()
                # des = cursor.description
                # des_list = [item[0] for item in des]
                des_list = [i[0] for i in cursor.description]
                return data, des_list


    def select_db_val(self, sql, val):
        """查询
            :return:
            data:tuple[tuple[Any, ...], ...]
        """
        connection = pymysql.connect(host=self.host,
                                     port=self.port,
                                     user=self.user,
                                     password=self.password,
                                     database=self.db_name,
                                     cursorclass=self.cursorclass)

        with connection:
            with connection.cursor() as cursor:
                cursor.execute(sql, val)
                data = cursor.fetchall()
                return data

    def execute_db(self, sql):
        """更新/插入/删除
            :return:
            data:int 成功数量
        """
        connection = pymysql.connect(host=self.host,
                                     port=self.port,
                                     user=self.user,
                                     password=self.password,
                                     database=self.db_name,
                                     cursorclass=self.cursorclass,
                                     client_flag=self.client_flag)
        try:
            with connection.cursor() as cursor:
                # cursor.execute(sql)
                data = cursor.execute(sql)

            connection.commit()
            return data
        except Exception as e:
            connection.rollback()
            print("操作出现错误：{}".format(e))
        finally:
            connection.close()

    def execute_db_val(self, sql, val):
        """更新/插入/删除
            :return:
            data:int 成功数量
        """
        connection = pymysql.connect(host=self.host,
                                     port=self.port,
                                     user=self.user,
                                     password=self.password,
                                     database=self.db_name,
                                     cursorclass=self.cursorclass)
        try:
            with connection.cursor() as cursor:
                data = cursor.execute(sql, val)

            connection.commit()
            return data
        except Exception as e:
            connection.rollback()
            print("操作出现错误：{}".format(e))
        finally:
            connection.close()
    def executemany_db_val(self, sql, val):
        # val 传入的是一个列表
        """更新/插入/删除
            :return:
            data:int 成功数量
        """
        connection = pymysql.connect(host=self.host,
                                     port=self.port,
                                     user=self.user,
                                     password=self.password,
                                     database=self.db_name,
                                     cursorclass=self.cursorclass)
        try:
            with connection.cursor() as cursor:
                data = cursor.executemany(sql, val)

            connection.commit()
            return data
        except Exception as e:
            connection.rollback()
            print("操作出现错误：{}".format(e))
        finally:
            connection.close()




