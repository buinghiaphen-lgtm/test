import redis
from redis.sentinel import Sentinel
import time
import json
from datetime import datetime
from pathlib import Path
import pylightxl as xl
from kafka import TopicPartition, KafkaConsumer
from setting import REDIS_HOST,REDIS_DB,REDIS_HNAME
from common.data import kafka_url,db_host,db_port,db_user,db_password,db_name,realtime_redis_sentinel_address,realtime_redis_password,realtime_redis_master_name
from setting import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER
# from setting import KAFKA_SERVER
import json
from common.logger import logger
import pylightxl as xl
from typing import Literal
import pymysql
from pymysql import FIELD_TYPE, converters
from pymysql.constants import CLIENT
# class REDIS:
#     '''
#     缓存库类
#     '''
#     def __init__(self,host:str = REDIS_HOST,
#                  port:int = REDIS_PORT,
#                  db:int = REDIS_DB):
#         self.redis_pool = redis.ConnectionPool(
#             host = host,
#             port = port,
#             db = db
#         )
#         self.hname = REDIS_HNAME
#         conn_time = 0
#         while conn_time < 3:
#             try:
#                 self.redis_conn = redis.Redis(connection_pool=self.redis_pool)
#                 return
#             except Exception as e:
#                 conn_time += 1
#                 print(f'redis连接失败:{e}，20秒后，第{conn_time}次重试')
#                 # 20秒后重试
#                 time.sleep(20)
#
#     def hget_value(self,redis_key):
#         res = self.redis_conn.hget(self.hname,redis_key)
#         return res
#
#     def hset_key_value(self,redis_key,redis_value):
#         res = self.redis_conn.hset(self.hname,redis_key,redis_value)
#         return res

# 2024-08-12修改，修改为哨兵模式
class REDIS:
    '''
    缓存库类
    '''
    def __init__(self,realtime_redis_sentinel_address:list = realtime_redis_sentinel_address,
                 REDIS_DB:int = 0):
        self.sentinel = Sentinel(realtime_redis_sentinel_address, password=realtime_redis_password, db=REDIS_DB)
        self.master_client = self.sentinel.master_for(service_name=realtime_redis_master_name)
        self.hname = realtime_redis_sentinel_address
    # def __init__(self,
    #              sentinel_address: str,
    #              master_name: str,
    #              password: str = None,
    #              db: int = 0):
    #     """Redis Sentinel连接
    #     Args:
    #         sentinel_address(str): _sentinel地址，格式为：ip1:port1,ip2:port2,ip3:port3
    #         master_name(str): _主库名称
    #         password(str, optional): _密码. Defaults to None.
    #         db(int, optional): _数据库. Defaults to 0.
    #     """
    #     self.sentinel_address = sentinel_address
    #     self.master_name = master_name
    #     self.password = password
    #     self.db = db
    #     self.sentinel = Sentinel(
    #         [(ip, port) for ip, port in
    #          [i.split(':') for i in sentinel_address.split(',')]],
    #         socket_timeout=5)
    #     self.master = self.sentinel.master_for(self.master_name,
    #                                            password=self.password,
    #                                            db=self.db,
    #                                            decode_responses=True)

    def hget_value(self,redis_key):
        res = self.master_client.hget(self.hname,redis_key)
        return res

    def hset_key_value(self,redis_key,redis_value):
        res = self.master_client.hset(self.hname,redis_key,redis_value)
        return res


def clear_ticket_message():
    '''清除KAFKA的bd_ticket消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_ticket', 0)])
    for message in consumer:
        consumer.commit()

def clear_cancel_ticket_message():
    '''清除KAFKA的bd_cancel_ticket消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_cancel_ticket', 0)])
    for message in consumer:
        consumer.commit()

def clear_undo_ticket_message():
    '''清除KAFKA的bd_undo_ticket消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_undo_ticket', 0)])
    for message in consumer:
        consumer.commit()

def clear_win_ticket_message():
    '''清除KAFKA的bd_win_ticket消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_win_ticket', 0)])
    for message in consumer:
        consumer.commit()

def clear_paid_ticket_message():
    '''清除KAFKA的bd_paid_ticket消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_paid_ticket', 0)])
    for message in consumer:
        consumer.commit()

def clear_win_ticket_prize_message():
    '''清除KAFKA的bd_win_ticket_prize消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_win_ticket_prize', 0)])
    for message in consumer:
        consumer.commit()

def consume_bd_ticket_message(count:int):
    if count == 1:
        return []
    else:
        count = count - 1
        consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                                      enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
        consumer.assign([TopicPartition('bd_ticket', 0)])
        t_count = 0
        data_list = []
        for message in consumer:
            consumer.commit()
            logger.info('bd_ticket最初的kafka的数据-----')
            logger.info(message.value)
            temp_list = []
            temp_count = 0
            m_json = json.loads(message.value.decode('utf-8'))
            m_json_len = len(m_json)
            if m_json_len > 1:
                temp_count = len(m_json[m_json_len - 1]['data']) + (m_json_len - 1) * 100
                for i in range(m_json_len):
                    temp_list.extend(m_json[i]['data'])
            else:
                temp_count += int(len(m_json[0]['data']))
                temp_list.extend(m_json[0]['data'])
            # 然后判断这一轮拿到的数据是否重复，不重复就把它放一起
            if all(x in data_list for x in temp_list):
                #print("temp_list包含在data_list中")
                pass
            else:
                # print("temp_list不包含在data_list中")
                data_list.extend(temp_list)
                t_count += temp_count
            if t_count == count:
                # return data_list
                return data_list
            elif t_count > count:
                return data_list
            # 在这里提交已消费过
        return data_list

def consume_bd_cancel_ticket_message(count:int):
    if count == 1:
        return []
    else:
        count = count - 1
        consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                                      enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
        consumer.assign([TopicPartition('bd_cancel_ticket', 0)])
        t_count = 0
        data_list = []
        for message in consumer:
            consumer.commit()
            logger.info('cancel_ticket最初的kafka的数据-----')
            logger.info(message.value)
            temp_list = []
            temp_count = 0
            m_json = json.loads(message.value.decode('utf-8'))
            m_json_len = len(m_json)
            if m_json_len > 1:
                temp_count = len(m_json[m_json_len - 1]['data']) + (m_json_len - 1) * 100
                for i in range(m_json_len):
                    temp_list.extend(m_json[i]['data'])
            else:
                temp_count += int(len(m_json[0]['data']))
                temp_list.extend(m_json[0]['data'])
            # 然后判断这一轮拿到的数据是否重复，不重复就把它放一起
            if all(x in data_list for x in temp_list):
                #print("temp_list包含在data_list中")
                pass
            else:
                # print("temp_list不包含在data_list中")
                data_list.extend(temp_list)
                t_count += temp_count

            if t_count == count:
                # return data_list
                return data_list
            elif t_count > count:
                return data_list
            # 在这里提交已消费过
        return data_list

def consume_bd_undo_ticket_message(count:int):
    if count == 1:
        return []
    else:
        count = count - 1
        consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                                      enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
        consumer.assign([TopicPartition('bd_undo_ticket', 0)])
        t_count = 0
        data_list = []
        for message in consumer:
            # 在这里提交已消费过
            consumer.commit()
            logger.info('undo_ticket最初的kafka的数据-----')
            logger.info(message.value)
            temp_list = []
            temp_count = 0
            m_json = json.loads(message.value.decode('utf-8'))
            m_json_len = len(m_json)
            if m_json_len > 1:
                temp_count = len(m_json[m_json_len - 1]['data']) + (m_json_len - 1) * 100
                for i in range(m_json_len):
                    temp_list.extend(m_json[i]['data'])
            else:
                temp_count += int(len(m_json[0]['data']))
                temp_list.extend(m_json[0]['data'])
            # 然后判断这一轮拿到的数据是否重复，不重复就把它放一起
            if all(x in data_list for x in temp_list):
                #print("temp_list包含在data_list中")
                pass
            else:
                # print("temp_list不包含在data_list中")
                data_list.extend(temp_list)
                t_count += temp_count

            if t_count == count:
                # return data_list
                return data_list
            elif t_count > count:
                return data_list
        return data_list

def consume_bd_win_ticket_message(count:int):
    if count == 1:
        return []
    else:
        count = count - 1
        consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                                      enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
        consumer.assign([TopicPartition('bd_win_ticket', 0)])
        t_count = 0
        data_list = []
        for message in consumer:
            # 在这里提交已消费过
            consumer.commit()
            logger.info('bd_win_ticket最初的kafka的数据-----')
            logger.info(message.value)
            temp_list = []
            temp_count = 0
            m_json = json.loads(message.value.decode('utf-8'))
            m_json_len = len(m_json)
            if m_json_len > 1:
                temp_count = len(m_json[m_json_len - 1]['data']) + (m_json_len - 1) * 100
                for i in range(m_json_len):
                    temp_list.extend(m_json[i]['data'])
            else:
                temp_count += int(len(m_json[0]['data']))
                temp_list.extend(m_json[0]['data'])
            # 然后判断这一轮拿到的数据是否重复，不重复就把它放一起
            if all(x in data_list for x in temp_list):
                #print("temp_list包含在data_list中")
                pass
            else:
                # print("temp_list不包含在data_list中")
                data_list.extend(temp_list)
                t_count += temp_count

            if t_count == count:
                # return data_list
                return data_list
            elif t_count > count:
                return data_list
        return data_list

def consume_bd_paid_ticket_message(count:int):
    if count == 1:
        return []
    else:
        count = count - 1
        consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                                      enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
        consumer.assign([TopicPartition('bd_paid_ticket', 0)])
        t_count = 0
        data_list = []
        for message in consumer:
            # 在这里提交已消费过
            consumer.commit()
            logger.info('bd_paid_ticket最初的kafka的数据-----')
            logger.info(message.value)
            temp_list = []
            temp_count = 0
            m_json = json.loads(message.value.decode('utf-8'))
            m_json_len = len(m_json)
            if m_json_len > 1:
                temp_count = len(m_json[m_json_len - 1]['data']) + (m_json_len - 1) * 100
                for i in range(m_json_len):
                    temp_list.extend(m_json[i]['data'])
            else:
                temp_count += int(len(m_json[0]['data']))
                temp_list.extend(m_json[0]['data'])
            # 然后判断这一轮拿到的数据是否重复，不重复就把它放一起
            if all(x in data_list for x in temp_list):
                #print("temp_list包含在data_list中")
                pass
            else:
                # print("temp_list不包含在data_list中")
                data_list.extend(temp_list)
                t_count += temp_count

            if t_count == count:
                # return data_list
                return data_list
            elif t_count > count:
                return data_list
        return data_list

def consume_bd_win_ticket_prize_message(count:int):
    if count == 1:
        return []
    else:
        count = count - 1
        consumer = KafkaConsumer(bootstrap_servers=kafka_url, auto_offset_reset='earliest',
                                      enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
        consumer.assign([TopicPartition('bd_win_ticket_prize', 0)])
        t_count = 0
        data_list = []
        for message in consumer:
            consumer.commit()
            logger.info('bd_win_ticket_prize最初的kafka的数据-----')
            logger.info(message.value)
            temp_list = []
            temp_count = 0
            m_json = json.loads(message.value.decode('utf-8'))
            m_json_len = len(m_json)
            if m_json_len > 1:
                temp_count = len(m_json[m_json_len - 1]['data']) + (m_json_len - 1) * 100
                for i in range(m_json_len):
                    temp_list.extend(m_json[i]['data'])
            else:
                temp_count += int(len(m_json[0]['data']))
                temp_list.extend(m_json[0]['data'])
            # 然后判断这一轮拿到的数据是否重复，不重复就把它放一起
            if all(x in data_list for x in temp_list):
                #print("temp_list包含在data_list中")
                pass
            else:
                # print("temp_list不包含在data_list中")
                data_list.extend(temp_list)
                t_count += temp_count

            if t_count == count:
                # return data_list
                return data_list
            elif t_count > count:
                return data_list
            # 在这里提交已消费过
        return data_list



class Select:
    """构造子查询语句
    """

    def __init__(self, table: str, column: str, query_dict: dict = None) -> None:
        """初始化

        Args:
            table (str): _表名
            column (str): _列名
            query_dict (dict, optional): _查询条件. Defaults to None.
        """
        self.sql, self.args = DB.sql_args_query(
            f'select `{column}`', table, query_dict)




class Excel:
    def __init__(self, filepath: str | Path):
        """excel文件，支持xlsx、xlsm

        Args:
            filepath (str | Path): _文件完整，支持str和pathlib.Path类型
        """
        self.file = xl.readxl(filepath)

    def __sheet(self, sheetname: str = None):
        """获取工作表对象

        Args:
            sheetname (str, optional): _工作表名称，不写默认第一个. Defaults to None.

        Returns:
            _type_: _description_
        """
        if not sheetname:
            sheetname = self.file.ws_names[0]
        return self.file.ws(sheetname)

    @staticmethod
    def __convert(type_: str, value):
        """根据type转换value的数据类型，返回转换后的结果

        Args:
            type_ (_str): _允许的数据类型有：str、int、float、json、list
            value (_type_): _需要转换的值
        """
        converted = value
        if value == 'null':
            converted = None
        elif type_ in ('str', 'int', 'float'):
            if value != '':
                converted = eval(f'{type_}(value)')
        elif type_ == 'json':
            if value:
                converted = json.loads(value)
            else:
                converted = {}
        elif type_ in ('list', 'timerange'):
            if value != '':
                converted = str(value).split(',')
            else:
                converted = []
        elif type_ == 'list|int':
            if value != '':
                converted = [int(v) for v in str(value).split(',')]
            else:
                converted = []
        elif type_ == 'list|float':
            if value != '':
                converted = [float(v) for v in str(value).split(',')]
            else:
                converted = []
        elif type_ == 'time':
            if value:
                converted = timestrf(datetime.strptime(
                    value, '%Y/%m/%d %H:%M:%S'))
        return converted

    def __get_data(self, return_type, sheetname: str = None, keyrow: int = 1, begin: int = 2, end: int = None) -> list:
        if return_type == 'dict' and not keyrow:
            raise ValueError('以字典形式返回数据时，必须要指定标题行keyrow')
        ws = self.__sheet(sheetname)
        result = []
        max_row = ws.maxrow
        if begin < 1:
            begin = 1
        if begin > max_row:
            begin = max_row
        if not end or end > max_row:
            end = max_row
        elif end < begin:
            end = begin
        if keyrow:
            if begin <= keyrow:
                begin = keyrow+1
            titlelist = ws.row(keyrow)
            keylist = []
            typelist = []
            for t in titlelist:
                t = t.strip()
                if not t:
                    break
                key_type = t.split('|', 1)
                key = key_type[0]
                if len(key_type) == 2:
                    type_ = key_type[1]
                else:
                    type_ = 'str'
                keylist.append(key)
                typelist.append(type_)
        for r in range(begin, end+1):
            valuelist = ws.row(r)
            if valuelist[0] == '':
                break
            try:
                if return_type == 'dict':
                    row_value = {}
                    for i, key in enumerate(keylist):
                        row_value[key] = self.__convert(
                            typelist[i], valuelist[i])
                elif keyrow:
                    row_value = [self.__convert(
                        typelist[i], valuelist[i]) for i in range(len(typelist))]
                else:
                    row_value = valuelist
                result.append(row_value)
            except:
                raise ValueError(
                    f'工作表：{sheetname}，第{r}行数据有误，类型：{typelist[i]}，值：{valuelist[i]}')
        return result

    def get_asdict(self, sheetname: str = None, keyrow: int = 1, begin: int = 2, end: int = None) -> list[dict]:
        """以字典列表的形式返回数据，字典的key为标题，要求每一列的第一个单元格不能为空。

        Args:
            sheetname (str, optional): _工作表名，不写默认第一个. Defaults to None.
            keyrow (int, optional): _标题行. Defaults to 1.
                标题中可以写明数据类型，以“|”间隔，获取数据时会自动转为对应类型，默认为str.
                允许的数据类型有：str、int、float、json、list、time、timerange,其中，list、timerange要求数据以英文逗号间隔，time要求数据为时间格式，获取时会
                自动转为%Y-%m-%d %H:%M:%S格式字符串。

                标题内容举例：

                    1. 标题1|json，表示该列数据类型为json字符串，获取数据时，该列数据将自动转为python object;
                    2. 比赛项目|list，单元格内容为：足球,篮球，获取该数据为：'比赛项目':['足球','篮球'];
            begin (int, optional): _数据开始行，行号从1开始。 Defaults to 2.
            end (int, optional): _数据结束行，行号从1开始。 Defaults to None. None时为最大行


        Returns:
            list: _一个列表，其中元素为字典
        """
        return self.__get_data('dict', sheetname=sheetname, keyrow=keyrow, begin=begin, end=end)

    def get_aslist(self, sheetname: str = None, keyrow: int = 1, begin: int = 2, end: int = None) -> list[list]:
        """以列表列表的形式返回数据，字典的key为标题，要求每一列的第一个单元格不能为空。

        Args:
            sheetname (str, optional): __工作表名，不写默认第一个. Defaults to None.
            keyrow (int, optional): _标题行. Defaults to 1.如果没有标题，则写None，数据不会处理格式。
                标题中可以写明数据类型，以“|”间隔，获取数据时会自动转为对应类型，默认为str.
                允许的数据类型有：str、int、float、json、list、time、timerange,其中，list、timerange要求数据以英文逗号间隔，time要求数据为时间格式，获取时会
                自动转为%Y-%m-%d %H:%M:%S格式字符串。

                标题内容举例：

                    1. 标题1|json，表示该列数据类型为json字符串，获取数据时，该列数据将自动转为python object;
                    2. 比赛项目|list，单元格内容为：足球,篮球，获取该数据为：['足球','篮球'];
            begin (int, optional): _数据开始行，行号从1开始。 Defaults to 2.
            end (int, optional): _数据结束行，行号从1开始。 Defaults to None. None时为最大行


        Returns:
            list: _一个列表，其中元素为列表
        """
        return self.__get_data('list', sheetname=sheetname, keyrow=keyrow, begin=begin, end=end)

def timestrf(dt: datetime) -> str:
    """调整时间格式
    Args:
        dt (datetime): _日期时间

    Returns:
        str: %Y-%m-%d %H:%M:%S格式字符串
    """
    try:
        result = dt.strftime('%Y-%m-%d %H:%M:%S')
    except:
        result = None
    return result



class DB:
    '''
    数据库类
    '''
    sql = None

    def __init__(self, host: str = db_host,
                 port: int = int(db_port), user: str = db_user,
                 password: str = db_password,
                 database: str = db_name) -> None:
        """_summary_

        Args:
            host (str, optional): _数据库IP. Defaults to '192.168.24.200'.
            port (int, optional): _端口号. Defaults to 3390.
            user (str, optional): _用户名. Defaults to 'v2t'.
            password (str, optional): _密码. Defaults to 'Vegas2.0'.
            database (str, optional): _库名. Defaults to 'test_vegas2'.
        """
        conn_time = 0
        conv = converters.conversions
        # conv[FIELD_TYPE.NEWDECIMAL] = float  # convert decimals to float
        conv[FIELD_TYPE.DATE] = str  # convert dates to strings
        conv[FIELD_TYPE.TIMESTAMP] = str  # convert dates to strings
        conv[FIELD_TYPE.TIME] = str  # convert dates to strings
        conv[FIELD_TYPE.DATETIME] = str  # convert dates to strings
        while conn_time < 3:
            try:
                self.conn = pymysql.connect(host=host, port=port,
                                            user=user,
                                            password=password,
                                            database=database,
                                            charset='utf8mb4', autocommit=True,
                                            client_flag=CLIENT.MULTI_STATEMENTS, conv=conv)
                return
            except Exception as e:
                conn_time += 1
                print(f'数据库连接失败:{e}，20秒后，第{conn_time}次重试')
                # 20秒后重试
                time.sleep(20)

    def get_asdict(self, sql: str, args=None) -> list[dict]:
        """以字典形式返回查询结果

        Args:
            sql (str): _查询语句
            args (tuple,list,dict): _参数化，用法同：https://pymysql.readthedocs.io/en/latest/modules/cursors.html#pymysql.cursors.Cursor.execute
        Returns:
            list: _列表中每一项为字典，格式
        """
        dict_cursor = self.conn.cursor(
            cursor=pymysql.cursors.SSDictCursor)
        dict_cursor.execute(sql, args=args)
        # 获取所有记录列表
        self.sql = dict_cursor._executed
        result = dict_cursor.fetchall()
        dict_cursor.close()
        return result

    def get_aslist(self, sql: str, args=None) -> list[list]:
        """以列表形式返回查询结果

        Args:
            sql (str): _查询语句
            args (tuple,list,dict): _参数化，用法同：https://pymysql.readthedocs.io/en/latest/modules/cursors.html#pymysql.cursors.Cursor.execute
        Returns:
            list: _列表中每一项为列表
        """
        cursor = self.conn.cursor(cursor=pymysql.cursors.SSCursor)
        cursor.execute(sql, args)
        self.sql = cursor._executed
        # 获取所有记录列表
        result = cursor.fetchall()
        cursor.close()
        return result

    def execute(self, sql: str, args=None) -> int:
        """执行sql

        Args:
            sql (str): _查询语句
            args (_type_, optional): _查询参数. Defaults to None.

        Returns:
            int: 受影响的行数
        """
        cursor = self.conn.cursor()
        try:
            result = cursor.execute(sql, args=args)
            self.sql = cursor._executed
            self.lastrowid = cursor.lastrowid
        except pymysql.err.ProgrammingError as e:
            print(f'查询语句：{cursor.mogrify(sql, args)}')
            raise e
        # self.conn.commit()
        finally:
            cursor.close()
        return result

    def executemany(self, sql: str, data: list | tuple):
        """根据data，执行多遍sql

        Args:
            sql (str): _查询语句
            data (list | tuple): _参数内容，例[[1,2],[3,4]]，第一遍执行使用[1,2]，第二遍执行使用[3,4]
        """
        cursor = self.conn.cursor()
        cursor.executemany(sql, data)
        self.sql = cursor._executed
        # self.conn.commit()
        cursor.close()

    def get_first(self, sql: str, sql_args: list = None) -> tuple:
        """返回第一条查询结果

        Args:
            sql (str): _查询语句
            sql_args (list, optional): _查询参数. Defaults to None.

        Returns:
            tuple: 第一条查询结果
        """
        cursor = self.conn.cursor()
        # sql = cursor.mogrify(sql, sql_args)
        cursor.execute(sql, sql_args)
        self.sql = cursor._executed
        result = cursor.fetchone()
        cursor.close()
        return result

    def get_one(self, sql: str, sql_args: list = None):
        """返回一个具体值

        Args:
            sql (str): _查询语句
            sql_args (list, optional): _查询参数. Defaults to None.

        Returns:
            any: 返回查询结果第一条第一列的内容
        """
        result = self.get_first(sql, sql_args)
        try:
            if result is not None:
                return result[0]
            else:
                return None
        except:
            print(self.sql)
            return None

    def get_max(self, table: str, column: str, query_dict: dict = {}):
        """返回指定表字段最大值

        Args:
            table (str): _表名
            column (str): _字段名
            query_dict (dict, optional): _条件，值为str时为等于，值为list时为in. Defaults to {}.
        """
        sql, sql_args = self.sql_args_query(
            f'select max({column})', table, query_dict)
        return self.get_one(sql, sql_args)

    def get_in_list(self, table: str, column: str, query_dict: dict = {}, orderby: Literal['ASC', 'DESC'] = None, size: int = None, page: int = None) -> list:
        """返回指定列的值列表

        Args:
            table (str): _指定表
            column (str): _指定字段
            query_dict (dict, optional): _条件，值为str时为等于，值为list时为in. Defaults to {}.
            orderby (Literal['ASC', 'DESC'], optional): _升序或降序，排序字段为入参column. Defaults to None，不排序.
            size (int, optional): _返回个数. Defaults to None.
            page (int, optional): _按照size分页，返回指定页的内容. Defaults to None.
        Returns:
            list: _指定列的值列表
        """
        if orderby and orderby.upper() in ['ASC', 'DESC']:
            orderby = f'{column} {orderby}'
        sql, sql_args = self.sql_args_query(
            f'select DISTINCT {column}', table, query_dict, orderby, size, page)
        tmp_list = self.get_aslist(sql, sql_args)
        return [t[0] for t in tmp_list]

    @staticmethod
    def _sql_where(query_dict: dict[str, int | list | str] = {}):
        """生成sql的where语句部分

        Args:
            query_dict (dict[str, int  |  list  |  str], optional): _参数字典. Defaults to {}.key可以由'列名__后缀'组成，
                后缀：
                'contains'表示包含，
                'startswith'表示以value开头，
                'endswith'表示以value结束，
                'lt'表示小于，
                'lte'表示小于等于，
                'gt'表示大于，
                'gte'表示大于等于，
                'isnull'表示为空,
                'not'表示不等于或not in。

        Raises:
            KeyError: _key的后缀错误
        """
        if not query_dict:
            return '', None
        sql_args = []
        sql = ' where '
        query_list = []
        for key, value in query_dict.items():
            args_flag = True
            if '__' in key:
                key, suffix = key.split('__')
            else:
                suffix = None
            if type(value) in (list, tuple, set):
                if len(value) == 0:
                    if suffix is None:
                        query_list.append(f'`{key}`!=`{key}`')
                    args_flag = False
                else:
                    if suffix == 'not':
                        query_list.append(f'`{key}` not in %s')
                    else:
                        query_list.append(f'`{key}` in %s')
            elif type(value) == Select:
                query_list.append(f'`{key}` in ({value.sql})')
                args_flag = False
                sql_args.extend(value.args)
            else:
                match suffix:
                    case None:
                        query_list.append(f'`{key}`=%s')
                    case 'contains':
                        query_list.append(
                            f"`{key}` like CONCAT('%%',%s,'%%')")
                    case 'startswith':
                        query_list.append(
                            f"`{key}` like CONCAT(%s,'%%')")
                    case 'endswith':
                        query_list.append(
                            f"`{key}` like CONCAT('%%',%s)")
                    case 'lt':
                        query_list.append(f"`{key}`<%s")
                    case 'lte':
                        query_list.append(f"`{key}`<=%s")
                    case 'gt':
                        query_list.append(f"`{key}`>%s")
                    case 'gte':
                        query_list.append(f"`{key}`>=%s")
                    case 'isnull':
                        query_list.append(f'`{key}` is NULL')
                        args_flag = False
                    case 'not':
                        query_list.append(f'`{key}`!=%s')
                    case _:
                        raise KeyError(f'{key}后缀{suffix}错误')
            if args_flag:
                sql_args.append(value)
        sql += ' and '.join(query_list)
        return sql, sql_args

    @staticmethod
    def sql_args_query(action: str, table: str, query_dict: dict[str, int | list | str] = {}, orderby: str = '', size: int = None, page: int = None):
        """根据字典构造查询语句

        Args:
            action (str): _sql的动作，例：'select draw_id'
            table (str): _表名
            query_dict (dict, optional): _参数字典. Defaults to {}.key可以由'列名__后缀'组成，
                后缀：
                'contains'表示包含，
                'startswith'表示以value开头，
                'endswith'表示以value结束，
                'lt'表示小于，
                'lte'表示小于等于，
                'gt'表示大于，
                'gte'表示大于等于，
                'isnull'表示为空,
                'not'表示不等于。
            orderby (str, optional): _排序条件. Defaults to ''.
            size (int, optional): _每页大小. Defaults to None.
            page (int, optional): _页数. Defaults to None.

        Returns:
            sql, sql_args: 查询语句，参数列表
        """
        sql = f'{action} from `{table}`'
        sql_where, sql_args = DB._sql_where(query_dict)
        sql += sql_where
        if orderby:
            sql += f' order by {orderby}'
        if size:
            if page:
                sql += f' limit {(page-1)*size},{size}'
            else:
                sql += f' limit {size}'
        return sql, sql_args

    def _update_exec(self, action: str, table: str, update_dict: dict[str, str | int] | list[dict], query_dict: dict = {}):
        sql = f'{action} `{table}` set '
        sql_args = []
        update_list = []
        if type(update_dict) == list:
            for key, value in update_dict[0].items():
                update_list.append(f'`{key}`=%s')
            for ud in update_dict:
                sql_args.append([v for k, v in ud.items()])
        else:
            for key, value in update_dict.items():
                update_list.append(f'`{key}`=%s')
                sql_args.append(value)
        sql += ','.join(update_list)
        if query_dict:
            sql_where, args_where = DB._sql_where(query_dict)
            sql += sql_where
            if type(update_dict) == list:
                for arg in sql_args:
                    arg.extend(args_where)
            else:
                sql_args.extend(args_where)
        if type(update_dict) == list:
            self.executemany(sql, sql_args)
        else:
            self.execute(sql, sql_args)

    def update(self, table: str, update_dict: dict | list[dict], query_dict: dict = {}):
        """执行update命令

        Args:
            table (str): _表名
            update_dict (dict,list[dict]): _更新字典，为list时，更新多条数据
            query_dict (dict, optional): _查询字典.key参考sql_args_query方法 Defaults to {}.
        """
        self._update_exec('update', table, update_dict, query_dict)

    def replace_into(self, table: str, update_dict: dict[str, str | int] | list[dict]):
        """执行replace into命令，入参说明参考update方法"""
        self._update_exec('replace into', table, update_dict)

    def insert(self, table: str, update_dict: dict[str, str | int] | list[dict]):
        """执行insert into命令，入参说明参考update方法"""
        self._update_exec('insert into', table, update_dict)

    def delete(self, table: str, query_dict: dict):
        """删除数据

        Args:
            table (str): _表名
            query_dict (dict): _查询条件字典，值可以为字符串、数字、列表，查询条件为=或in
        """
        sql, sql_args = self.sql_args_query('delete', table, query_dict)
        self.execute(sql, sql_args)

    def count(self, table: str, query_dict: dict) -> int:
        """获取数量

        Args:
            table (str): _表名
            query_dict (dict): _查询条件字典，值可以为字符串、数字、列表，查询条件为=或in

        Returns:
            int: _符合条件的数量
        """
        sql, sql_args = self.sql_args_query(
            'select count(1)', table, query_dict)
        return self.get_one(sql, sql_args)

    def exec_file(self, filepath: str | Path):
        """执行sql文件

        Args:
            filepath (str | Path): _文件路径

        """
        with open(filepath, 'r', encoding='utf-8') as f:
            sql_file = f.read()
        cursor = self.conn.cursor()
        cursor.execute(sql_file)
        self.sql = cursor._executed
        cursor.close()

    def recovery_from_data(self, table: str, data_list: list[dict] | list[list], col_list: list = None):
        """恢复数据，数据中必须有主键或唯一索引

        Args:
            table (str): 要恢复的表名
            data_list (list): _需要恢复的数据，每条数据为字典或列表形式，字典key：列名
            col_list (list): _表的列，只有data_list由列表组成时使用，不写则全部列，需要注意顺序
        """
        if type(data_list[0]) == dict:
            col_list = [f'`{col}`' for col in data_list[0].keys()]
            values = [(value for value in data.values())
                      for data in data_list]
        else:
            values = data_list
            if col_list:
                col_list = [f'`{col}`' for col in col_list]
        colstr = f"({','.join(col_list)})" if col_list else ''
        sql = f'replace into `{table}`{colstr} values({",".join(["%s"]*len(values[0]))})'
        self.executemany(sql, values)

    def recovery_from_table(self, table: str, query_dict: dict = None, table_bak: str = None):
        """从table_bak中恢复数据到表table中

        Args:
            table (str): _想要恢复的表
            query_dict (dict, optional): _查询字典，不写为所有数据. Defaults to None.
            table_bak (str, optional): _备份表名，默认为table后面加_bak
        """
        if not table_bak:
            table_bak = f'{table}_bak'
        if self.get_one('SHOW TABLES LIKE %s', [table_bak]) is None:
            print(f'不存在备份表{table_bak}')
            return
        sql, sqlargs = self.sql_args_query(
            f'replace into `{table}` select *', table_bak, query_dict)
        self.execute(sql, sqlargs)

    def backup_to_table(self, table: str, query_dict: dict = None, table_bak: str = None):
        """备份表table数据，到新表table_bak中，存在则更新

        Args:
            table (str): _要备份的表
            query_dict (dict): _查询字典，不写为备份全部数据
            table_bak (str): _备份表名，默认为table后面加_bak
        """
        if not table_bak:
            table_bak = f'{table}_bak'
        create_ddl = self.get_first(f'show CREATE table {table}')[
            1].split('ENGINE', 1)[0].replace(f'CREATE TABLE `{table}`', f'CREATE TABLE IF NOT EXISTS `{table_bak}`', 1)
        self.execute(create_ddl)
        sql, sqlargs = self.sql_args_query(
            f'replace into `{table_bak}` select *', table, query_dict)
        self.execute(sql, sqlargs)

    def backup_to_file(self, bak_file: Path, table: str, query_dict: dict = None):
        """备份表table数据成replace语句，到文件bak_file中

        Args:
            bak_file (Path): _备份文件的Path对象
            table (str): _要备份的表
            query_dict (dict): _查询字典，不写为备份全部数据
        """
        bak_file.parent.mkdir(parents=True, exist_ok=True)
        cols_list = [c[0] for c in self.get_aslist(
            'select COLUMN_NAME from information_schema.columns where TABLE_NAME=%s and TABLE_SCHEMA=%s', [table, DB_NAME])]
        sql, sqlargs = self.sql_args_query(
            f'select *', table, query_dict)
        data_to_bak = self.get_aslist(sql, sqlargs)
        cols = ','.join(cols_list)
        sql = ''
        for data in data_to_bak:
            sql += f'replace into {table} ({cols}) values {data};'
        sql = sql.replace(', None', ', NULL').replace('Decimal(', '(')
        with open(bak_file, 'w', encoding='utf8') as f:
            f.write(sql)

    def drop_partition(self, table: str, names: list, prefix: str = 'p'):
        """删除分区

        Args:
            table (str): _表名
            names (list): _PARTITION_NAME除去前缀的列表
            prefix (str, optional): _PARTITION_NAME的前缀，默认为p
        """
        p_names = [f'{prefix}{name}' for name in names]
        p_list = self.get_aslist("""select t.PARTITION_NAME
            from INFORMATION_SCHEMA.`PARTITIONS` t
            where t.TABLE_SCHEMA =%s
            and t.TABLE_NAME=%s and t.PARTITION_NAME in %s""", [DB_NAME, table, p_names])
        pn_list = [pn[0] for pn in p_list]
        sql = ''
        for name in p_names:
            if name in pn_list:
                sql += f'alter table {table} drop PARTITION {name};'
        self.execute(sql)
        # self.executemany(f'alter table {table} drop PARTITION {prefix}%s', [
        #                  [int(name)]for name in names])

    def add_partition(self, table: str, names: list, prefix: str = 'p'):
        """创建分区

        Args:
            table (str): _表名
            names (list): _PARTITION_NAME除去前缀的列表
            prefix (str, optional): _PARTITION_NAME的前缀，默认为p
        """
        p_names = [f'{prefix}{name}' for name in names]
        p_list = self.get_aslist("""select t.PARTITION_NAME
            from INFORMATION_SCHEMA.`PARTITIONS` t
            where t.TABLE_SCHEMA =%s
            and t.TABLE_NAME=%s and t.PARTITION_NAME in %s""", [DB_NAME, table, p_names])
        pn_list = [pn[0] for pn in p_list]
        sql = ''
        for name in p_names:
            if name not in pn_list:
                sql += f'alter table {table} add partition (partition {name} values in ({name[1:]}));'
        if sql:
            self.execute(sql)

    def close(self):
        self.conn.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, trace):
        self.close()


class Excel:
    def __init__(self, filepath: str | Path):
        """excel文件，支持xlsx、xlsm

        Args:
            filepath (str | Path): _文件完整，支持str和pathlib.Path类型
        """
        self.file = xl.readxl(filepath)

    def __sheet(self, sheetname: str = None):
        """获取工作表对象

        Args:
            sheetname (str, optional): _工作表名称，不写默认第一个. Defaults to None.

        Returns:
            _type_: _description_
        """
        if not sheetname:
            sheetname = self.file.ws_names[0]
        return self.file.ws(sheetname)

    @staticmethod
    def __convert(type_: str, value):
        """根据type转换value的数据类型，返回转换后的结果

        Args:
            type_ (_str): _允许的数据类型有：str、int、float、json、list
            value (_type_): _需要转换的值
        """
        converted = value
        if value == 'null':
            converted = None
        elif type_ in ('str', 'int', 'float'):
            if value != '':
                converted = eval(f'{type_}(value)')
        elif type_ == 'json':
            if value:
                converted = json.loads(value)
            else:
                converted = {}
        elif type_ in ('list', 'timerange'):
            if value != '':
                converted = str(value).split(',')
            else:
                converted = []
        elif type_ == 'list|int':
            if value != '':
                converted = [int(v) for v in str(value).split(',')]
            else:
                converted = []
        elif type_ == 'list|float':
            if value != '':
                converted = [float(v) for v in str(value).split(',')]
            else:
                converted = []
        elif type_ == 'time':
            if value:
                converted = timestrf(datetime.strptime(
                    value, '%Y/%m/%d %H:%M:%S'))
        return converted

    def __get_data(self, return_type, sheetname: str = None, keyrow: int = 1, begin: int = 2, end: int = None) -> list:
        if return_type == 'dict' and not keyrow:
            raise ValueError('以字典形式返回数据时，必须要指定标题行keyrow')
        ws = self.__sheet(sheetname)
        result = []
        max_row = ws.maxrow
        if begin < 1:
            begin = 1
        if begin > max_row:
            begin = max_row
        if not end or end > max_row:
            end = max_row
        elif end < begin:
            end = begin
        if keyrow:
            if begin <= keyrow:
                begin = keyrow+1
            titlelist = ws.row(keyrow)
            keylist = []
            typelist = []
            for t in titlelist:
                t = t.strip()
                if not t:
                    break
                key_type = t.split('|', 1)
                key = key_type[0]
                if len(key_type) == 2:
                    type_ = key_type[1]
                else:
                    type_ = 'str'
                keylist.append(key)
                typelist.append(type_)
        for r in range(begin, end+1):
            valuelist = ws.row(r)
            if valuelist[0] == '':
                break
            try:
                if return_type == 'dict':
                    row_value = {}
                    for i, key in enumerate(keylist):
                        row_value[key] = self.__convert(
                            typelist[i], valuelist[i])
                elif keyrow:
                    row_value = [self.__convert(
                        typelist[i], valuelist[i]) for i in range(len(typelist))]
                else:
                    row_value = valuelist
                result.append(row_value)
            except:
                raise ValueError(
                    f'工作表：{sheetname}，第{r}行数据有误，类型：{typelist[i]}，值：{valuelist[i]}')
        return result

    def get_asdict(self, sheetname: str = None, keyrow: int = 1, begin: int = 2, end: int = None) -> list[dict]:
        """以字典列表的形式返回数据，字典的key为标题，要求每一列的第一个单元格不能为空。

        Args:
            sheetname (str, optional): _工作表名，不写默认第一个. Defaults to None.
            keyrow (int, optional): _标题行. Defaults to 1.
                标题中可以写明数据类型，以“|”间隔，获取数据时会自动转为对应类型，默认为str.
                允许的数据类型有：str、int、float、json、list、time、timerange,其中，list、timerange要求数据以英文逗号间隔，time要求数据为时间格式，获取时会
                自动转为%Y-%m-%d %H:%M:%S格式字符串。

                标题内容举例：

                    1. 标题1|json，表示该列数据类型为json字符串，获取数据时，该列数据将自动转为python object;
                    2. 比赛项目|list，单元格内容为：足球,篮球，获取该数据为：'比赛项目':['足球','篮球'];
            begin (int, optional): _数据开始行，行号从1开始。 Defaults to 2.
            end (int, optional): _数据结束行，行号从1开始。 Defaults to None. None时为最大行


        Returns:
            list: _一个列表，其中元素为字典
        """
        return self.__get_data('dict', sheetname=sheetname, keyrow=keyrow, begin=begin, end=end)

    def get_aslist(self, sheetname: str = None, keyrow: int = 1, begin: int = 2, end: int = None) -> list[list]:
        """以列表列表的形式返回数据，字典的key为标题，要求每一列的第一个单元格不能为空。

        Args:
            sheetname (str, optional): __工作表名，不写默认第一个. Defaults to None.
            keyrow (int, optional): _标题行. Defaults to 1.如果没有标题，则写None，数据不会处理格式。
                标题中可以写明数据类型，以“|”间隔，获取数据时会自动转为对应类型，默认为str.
                允许的数据类型有：str、int、float、json、list、time、timerange,其中，list、timerange要求数据以英文逗号间隔，time要求数据为时间格式，获取时会
                自动转为%Y-%m-%d %H:%M:%S格式字符串。

                标题内容举例：

                    1. 标题1|json，表示该列数据类型为json字符串，获取数据时，该列数据将自动转为python object;
                    2. 比赛项目|list，单元格内容为：足球,篮球，获取该数据为：['足球','篮球'];
            begin (int, optional): _数据开始行，行号从1开始。 Defaults to 2.
            end (int, optional): _数据结束行，行号从1开始。 Defaults to None. None时为最大行


        Returns:
            list: _一个列表，其中元素为列表
        """
        return self.__get_data('list', sheetname=sheetname, keyrow=keyrow, begin=begin, end=end)
