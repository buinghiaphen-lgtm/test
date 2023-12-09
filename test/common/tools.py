import redis
import time
import json
from datetime import datetime
from pathlib import Path
import pylightxl as xl
from kafka import TopicPartition, KafkaConsumer
from setting import REDIS_HOST,REDIS_PORT,REDIS_DB,REDIS_HNAME
from setting import KAFKA_SERVER
import json
from common.logger import logger
class REDIS:
    '''
    缓存库类
    '''
    def __init__(self,host:str = REDIS_HOST,
                 port:int = REDIS_PORT,
                 db:int = REDIS_DB):
        self.redis_pool = redis.ConnectionPool(
            host = host,
            port = port,
            db = db
        )
        self.hname = REDIS_HNAME
        conn_time = 0
        while conn_time < 3:
            try:
                self.redis_conn = redis.Redis(connection_pool=self.redis_pool)
                return
            except Exception as e:
                conn_time += 1
                print(f'redis连接失败:{e}，20秒后，第{conn_time}次重试')
                # 20秒后重试
                time.sleep(20)

    def hget_value(self,redis_key):
        res = self.redis_conn.hget(self.hname,redis_key)
        return res

    def hset_key_value(self,redis_key,redis_value):
        res = self.redis_conn.hset(self.hname,redis_key,redis_value)
        return res


def clear_ticket_message():
    '''清除KAFKA的bd_ticket消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_ticket', 0)])
    for message in consumer:
        consumer.commit()

def clear_cancel_ticket_message():
    '''清除KAFKA的bd_cancel_ticket消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_cancel_ticket', 0)])
    for message in consumer:
        consumer.commit()

def clear_undo_ticket_message():
    '''清除KAFKA的bd_undo_ticket消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_undo_ticket', 0)])
    for message in consumer:
        consumer.commit()

def clear_win_ticket_message():
    '''清除KAFKA的bd_win_ticket消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_win_ticket', 0)])
    for message in consumer:
        consumer.commit()

def clear_paid_ticket_message():
    '''清除KAFKA的bd_paid_ticket消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_paid_ticket', 0)])
    for message in consumer:
        consumer.commit()

def clear_win_ticket_prize_message():
    '''清除KAFKA的bd_win_ticket_prize消息
    :return:
    '''
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
                             enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_win_ticket_prize', 0)])
    for message in consumer:
        consumer.commit()

def consume_bd_ticket_message(count:int):
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
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
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
                                  enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_cancel_ticket', 0)])
    t_count = 0
    data_list = []
    for message in consumer:
        logger.info('最初的kafka的数据-----')
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
        consumer.commit()
    return data_list

def consume_bd_undo_ticket_message(count:int):
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
                                  enable_auto_commit=True, group_id='test', consumer_timeout_ms=5000)
    consumer.assign([TopicPartition('bd_undo_ticket', 0)])
    t_count = 0
    data_list = []
    for message in consumer:
        # 在这里提交已消费过
        consumer.commit()
        logger.info('最初的kafka的数据-----')
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
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
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
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
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
    consumer = KafkaConsumer(bootstrap_servers=KAFKA_SERVER, auto_offset_reset='earliest',
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
