from datetime import datetime,date,time,timedelta
import json
import os
import re
from setting import KUBE_CONFIG_FILE
from common.k8s import  read_secret,read_configmap

def getCurrentTime():
    '''获取当前时间
    :return:
        str: %Y-%m-%d %H:%M:%S
        int: H%
    '''
    current = datetime.now()
    current_time = current.strftime('%Y-%m-%d %H:%M:%S')
    hour = int(current.strftime('%H'))
    return current_time,hour

def getIntervalTime(current_hour, origdata):
    '''初始化中断重推最大时长
    :param current_hour: 当前时间所在的小时
    :param origdata: 测试数据中的中断重推最大时长
    :return: int: 中断重推最大时长
    '''
    offset = int(origdata.split('Hour')[1])
    hour = current_hour + offset
    return hour

def getRedisTime(current_time, current_hour, origdata):
    '''初始化redis最后推送时间
    :param current_time: 当前时间
    :param current_hour: 当前时间所在的小时
    :param origdata: 测试数据中的redis最后推送时间
    :return: redis最后推送时间
    '''
    offset = int(origdata.split('Hour')[1])
    hour = current_hour + offset
    current = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
    redis_time_str = current - timedelta(hours=hour)
    redis_time = redis_time_str.strftime('%Y-%m-%d %H:%M:%S')
    return redis_time

def getPageRedisTimeList(current_time, origdata) ->list:
    '''初始化redis最后推送时间
    :param current_time: 当前时间
    :param origdata: 测试数据中的redis最后推送时间
    :return: 六类数据redis最后推送时间
    '''
    current_date = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
    origdata_list = origdata.split(',')
    data_list = []
    for dataone in origdata_list:
        if 'sec' in dataone:
            offset = int(dataone.split('sec')[0])
            redis_time = current_date - timedelta(seconds=offset)
            redis_time_str = redis_time.strftime('%Y-%m-%d %H:%M:%S')
            data_list.append(redis_time_str)
        elif 'min' in dataone:
            offset = int(dataone.split('min')[0])
            redis_time = current_date - timedelta(minutes=offset)
            redis_time_str = redis_time.strftime('%Y-%m-%d %H:%M:%S')
            data_list.append(redis_time_str)
    return data_list

def extract_jdbc_info(jdbc_url):
    pattern = r'jdbc:mysql://(?P<host>[^:/?]+)(:(?P<port>\d+))?(?P<dbname>/[^?]+)?(\?(?P<params>.*))?'
    match = re.match(pattern, jdbc_url)

    if match:
        groups = match.groupdict()
        host = groups['host']
        port = groups['port']
        dbname = groups['dbname'].lstrip('/') if groups['dbname'] else None
        params = groups['params']

        # 如果端口号存在，则转换为整数类型
        port = int(port) if port is not None else 3306

        # 解析参数
        params_dict = {}
        if params:
            param_pairs = params.split('&')
            for pair in param_pairs:
                key, value = pair.split('=')
                params_dict[key] = value

        return {
            'host': host,
            'port': port,
            'dbname': dbname,
            'params': params_dict
        }
    else:
        return None

if KUBE_CONFIG_FILE.exists():
    #db
    db_config_dict = read_secret(
        'tidb-secret',
        ('db_url', 'db_rw_password', 'db_rw_username', 'monitor_db_url'))
    db_conn_dict = extract_jdbc_info(db_config_dict['db_url'])
    db_host = db_conn_dict['host']
    db_port = db_conn_dict['port']
    db_name = db_conn_dict['dbname']
    db_user = db_config_dict['db_rw_username']
    db_password = db_config_dict['db_rw_password']
    #redis_ip
    redis_conn_dict = read_configmap(
        'service-config', ('realtime_redis_node', 'realtime_sentinel_master',
                           'sp_lock_redis_node', 'sp_lock_sentinel_master'))
    #redis_pwd
    redis_password_dict = read_secret(
        'redis-rmq-secret',
        ('realtime_redis_password', 'sp_lock_redis_password'))
    #realtime
    realtime_redis_sentinel_address = redis_conn_dict['realtime_redis_node']
    realtime_redis_password = redis_password_dict['realtime_redis_password']
    realtime_redis_master_name = redis_conn_dict['realtime_sentinel_master']
    #kafka
    kafka_service_dict = read_configmap(
        'realtime-kafka-config',('kafka_url'))
    kafka_url = kafka_service_dict['kafka_url']

