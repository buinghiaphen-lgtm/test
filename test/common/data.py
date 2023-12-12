from datetime import datetime,date,time,timedelta
import json

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

