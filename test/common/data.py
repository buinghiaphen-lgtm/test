from datetime import datetime,date,time,timedelta


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


