import time

import pytest
from textwrap import dedent
from pathlib import Path
import asyncio
from datetime import datetime, timedelta

from common import data as comdata
from common.tools import Excel,REDIS
from common.tools import consume_bd_ticket_message, consume_bd_cancel_ticket_message, consume_bd_undo_ticket_message, consume_bd_win_ticket_message, consume_bd_paid_ticket_message, consume_bd_win_ticket_prize_message
from common.tools import  clear_ticket_message, clear_cancel_ticket_message, clear_undo_ticket_message, clear_win_ticket_message, clear_paid_ticket_message, clear_win_ticket_prize_message
from common.logger import logger
from common.k8sops import load_client_config, modify_configmap_by_key, restart_deployment, stop_deployment
from common import processData as proData
from common.data import realtime_redis_sentinel_address,realtime_redis_master_name,realtime_redis_password
from setting import TEST_DATA_PATH,BASE_DIR


excel_data = Excel(TEST_DATA_PATH)

redis_init_time = ''
redis_init_time_list = []
current_init_time = ''
kubeconfig_path = None
# kubeconfig_path = str(Path(BASE_DIR, "config", "kubeconfig.yaml"))
kafkaLastTime = 'kafkaLastTime'

redis_request = REDIS(
    sentinel_address=realtime_redis_sentinel_address, master_name=realtime_redis_master_name,
    password=realtime_redis_password).master


@pytest.fixture()
def realtime_kafka_fixture(request):
    global kubeconfig_path
    namespace = load_client_config(kubeconfig_path)
    configmap_name = 'realtime-kafka-config'
    deployment_name = 'realtime-kafka-service'

    stop_deployment(kubeconfig_path, namespace, deployment_name)
    logger.info(request.param)
    data = request.param
    global redis_init_time
    global current_init_time
    global redis_init_time_list
    if ('分页推送' not in data[0])  and data[0] != '循环推送':
        # 获取当前时间 和 当前时间的小时
        current_time, current_hour = comdata.getCurrentTime()
        interval_time = comdata.getIntervalTime(current_hour,data[3])
        redis_time = comdata.getRedisTime(current_time, current_hour, data[4])
        redis_init_time = redis_time
        current_init_time = current_time
        logger.info(current_time)
        logger.info(current_hour)
        logger.info(interval_time)
        logger.info(redis_time)
        # data[3]是测试数据未处理的中断重推最大时长
        # 先去修改k8s的配置，data[1]是推送时间间隔,data[2]是kafka一次最多推送N条数据,interval_time是中断重推最大时长
        config_key_list = ['second', 'count', 'intervalTime']
        config_value_list = [str(data[1]), str(data[2]), str(interval_time)]
        # kubeconfig_path = None
        # kubeconfig_path = str(Path(BASE_DIR, "config", "kubeconfig.yaml"))
        namespace = load_client_config(kubeconfig_path)
        configmap_name = 'realtime-kafka-config'
        for i in range(int(len(config_key_list))):
            key = config_key_list[i]
            value = dedent(config_value_list[i])
            modify_configmap_by_key(namespace, configmap_name, key, value)
        deployment_name = 'realtime-kafka-service'
        # 修改完配置后先停止服务
        # stop_deployment(kubeconfig_path, namespace,deployment_name )
        # 清除KAFKA的消息
        clear_ticket_message()
        clear_cancel_ticket_message()
        clear_undo_ticket_message()
        clear_win_ticket_message()
        clear_paid_ticket_message()
        clear_win_ticket_prize_message()
        # 然后加数据
        proData.insert_into_ticket(current_time)
        proData.insert_into_cancel_ticket(current_time)
        proData.insert_into_undo_ticket(current_time)
        proData.insert_into_win_ticket(current_time)
        proData.insert_into_paid_ticket(current_time)
        proData.insert_into_win_ticket_prize(current_time)
        # 清除KAFKA的消息
        clear_ticket_message()
        clear_cancel_ticket_message()
        clear_undo_ticket_message()
        clear_win_ticket_message()
        clear_paid_ticket_message()
        clear_win_ticket_prize_message()
        # 修改六个key的redis，redis_time是最后推送时间
        # datetime_test = datetime.strptime(redis_time, "%Y-%m-%d %H:%M:%S")
        logger.info(redis_time)
        redis_request.hset(kafkaLastTime,'Ticket', redis_time)
        redis_request.hset(kafkaLastTime,'cancel_ticket', redis_time)
        redis_request.hset(kafkaLastTime,'undo_ticket', redis_time)
        redis_request.hset(kafkaLastTime,'Win_ticket', redis_time)
        redis_request.hset(kafkaLastTime,'zWin_ticket', redis_time)
        redis_request.hset(kafkaLastTime,'Win_ticket_prize', redis_time)
        # 加完数据后重启服务
        restart_deployment(kubeconfig_path, namespace, deployment_name)
    elif data[0] == '循环推送':
        # 获取当前时间 和 当前时间的小时
        current_time, current_hour = comdata.getCurrentTime()
        interval_time = 24
        redis_time = comdata.getRedisTime(current_time, current_hour, data[4])
        redis_init_time = redis_time
        current_init_time = current_time
        logger.info(current_time)
        logger.info(current_hour)
        logger.info(interval_time)
        logger.info(redis_time)
        # data[3]是测试数据未处理的中断重推最大时长
        # 先去修改k8s的配置，data[1]是推送时间间隔,data[2]是kafka一次最多推送N条数据,interval_time是中断重推最大时长
        config_key_list = ['second', 'count', 'intervalTime']
        config_value_list = [str(data[1]), str(data[2]), interval_time]
        # kubeconfig_path = None
        # kubeconfig_path = str(Path(BASE_DIR, "config", "kubeconfig.yaml"))
        namespace = load_client_config(kubeconfig_path)
        configmap_name = 'realtime-kafka-config'
        for i in range(int(len(config_key_list))):
            key = config_key_list[i]
            value = dedent(str(config_value_list[i]))
            modify_configmap_by_key(namespace, configmap_name, key, value)
        deployment_name = 'realtime-kafka-service'
        # 修改完配置后先停止服务
        # stop_deployment(kubeconfig_path, namespace, deployment_name)
        # 清除KAFKA的消息
        clear_ticket_message()
        clear_cancel_ticket_message()
        clear_undo_ticket_message()
        clear_win_ticket_message()
        clear_paid_ticket_message()
        clear_win_ticket_prize_message()
        # 然后加数据
        logger.info(f'current_time插入ticket表的时间{current_time}')
        proData.insert_into_ticket(current_time)
        logger.info(f'current_time插入cancel_ticket表的时间{current_time}')
        proData.insert_into_cancel_ticket(current_time)
        logger.info(f'current_time插入undo_ticket表的时间{current_time}')
        proData.insert_into_undo_ticket(current_time)
        logger.info(f'current_time插入win_ticket表的时间{current_time}')
        proData.insert_into_win_ticket(current_time)
        logger.info(f'current_time插入paid_ticket表的时间{current_time}')
        proData.insert_into_paid_ticket(current_time)
        logger.info(f'current_time插入win_ticket_prize表的时间{current_time}')
        proData.insert_into_win_ticket_prize(current_time)
        # 修改六个key的redis，redis_time是最后推送时间
        logger.info(f'redis_time修改redis中Ticket表的时间{redis_time}')
        redis_request.hset(kafkaLastTime,'Ticket', redis_time)
        logger.info(f'redis_time修改redis中cancel_ticket表的时间{redis_time}')
        redis_request.hset(kafkaLastTime,'cancel_ticket', redis_time)
        logger.info(f'redis_time修改redis中undo_ticket表的时间{redis_time}')
        redis_request.hset(kafkaLastTime,'undo_ticket', redis_time)
        logger.info(f'redis_time修改redis中Win_ticket表的时间{redis_time}')
        redis_request.hset(kafkaLastTime,'Win_ticket', redis_time)
        logger.info(f'redis_time修改redis中zWin_ticket表的时间{redis_time}')
        redis_request.hset(kafkaLastTime,'zWin_ticket', redis_time)
        logger.info(f'redis_time修改redis中Win_ticket_prize表的时间{redis_time}')
        redis_request.hset(kafkaLastTime,'Win_ticket_prize', redis_time)
        # 加完数据后重启服务
        restart_deployment(kubeconfig_path, namespace, deployment_name)
    else:
        # 获取当前时间 和 当前时间的小时
        current_time, current_hour = comdata.getCurrentTime()
        interval_time = 24
        # redis_time = comdata.getRedisTime(current_time, current_hour, data[4])
        redis_time_list = comdata.getPageRedisTimeList(current_time, data[4])
        redis_init_time_list = redis_time_list
        current_init_time = current_time
        logger.info(current_time)
        logger.info(current_hour)
        logger.info(interval_time)
        logger.info(redis_time_list)
        # data[3]是测试数据未处理的中断重推最大时长
        # 先去修改k8s的配置，data[1]是推送时间间隔,data[2]是kafka一次最多推送N条数据,interval_time是中断重推最大时长
        config_key_list = ['second', 'count', 'intervalTime']
        config_value_list = [str(data[1]), str(data[2]), interval_time]
        # kubeconfig_path = None
        # kubeconfig_path = str(Path(BASE_DIR, "config", "kubeconfig.yaml"))
        namespace = load_client_config(kubeconfig_path)
        configmap_name = 'realtime-kafka-config'
        for i in range(int(len(config_key_list))):
            key = config_key_list[i]
            value = dedent(str(config_value_list[i]))
            modify_configmap_by_key(namespace, configmap_name, key, value)
        deployment_name = 'realtime-kafka-service'
        # 修改完配置后先停止服务
        # stop_deployment(kubeconfig_path, namespace, deployment_name)
        # 清除KAFKA的消息
        clear_ticket_message()
        clear_cancel_ticket_message()
        clear_undo_ticket_message()
        clear_win_ticket_message()
        clear_paid_ticket_message()
        clear_win_ticket_prize_message()
        # 然后加数据
        proData.insert_into_ticket_page(current_time)
        proData.insert_into_cancel_ticket_page(current_time)
        proData.insert_into_undo_ticket_page(current_time)
        proData.insert_into_win_ticket_page(current_time)
        proData.insert_into_paid_ticket_page(current_time)
        proData.insert_into_win_ticket_prize_page(current_time)
        # 修改六个key的redis，redis_time是最后推送时间
        redis_request.hset(kafkaLastTime,'Ticket', redis_time_list[0])
        redis_request.hset(kafkaLastTime,'cancel_ticket', redis_time_list[1])
        redis_request.hset(kafkaLastTime,'undo_ticket', redis_time_list[2])
        redis_request.hset(kafkaLastTime,'zWin_ticket', redis_time_list[3])
        redis_request.hset(kafkaLastTime,'Win_ticket', redis_time_list[4])
        redis_request.hset(kafkaLastTime,'Win_ticket_prize', redis_time_list[5])
        # 加完数据后重启服务
        restart_deployment(kubeconfig_path, namespace, deployment_name)
    yield
    proData.delete_from_table()
    # if '分页推送' in data[0]:
    #     # 删除数据
    #     proData.delete_from_ticket_page()
    #     proData.delete_from_cancel_ticket_page()
    #     proData.delete_from_undo_ticket_page()
    #     proData.delete_from_win_ticket_page()
    #     proData.delete_from_paid_ticket_page()
    #     proData.delete_from_win_ticket_prize_page()
    # else:
    #     # 删除数据
    #     proData.delete_from_ticket()
    #     proData.delete_from_cancel_ticket()
    #     proData.delete_from_undo_ticket()
    #     proData.delete_from_win_ticket()
    #     proData.delete_from_paid_ticket()
    #     proData.delete_from_win_ticket_prize()


class TestKafka:

    # 测试用例 1：推送服务中断小于X小时且当天重启
    # @pytest.mark.parametrize("realtime_kafka_fixture", excel_data.get_aslist('testcase', 1, 3, 3), indirect=True)
    # def test_lessinterval_samedayrestart(self,realtime_kafka_fixture):
    #     # 测试用例1：推送服务中断小于X小时且当天重启 flag=1
    #     # 现在前置已经准备好了，现在要去kafka拿数据消费六种数据
    #     logger.info('-------------------- 开始测试...推送服务中断小于X小时且当天重启 --------------------')
    #     asyncio.run(self.bd_data(1,0))
    #     logger.info('-------------------- 测试结束...推送服务中断小于X小时且当天重启 --------------------')
    #     time.sleep(20)
    #
    #
    # # 测试用例 2：推送服务中断小于X小时且跨天重启
    # @pytest.mark.parametrize("realtime_kafka_fixture", excel_data.get_aslist('testcase', 1, 4, 4), indirect=True)
    # def test_lessinterval_diffdayrestart(self, realtime_kafka_fixture):
    #     # 测试用例2：推送服务中断小于X小时且跨天重启
    #     logger.info('-------------------- 开始测试...推送服务中断小于X小时且跨天重启 --------------------')
    #     asyncio.run(self.bd_data(2,0))
    #     logger.info('-------------------- 测试结束...推送服务中断小于X小时且跨天重启 --------------------')
    #     time.sleep(20)
    #
    #
    # # 测试用例 3：推送服务中断大于X小时且当天重启
    # @pytest.mark.parametrize("realtime_kafka_fixture", excel_data.get_aslist('testcase', 1, 5, 5), indirect=True)
    # def test_greaterinterval_samedayrestart(self, realtime_kafka_fixture):
    #     # 测试用例3：推送服务中断大于X小时且当天重启
    #     logger.info('-------------------- 开始测试...推送服务中断大于X小时且当天重启 --------------------')
    #     asyncio.run(self.bd_data(3,0))
    #     logger.info('-------------------- 测试结束...推送服务中断大于X小时且当天重启 --------------------')
    #     time.sleep(20)
    #
    #
    # # 测试用例 4：推送服务中断大于X小时且跨天重启
    # @pytest.mark.parametrize("realtime_kafka_fixture", excel_data.get_aslist('testcase', 1, 6, 6), indirect=True)
    # def test_greaterinterval_diffdayrestart(self, realtime_kafka_fixture):
    #     # 测试用例4：推送服务中断大于X小时且跨天重启
    #     logger.info('-------------------- 开始测试...推送服务中断大于X小时且跨天重启 --------------------')
    #     asyncio.run(self.bd_data(4,0))
    #     logger.info('-------------------- 测试结束...推送服务中断大于X小时且跨天重启 --------------------')
    #     time.sleep(40)
    #
    #
    # # 测试用例 5：推送服务中断等于X小时且当天重启
    # @pytest.mark.parametrize("realtime_kafka_fixture", excel_data.get_aslist('testcase', 1, 7, 7), indirect=True)
    # def test_equalsinterval_samedayrestart(self, realtime_kafka_fixture):
    #     # 测试用例5：推送服务中断等于X小时且当天重启
    #     logger.info('-------------------- 开始测试...推送服务中断等于X小时且当天重启 --------------------')
    #     asyncio.run(self.bd_data(5,0))
    #     logger.info('-------------------- 测试结束...推送服务中断等于X小时且当天重启 --------------------')
    #     time.sleep(60)
    #
    #
    # # 测试用例 6：推送服务中断等于X小时且跨天重启
    # @pytest.mark.parametrize("realtime_kafka_fixture", excel_data.get_aslist('testcase', 1, 8, 8), indirect=True)
    # def test_equalsinterval_diffdayrestart(self, realtime_kafka_fixture):
    #     # 测试用例6：推送服务中断等于X小时且跨天重启    注意
    #     logger.info('-------------------- 开始测试...推送服务中断等于X小时且跨天重启 --------------------')
    #     asyncio.run(self.bd_data(6,0))
    #     logger.info('-------------------- 测试结束...推送服务中断等于X小时且跨天重启 --------------------')
    #     time.sleep(60)


    # 测试用例 7：循环推送
    @pytest.mark.parametrize("realtime_kafka_fixture", excel_data.get_aslist('testcase', 1, 9, 9), indirect=True)
    def test_circle(self, realtime_kafka_fixture):
        # 测试用例7：循环推送
        for i in range(3):
            logger.info(f'第{i+1}次推送......')
            asyncio.run(self.bd_data(7,i))



    # 测试用例 8：分页推送---总条数未满一页
    # @pytest.mark.parametrize("realtime_kafka_fixture", excel_data.get_aslist('testcase', 1, 10, 10), indirect=True)
    # def test_paging_lessone(self, realtime_kafka_fixture):
    #     # 测试用例8：分页推送---总条数未满一页
    #     logger.info('-------------------- 开始测试...分页推送-总条数未满一页 --------------------')
    #     asyncio.run(self.bd_data(8,0))
    #     logger.info('-------------------- 测试结束...分页推送-总条数未满一页 --------------------')
    #
    #
    # # 测试用例 9：分页推送---总条数大于一页非整数
    # @pytest.mark.parametrize("realtime_kafka_fixture", excel_data.get_aslist('testcase', 1, 11, 11), indirect=True)
    # def test_paging_lessone_notint(self, realtime_kafka_fixture):
    #     # 测试用例9：分页推送---总条数大于一页非整数
    #     logger.info('-------------------- 开始测试...分页推送-总条数大于一页非整数 --------------------')
    #     asyncio.run(self.bd_data(9,0))
    #     logger.info('-------------------- 测试结束...分页推送-总条数大于一页非整数 --------------------')
    #     time.sleep(40)
    #
    #
    # # 测试用例 10：分页推送---总条数大于一页正好整数页
    # @pytest.mark.parametrize("realtime_kafka_fixture", excel_data.get_aslist('testcase', 1, 12, 12), indirect=True)
    # def test_paging_lessone_isint(self, realtime_kafka_fixture):
    #     # 测试用例10：分页推送---总条数大于一页正好整数页
    #     logger.info('-------------------- 开始测试...分页推送-总条数大于一页正好整数页 --------------------')
    #     asyncio.run(self.bd_data(10,0))
    #     logger.info('-------------------- 测试结束...分页推送-总条数大于一页正好整数页 --------------------')


    async def bd_data(self,flag,countflag):
        task_bd_ticket = asyncio.create_task(self.bd_ticket(flag, countflag))
        task_bd_cancel_ticket = asyncio.create_task(self.bd_cancel_ticket(flag, countflag))
        task_bd_undo_ticket = asyncio.create_task(self.bd_undo_ticket(flag, countflag))
        task_bd_win_ticket = asyncio.create_task(self.bd_win_ticket(flag, countflag))
        task_bd_paid_ticket = asyncio.create_task(self.bd_paid_ticket(flag, countflag))
        task_bd_win_ticket_prize = asyncio.create_task(self.bd_win_ticket_prize(flag, countflag))
        # await task_bd_ticket
        # await task_bd_cancel_ticket
        # await task_bd_undo_ticket
        await task_bd_win_ticket
        # await task_bd_paid_ticket
        # await task_bd_win_ticket_prize

    async def bd_ticket(self, flag, countflag):
        start_time = ''
        end_time = ''
        if countflag == 0:
        # flag代表执行哪个用例
            if flag == 4 or flag==6:
                start_time =  datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:00'
                # 去查redis发生变化，把值赋给end_time
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('Ticket').decode('utf-8')
                # redis_time_new = REDIS().hget_value('Ticket').encode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime,'Ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('Ticket').decode('utf-8')
                    # redis_time_new = REDIS().hget_value('Ticket').encode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'Ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
            elif flag == 8 or flag == 9 or flag == 10:
                start_time = datetime.strptime(redis_init_time_list[0], '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('Ticket').decode('utf-8')
                # redis_time_new = REDIS().hget_value('Ticket').encode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'Ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time_list[0] == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('Ticket').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'Ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
            else:
                start_time = datetime.strptime(redis_init_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time_new = REDIS().hget_value('Ticket').encode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'Ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time_new = REDIS().hget_value('Ticket').encode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'Ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
        else:
            # redis_time = REDIS().hget_value('Ticket').decode('utf-8')
            # redis_time_new = REDIS().hget_value('Ticket').encode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'Ticket').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            start_time = datetime.strptime(redis_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
            start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            # redis_time_new = REDIS().hget_value('Ticket').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'Ticket').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            while redis_time == redis_time_new:
                await asyncio.sleep(0.5)
                # redis_time_new = REDIS().hget_value('Ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'Ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
            end_time = redis_time

        # 拿到开始时间和结束时间了，然后去库里查，查出来数据内容和数据条数
        logger.info(f'bd_ticket开始时间---{start_time}')
        logger.info(f'bd_ticket结束时间---{end_time}')
        selectResultListList, count = proData.select_from_ticket(start_time, end_time)
        #logger.info(f'bd_ticket自己select出来的数据---{selectResultListList}')
        logger.info(f'bd_ticket自己select出来的条数（加上表头）{count}')
        # 传参数据条数，去kafka里取数据
        data_key_value_list = consume_bd_ticket_message(count)
        data_list = proData.extract_ticket_values_add_title(data_key_value_list)
        data_list_len = len(data_list)
        #logger.info(f'bd_ticket在kafka拿到的的数据{data_list}')
        logger.info(f'bd_ticket在kafka拿到的的条数（加上表头）{data_list_len}')
        # 先比较条数是否正确
        assert count == data_list_len, f'bd_ticket条数错误，应该为{count}条，实际推送了{data_list_len}条'
        # 再比较数据是否一样
        for si in range(int(len(selectResultListList))):
            for sj in range(int(len(selectResultListList[si]))):
                assert selectResultListList[si][sj] == data_list[si][sj], \
                    f'bd_ticket数据错误，第{si + 1}组第{sj + 1}列的数据错误为{data_list[si][sj]}；实际应该为{selectResultListList[si][sj]}'
        logger.info(f'bd_ticket数据比较完成，均正确且符合需求......')

    async def bd_cancel_ticket(self, flag, countflag):
        start_time = ''
        end_time = ''
        logger.info(f'countflag={countflag}')
        if countflag == 0:
        # flag代表执行哪个用例
            if flag == 4 or flag == 6:
                start_time = datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:00'
                end_time = datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:01'
            elif flag == 8 or flag == 9 or flag == 10:
                start_time = datetime.strptime(redis_init_time_list[1], '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('cancel_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'cancel_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time_list[1] == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('cancel_ticket').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'cancel_ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
            else:
                start_time = datetime.strptime(redis_init_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('cancel_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'cancel_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('cancel_ticket').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'cancel_ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
        else:
            # redis_time = REDIS().hget_value('cancel_ticket').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'cancel_ticket').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            start_time = datetime.strptime(redis_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
            start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            # redis_time_new = REDIS().hget_value('cancel_ticket').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'cancel_ticket').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            while redis_time == redis_time_new:
                await asyncio.sleep(0.5)
                # redis_time_new = REDIS().hget_value('cancel_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'cancel_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
            end_time = redis_time
        # 拿到开始时间和结束时间了，然后去库里查，查出来数据内容和数据条数
        logger.info(f'bd_cancel_ticket开始时间{start_time}')
        logger.info(f'bd_cancel_ticket结束时间{end_time}')
        selectResultListList, count = proData.select_from_cancel_ticket(start_time, end_time)
        logger.info(f'bd_cancel_ticket自己select出来的数据{selectResultListList}')
        logger.info(f'bd_cancel_ticket自己select出来的条数（加上表头）{count}')
        # 传参数据条数，去kafka里取数据
        data_key_value_list = consume_bd_cancel_ticket_message(count)
        data_list = proData.extract_cancel_ticket_values_add_title(data_key_value_list)
        data_list_len = len(data_list)
        logger.info(f'bd_cancel_ticket在kafka拿到的的数据{data_list}')
        logger.info(f'bd_cancel_ticket在kafka拿到的的条数（加上表头）{data_list_len}')
        # 先比较条数是否正确
        assert count == data_list_len, f'bd_cancel_ticket条数错误，应该为{count}条，实际推送了{data_list_len}条'
        # 再比较数据是否一样
        for si in range(int(len(selectResultListList))):
            for sj in range(int(len(selectResultListList[si]))):
                assert selectResultListList[si][sj] == data_list[si][sj], \
                    f'数据错误，第{si + 1}组第{sj + 1}列的数据错误为{data_list[si][sj]}；实际应该为{selectResultListList[si][sj]}'
        logger.info(f'bd_cancel_ticket数据比较完成，均正确且符合需求......')

    async def bd_undo_ticket(self, flag, countflag):
        start_time = ''
        end_time = ''
        # flag代表执行哪个用例
        if countflag == 0:
            if flag == 4 or flag == 6:
                start_time = datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:00'
                end_time = datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:01'
            elif flag == 8 or flag == 9 or flag == 10:
                start_time = datetime.strptime(redis_init_time_list[2], '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('undo_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'undo_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time_list[2] == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('undo_ticket').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'undo_ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
            else:
                start_time = datetime.strptime(redis_init_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('undo_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'undo_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('undo_ticket').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'undo_ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
        else:
            # redis_time = REDIS().hget_value('undo_ticket').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'undo_ticket').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            start_time = datetime.strptime(redis_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
            start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            # redis_time_new = REDIS().hget_value('undo_ticket').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'undo_ticket').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            while redis_time == redis_time_new:
                await asyncio.sleep(0.5)
                # redis_time_new = REDIS().hget_value('undo_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'undo_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
            end_time = redis_time
        # 拿到开始时间和结束时间了，然后去库里查，查出来数据内容和数据条数
        logger.info(f'bd_undo_ticket开始时间{start_time}')
        logger.info(f'bd_undo_ticket结束时间{end_time}')
        selectResultListList, count = proData.select_from_undo_ticket(start_time, end_time)
        logger.info(f'bd_undo_ticket自己select出来的数据{selectResultListList}')
        logger.info(f'bd_undo_ticket自己select出来的条数（加上表头）{count}')
        # 传参数据条数，去kafka里取数据
        data_key_value_list = consume_bd_undo_ticket_message(count)
        data_list = proData.extract_undo_ticket_values_add_title(data_key_value_list)
        data_list_len = len(data_list)
        logger.info(f'bd_undo_ticket在kafka拿到的的数据{data_list}')
        logger.info(f'bd_undo_ticket在kafka拿到的的条数（加上表头）{data_list_len}')
        # 先比较条数是否正确
        assert count == data_list_len, f'bd_undo_ticket条数错误，应该为{count}条，实际推送了{data_list_len}条'
        # 再比较数据是否一样
        for si in range(int(len(selectResultListList))):
            for sj in range(int(len(selectResultListList[si]))):
                assert selectResultListList[si][sj] == data_list[si][sj], \
                    f'bd_undo_ticket数据错误，第{si + 1}组第{sj + 1}列的数据错误为{data_list[si][sj]}；实际应该为{selectResultListList[si][sj]}'
        logger.info(f'bd_undo_ticket数据比较完成，均正确且符合需求......')

    async def bd_win_ticket(self, flag, countflag):
        start_time = ''
        end_time = ''
        if countflag == 0:
            # flag代表执行哪个用例
            if flag == 4 or flag == 6:
                start_time = datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:00'
                end_time = datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:01'
            elif flag == 8 or flag == 9 or flag == 10:
                start_time = datetime.strptime(redis_init_time_list[3], '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('zWin_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'zWin_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time_list[3] == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('zWin_ticket').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'zWin_ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
            else:
                start_time = datetime.strptime(redis_init_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time

                # redis_time = REDIS().hget_value('zWin_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'zWin_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                logger.info(f'11redis_time{redis_time}')
                logger.info(f'11redis_init_time{redis_init_time}')
                while redis_init_time == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('zWin_ticket').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'zWin_ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
        else:
            # redis_time = REDIS().hget_value('zWin_ticket').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'zWin_ticket').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            start_time = datetime.strptime(redis_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
            start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            # redis_time_new = REDIS().hget_value('zWin_ticket').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'zWin_ticket').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            while redis_time == redis_time_new:
                await asyncio.sleep(0.5)
                # redis_time_new = REDIS().hget_value('zWin_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'zWin_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
            end_time = redis_time
        # 拿到开始时间和结束时间了，然后去库里查，查出来数据内容和数据条数
        logger.info(f'bd_win_ticket开始时间{start_time}')
        logger.info(f'bd_win_ticket结束时间{end_time}')
        selectResultListList, count = proData.select_from_win_ticket(start_time, end_time)
        logger.info(f'bd_win_ticket自己select出来的数据{selectResultListList}')
        logger.info(f'bd_win_ticket自己select出来的条数（加上表头）{count}')
        # 传参数据条数，去kafka里取数据
        data_key_value_list = consume_bd_win_ticket_message(count)
        data_list = proData.extract_win_ticket_values_add_title(data_key_value_list)
        data_list_len = len(data_list)
        logger.info(f'bd_win_ticket在kafka拿到的的数据{data_list}')
        logger.info(f'bd_win_ticket在kafka拿到的的条数（加上表头）{data_list_len}')
        # 先比较条数是否正确
        assert count == data_list_len, f'bd_win_ticket条数错误，应该为{count}条，实际推送了{data_list_len}条'
        # 再比较数据是否一样
        for si in range(int(len(selectResultListList))):
            for sj in range(int(len(selectResultListList[si]))):
                assert selectResultListList[si][sj] == data_list[si][sj], \
                    f'bd_win_ticket数据错误，第{si + 1}组第{sj + 1}列的数据错误为{data_list[si][sj]}；实际应该为{selectResultListList[si][sj]}'
        logger.info(f'bd_win_ticket数据比较完成，均正确且符合需求......')

    async def bd_paid_ticket(self, flag, countflag):
        start_time = ''
        end_time = ''
        if countflag == 0:
            # flag代表执行哪个用例
            if flag == 4 or flag == 6:
                start_time = datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:00'
                end_time = datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:01'
            elif flag == 8 or flag == 9 or flag == 10:
                start_time = datetime.strptime(redis_init_time_list[4], '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('Win_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time_list[4] == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('Win_ticket').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
            else:
                start_time = datetime.strptime(redis_init_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('Win_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('Win_ticket').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
        else:
            # redis_time = REDIS().hget_value('Win_ticket').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            start_time = datetime.strptime(redis_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
            start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            # redis_time_new = REDIS().hget_value('Win_ticket').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            while redis_time == redis_time_new:
                await asyncio.sleep(0.5)
                # redis_time_new = REDIS().hget_value('Win_ticket').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
            end_time = redis_time
        # 拿到开始时间和结束时间了，然后去库里查，查出来数据内容和数据条数
        logger.info(f'bd_paid_ticket开始时间{start_time}')
        logger.info(f'bd_paid_ticket结束时间{end_time}')
        selectResultListList, count = proData.select_from_paid_ticket(start_time, end_time)
        logger.info(f'bd_paid_ticket自己select出来的数据{selectResultListList}')
        logger.info(f'bd_paid_ticket自己select出来的条数（加上表头）{count}')
        # 传参数据条数，去kafka里取数据
        data_key_value_list = consume_bd_paid_ticket_message(count)
        data_list = proData.extract_paid_ticket_values_add_title(data_key_value_list)
        data_list_len = len(data_list)
        logger.info(f'bd_paid_ticket在kafka拿到的的数据{data_list}')
        logger.info(f'bd_paid_ticket在kafka拿到的的条数（加上表头）{data_list_len}')
        # 先比较条数是否正确
        assert count == data_list_len, f'bd_paid_ticket条数错误，应该为{count}条，实际推送了{data_list_len}条'
        # 再比较数据是否一样
        for si in range(int(len(selectResultListList))):
            for sj in range(int(len(selectResultListList[si]))):
                assert selectResultListList[si][sj] == data_list[si][sj], \
                    f'数据错误，第{si + 1}组第{sj + 1}列的数据错误为{data_list[si][sj]}；实际应该为{selectResultListList[si][sj]}'
        logger.info(f'bd_paid_ticket数据比较完成，均正确且符合需求......')

    async def bd_win_ticket_prize(self, flag, countflag):
        start_time = ''
        end_time = ''
        if countflag == 0:
            # flag代表执行哪个用例
            if flag == 4 or flag == 6:
                start_time = datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:00'
                end_time = datetime.strptime(current_init_time, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d') + ' 00:00:01'
            elif flag == 8 or flag == 9 or flag == 10:
                start_time = datetime.strptime(redis_init_time_list[5], '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('Win_ticket_prize').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket_prize').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time_list[5] == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('Win_ticket_prize').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket_prize').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
            else:
                start_time = datetime.strptime(redis_init_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
                start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
                # 去查redis发生变化，把值赋给end_time
                # redis_time = REDIS().hget_value('Win_ticket_prize').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket_prize').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
                while redis_init_time == redis_time:
                    await asyncio.sleep(0.5)
                    # redis_time = REDIS().hget_value('Win_ticket_prize').decode('utf-8')
                    redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket_prize').encode('utf-8')
                    redis_time = redis_time_new.decode('utf-8')
                end_time = redis_time
        else:
            # redis_time = REDIS().hget_value('Win_ticket_prize').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket_prize').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            start_time = datetime.strptime(redis_time, '%Y-%m-%d %H:%M:%S') + timedelta(seconds=1)
            start_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            # redis_time_new = REDIS().hget_value('Win_ticket_prize').decode('utf-8')
            redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket_prize').encode('utf-8')
            redis_time = redis_time_new.decode('utf-8')
            while redis_time == redis_time_new:
                await asyncio.sleep(0.5)
                # redis_time_new = REDIS().hget_value('Win_ticket_prize').decode('utf-8')
                redis_time_new = redis_request.hget(kafkaLastTime, 'Win_ticket_prize').encode('utf-8')
                redis_time = redis_time_new.decode('utf-8')
            end_time = redis_time
        # 拿到开始时间和结束时间了，然后去库里查，查出来数据内容和数据条数
        logger.info(f'bd_win_ticket_prize开始时间{start_time}')
        logger.info(f'bd_win_ticket_prize结束时间{end_time}')
        selectResultListList, count = proData.select_from_win_ticket_prize(start_time, end_time)
        logger.info(f'bd_win_ticket_prize自己select出来的数据{selectResultListList}')
        logger.info(f'bd_win_ticket_prize自己select出来的条数（加上表头）{count}')
        # 传参数据条数，去kafka里取数据
        data_key_value_list = consume_bd_win_ticket_prize_message(count)
        data_list = proData.extract_win_ticket_prize_values_add_title(data_key_value_list)
        data_list_len = len(data_list)
        logger.info(f'bd_win_ticket_prize在kafka拿到的的数据{data_list}')
        logger.info(f'bd_win_ticket_prize在kafka拿到的的条数（加上表头）{data_list_len}')
        # 先比较条数是否正确
        assert count == data_list_len, f'bd_win_ticket_prize条数错误，应该为{count}条，实际推送了{data_list_len}条'
        # 再比较数据是否一样
        for si in range(int(len(selectResultListList))):
            for sj in range(int(len(selectResultListList[si]))):
                assert selectResultListList[si][sj] == data_list[si][sj], \
                    f'数据错误，第{si + 1}组第{sj + 1}列的数据错误为{data_list[si][sj]}；实际应该为{selectResultListList[si][sj]}'
        logger.info(f'bd_win_ticket_prize数据比较完成，均正确且符合需求......')


