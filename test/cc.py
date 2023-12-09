from kafka import TopicPartition, KafkaConsumer
import json
from common.logger import logger


consumer = KafkaConsumer(bootstrap_servers='192.168.24.213:31050',auto_offset_reset='earliest',
                         enable_auto_commit=False,group_id='test', consumer_timeout_ms = 5000)
consumer.assign([TopicPartition('bd_cancel_ticket', 0)])


count = 300
t_count = 0
data_list = []


for message in consumer:
    temp_list = []
    temp_count = 0
    m_json = json.loads(message.value.decode('utf-8'))
    a=message.value
    logger.info(m_json)
    consumer.commit()

    # m_json_len = len(m_json)
    # if m_json_len > 1:
    #     temp_count = len(m_json[m_json_len - 1]['data']) + (m_json_len - 1) * 100
    #     for i in range(m_json_len):
    #         temp_list.extend(m_json[i]['data'])
    # else:
    #     temp_count += len(m_json[0]['data'])
    #     temp_list.extend(m_json[0]['data'])
    # # 然后判断这一轮拿到的数据是否重复，不重复就把它放一起
    # if all(x in data_list for x in temp_list):
    #     print("temp_list包含在data_list中")
    # else:
    #     print("temp_list不包含在data_list中")
    #     data_list.extend(temp_list)
    #     t_count += temp_count
#     # 在这里提交已消费过
#     consumer.commit()
#     # if t_count == count:
#     #     # return data_list
#     #     print(data_list)
#     #     break
#     # elif t_count > count:
#     #     print("数据有误")
#     #     break
#
print(data_list)





