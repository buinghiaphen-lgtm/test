# from kafka import KafkaConsumer
#
# # 创建 KafkaConsumer 对象
# consumer = KafkaConsumer( bootstrap_servers='192.168.24.213:31050')
# # ,auto_offset_reset='earliest'
# print(consumer.topics())
# consumer.subscribe(topics=['quickstart-events'])
# consumer.poll()
# # 持续监听消息
# # 启动服务，让杨震重新推，看看是不是获取实时的
# for message in consumer:
#     print(message)
#     print(message.value)
#
# # 关闭 KafkaConsumer
# consumer.close()
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import kafka_errors
import traceback
import json
def producer_demo():
    # 假设生产的消息为键值对（不是一定要键值对），且序列化方式为json
    producer = KafkaProducer(
        bootstrap_servers=['192.168.24.213:31050'])
    # 发送三条消息
    for i in range(0, 3):
        future = producer.send(
            'wll',
            key='count_num',  # 同一个key值，会被送至同一个分区
            value=str(i))  # 向分区1发送消息
        print("send {}".format(str(i)))
        try:
            future.get(timeout=10) # 监控是否发送成功
        except kafka_errors:  # 发送失败抛出kafka_errors
            traceback.format_exc()

if __name__ == '__main__':
    producer_demo()