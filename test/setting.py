from pathlib import Path
import os

# 文件项目根目录
BASE_DIR = Path(__file__).parent.resolve()
# 测试数据路径
TEST_DATA_PATH = Path(BASE_DIR, 'data', '测试数据-实时推送-kafka工具.xlsx')

# REDIS相关配置
host_list = [
    ('realtime-redis-node-0.realtime-redis-headless.ns-test-pub',26379),
    ('realtime-redis-node-1.realtime-redis-headless.ns-test-pub',26379),
    ('realtime-redis-node-2.realtime-redis-headless.ns-test-pub',26379)
]
# redis调试
# host_list = [
#     ('192.168.24.213',30033)
# ]
REDIS_HOST = os.getenv('REDIS_HOST') or host_list
# REDIS_PORT = os.getenv('REDIS_PORT') or 30008
REDIS_DB = os.getenv('REDIS_DB') or 0
REDIS_HNAME = os.getenv('REDIS_HNAME') or 'kafkaLastTime'


# KAFKA相关配置
KAFKA_TNAME = []
# KAFKA_SERVER = os.getenv('KAFKA_SERVER') or '192.168.24.213:30014'
# KAFKA_SERVER = os.getenv('KAFKA_SERVER') or 'my-cluster-kafka-bootstrap.ns-national:9092'
# 调试用
# KAFKA_SERVER = os.getenv('KAFKA_SERVER') or '192.168.24.213:30041,192.168.24.213:30042,192.168.24.213:30043'
# KAFKA_SERVER = '192.168.24.213:31155'
# 内网地址
KAFKA_SERVER = os.getenv('KAFKA_SERVER') or 'national-mock-kafka.ns-stage-mock:9092'

# 数据库连接地址
DB_HOST = os.getenv('DB_HOST') or '192.168.24.200'
DB_PORT = os.getenv('DB_PORT') or 3390
DB_USER = os.getenv('DB_USER') or 'v2t'
DB_PASSWORD = os.getenv('DB_PASSWORD') or 'Vegas2.0'
DB_NAME = os.getenv('DB_NAME') or 'test_vegas2'

# DB_HOST = os.getenv('DB_HOST') or '192.168.24.200'
# DB_PORT = os.getenv('DB_PORT') or 3390
# DB_USER = os.getenv('DB_USER') or 'v2t_debug'
# DB_PASSWORD = os.getenv('DB_PASSWORD') or 'Vegas2.0'
# DB_NAME = os.getenv('DB_NAME') or 'debug_vegas2'