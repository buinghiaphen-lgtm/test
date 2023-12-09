from pathlib import Path
import os

# 文件项目根目录
BASE_DIR = Path(__file__).parent.resolve()
# 测试数据路径
TEST_DATA_PATH = Path(BASE_DIR, 'data', '测试数据-实时推送-kafka工具.xlsx')

# REDIS相关配置
REDIS_HOST = '192.168.24.213'
# REDIS_PORT = 56322
REDIS_PORT = 32385
REDIS_DB = 0
REDIS_HNAME = 'kafkaLastTime'

# KAFKA相关配置
KAFKA_TNAME = []
KAFKA_SERVER = '192.168.24.213:31050'

# MYSQL相关配置
DB_HOST = '192.168.24.200'
DB_PORT = 3390
DB_USER = 'v2t_debug'
DB_PASSWORD = 'Vegas2.0'
DB_NAME = 'debug_vegas2'