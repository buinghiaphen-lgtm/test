# import os
# from pathlib import Path
# from setting import TEST_DATA_PATH,BASE_DIR
# import time
# from common.k8sops import load_client_config, modify_configmap_by_key, restart_deployment
#
# from kubernetes import client, config
#
#
# kubeconfig_path = str(Path(BASE_DIR, "config", "kubeconfig.yaml"))
# # 指定配置文件路径
# config.load_kube_config(config_file=kubeconfig_path)
#
# # 创建 Kubernetes API 客户端
# v1 = client.AppsV1Api()
# # v2 = client.CoreV1Api()
# # 指定命名空间
# # namespace = 'default'
# namespace = load_client_config(kubeconfig_path)
#
# # 指定 Deployments 名称列表
# deployment_names = ['realtime-kafka-service']
#
# # 重启 Deployment
# print('正在重启 Deployment...')
# for deploy in deployment_names:
#     print('重启的Deployment-------------')
#     print(deploy)
# for deployment_name in deployment_names:
#     v1.patch_namespaced_deployment(deployment_name, namespace, body={'spec': {'replicas': 0}})
#     time.sleep(3)
#     v1.patch_namespaced_deployment(deployment_name, namespace, body={'spec': {'replicas': 1}})
#
# # # 获取 Pod 列表
# # print('获取 Pod 列表...')
# # for namespace in ["ns-test"]:
# #     ret = v2.list_namespaced_pod(namespace=namespace, watch=False)
# #     for i in ret.items:
# #         print("%s   \t%s \t%s \t%s        \t%s" %
# #               (i.status.host_ip, i.status.pod_ip, i.status.phase, i.metadata.namespace, i.metadata.name))
from datetime import datetime, timedelta

current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

current_date = datetime.strptime(current_time, '%Y-%m-%d %H:%M:%S')
print(current_time)
origdata = "10sec,10min,10min,40min,10sec,600sec"

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
print(data_list)
