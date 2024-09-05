"""
K8S相关操作，使用前需要判断KUBE_CONFIG_FILE是否存在
"""
import base64
import logging
import time
from datetime import datetime, timedelta

from kubernetes import client, config

from setting import KUBE_CONFIG_FILE, NAMESPACE

logger = logging.getLogger(__name__)
if KUBE_CONFIG_FILE.exists():
    config.load_kube_config(str(KUBE_CONFIG_FILE))


def read_configmap(name: str, keys: list | tuple) -> dict:
    """读取配置字典configmap的配置

    Args:
        name (str): _名称
        keys (list, tuple): _key的列表

    Returns:
        dict: _配置和值
    """
    if isinstance(keys, str):
        keys = (keys,)
    try:
        v1 = client.CoreV1Api()
        c_dict = v1.read_namespaced_config_map(name, NAMESPACE).data
        conf_dict = {k: c_dict[k] for k in keys}
    except Exception as e:
        logger.error(e)
    return conf_dict


def read_secret(name: str, keys: list | tuple) -> dict:
    """读取保密字典Secret的配置

    Args:
        name (str): _名称
        keys (list, tuple): _key的列表

    Returns:
        dict: _配置和值
    """
    if isinstance(keys, str):
        keys = (keys,)
    v1 = client.CoreV1Api()
    try:
        c_dict = v1.read_namespaced_secret(name, NAMESPACE).data
        conf_dict = {k: base64.b64decode(
            c_dict[k]).decode('utf-8') for k in keys}
    except Exception as e:
        logger.error(e)
        conf_dict = None
    return conf_dict


def modify_configmap(name: str, new_config: dict[str, str], force_rebuild: bool = False):
    """修改configmap的配置

    Args:
        name (str): _名称
        new_config (dict): _想要修改的配置项键值对
        force_rebuild (dict): _默认只重新创建配置发生变化的工作负载，为True时，使用配置的工作负载均会重新创建。
    """
    v1 = client.CoreV1Api()
    origin_config = read_configmap(name, new_config.keys())
    modify_config = {k: str(v) for k, v in new_config.items() if force_rebuild or (str(v) !=
                     origin_config[k])}
    if not modify_config:
        print(f'{name}配置{new_config}未变更，不进行修改')
        return
    body = {'data': modify_config}
    print(f'修改{name}配置：{modify_config}')
    v1.patch_namespaced_config_map(name, NAMESPACE, body)
    appv1 = client.AppsV1Api()
    dl = appv1.list_namespaced_deployment(namespace=NAMESPACE, watch=False)
    restart_list = []
    utc_time = (datetime.now()-timedelta(hours=8)
                ).strftime('%Y-%m-%dT%H:%M:%S.%fZ')
    body = {"spec": {"template": {"metadata": {"annotations": {
        "kubesphere.io/restartedAt": utc_time}}}}}
    for d in dl.items:
        for env in d.spec.template.spec.containers[0].env:
            if env.value_from and env.value_from.config_map_key_ref and env.value_from.config_map_key_ref.name == name and env.value_from.config_map_key_ref.key in modify_config.keys():
                restart_list.append(appv1.patch_namespaced_deployment(
                    d.metadata.name, NAMESPACE, body=body, async_req=True))
                print(f'开始重新创建工作负载{d.metadata.name}')
                break
    sleep = False  # 根据工作负载有没有探针判断是否需要等待
    time.sleep(1)
    for r in restart_list:
        result = r.get()
        retry = 120
        while retry > 0:
            deployment = appv1.read_namespaced_deployment(
                name=result.metadata.name, namespace=NAMESPACE)
            d_container = deployment.spec.template.spec.containers[0]
            pods = v1.list_namespaced_pod(namespace=NAMESPACE,
                                          label_selector=f"app={result.metadata.name}")
            ready = True
            for pod in pods.items:
                for condition in pod.status.conditions:
                    if condition.status != "True":
                        ready = False
                        break
                if not ready:
                    break
            if not ready or deployment.status.replicas != deployment.status.ready_replicas:
                time.sleep(1)
                retry -= 1
                continue
            if not (d_container.liveness_probe or d_container.readiness_probe):
                sleep = True
            break
        else:
            logger.error(f'{result.metadata.name}工作负载重启失败')
    if sleep:
        time.sleep(5)


def suspend_all_cronjob(suspend: bool):
    """暂停或恢复所有定时任务

    Args:
        suspend (bool): _是否暂停
    """
    bapi = client.BatchV1Api()
    response = bapi.list_namespaced_cron_job(NAMESPACE).items
    threads = []
    for item in response:
        if item.spec.suspend == suspend:
            continue
        threads.append(bapi.patch_namespaced_cron_job(item.metadata.name, NAMESPACE, {
            "spec": {"suspend": suspend}}, async_req=True))
    # for t in threads:
    #     t.get()
