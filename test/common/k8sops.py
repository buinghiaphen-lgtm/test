import time
import logging
from os.path import exists, expanduser
from kubernetes import client, config, watch
from kubernetes.client.rest import ApiException
from urllib3.util import connection
from urllib3.exceptions import HTTPError


log = logging.getLogger(__name__)

API_SERVER_IP = '192.168.24.191'
DEFAULT_REQUEST_TIMEOUT = 30
_orig_create_connection = connection.create_connection

def patched_create_connection(address, *args, **kwargs):
    hostname, port = address
    host = API_SERVER_IP if hostname == 'lb.kubesphere.local' else hostname
    return _orig_create_connection((host, port), *args, **kwargs)

connection.create_connection = patched_create_connection


def load_client_config(local_config_file=None, **kwargs):
    """
    加载K8s客户端配置

    :param local_config_file: kubeconfig配置文件路径

    :return: 配置文件中的的namespace
    """
    namespace = None
    if local_config_file:
        kwargs['config_file'] = local_config_file
        _, current_context = config.list_kube_config_contexts(local_config_file)
        namespace = current_context['context']['namespace']
    elif exists(expanduser(config.KUBE_CONFIG_DEFAULT_LOCATION)):
        _, current_context = config.list_kube_config_contexts(local_config_file)
        namespace = current_context['context']['namespace']
    else:
        with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace') as f:
            namespace = f.read()
    config.load_config(**kwargs)
    return namespace


def modify_configmap_by_key(namespace, cmname, key, value):
    """
    修改配置字典

    :param namespace: 项目名称
    :param cmname: 配置字典名称
    :param key: 配置字典key
    :param cmname: 配置字典value
    """
    v1 = client.CoreV1Api()
    body = {'data': {key: value}}
    # body = [{'op': 'replace', 'path': '/data', 'value': {key: content}}]
    v1.patch_namespaced_config_map(cmname, namespace, body, _request_timeout=DEFAULT_REQUEST_TIMEOUT)
    log.info('modify configmap %s/%s successful', namespace, cmname)


def wait_job_completed(namespace, jobname, resource_version, request_timeout):
    """
    等待任务执行完成

    :param namespace: 项目名称
    :param jobname: 任务名称
    :param resource_version: 资源版本
    :param request_timeout: 等待任务完成的超时时间
    :return: str
             completed 任务完成, failed 任务失败, unknown 任务状态未知或超时
    """
    log.info("waiting for job %s/%s to complete", namespace, jobname)
    v1 = client.BatchV1Api()
    max_retry = 300
    for _ in range(max_retry):
        try:
            jobstatus = 'unknown'
            count = 10
            w = watch.Watch()
            log.info('watch job/%s resource_version = %s', jobname, resource_version)
            for event in w.stream(v1.list_namespaced_job,
                                  namespace=namespace,
                                #   label_selector=','.join(k + '=' + v for k, v in labels.items()),
                                  field_selector='metadata.name=' + jobname,
                                  resource_version=resource_version,
                                  timeout_seconds=request_timeout,
                                  _request_timeout=request_timeout):
                s = event['object'].status

                if s.conditions:
                    w.stop()
                    if any(x.status == 'True' and x.type == 'Complete' for x in s.conditions):
                        jobstatus = 'completed'
                    else:
                        log.error('%s', s.conditions)
                        jobstatus = 'failed'

                count -= 1
                if not count:
                    w.stop()

            log.info('job %s/%s is %s', namespace, jobname, jobstatus)
            return jobstatus
        except ApiException as ex:
            if ex.status == watch.watch.HTTP_STATUS_GONE:
                log.warning('Got `410 Gone`, retry again. ApiException Exception: %s', repr(ex))
                resource_version = None
                break
            raise
        except HTTPError as ex:
            log.warning('An error occurred in the request, retry again. HTTPError Exception: %s', repr(ex))
    else: # pylint: disable=useless-else-on-loop
        return 'unknown'


def run_job(namespace, jobname):
    """
    运行任务

    :param namespace: 项目名称
    :param jobname: 任务名称

    :return: V1Job
    """
    v1 = client.BatchV1Api()
    job: client.V1Job = v1.read_namespaced_job(jobname, namespace, _request_timeout=DEFAULT_REQUEST_TIMEOUT)

    job.metadata.resource_version = ''
    job.status = client.V1JobStatus()
    job.metadata.uid = ''
    if 'revisions' in job.metadata.annotations:
        revisions: str = job.metadata.annotations['revisions']
        job.metadata.annotations['revisions'] = revisions.replace('running', 'unfinished')
    del job.spec.selector.match_labels['controller-uid']
    del job.spec.template.metadata.labels['controller-uid']

    v1.delete_namespaced_job(jobname, namespace, _request_timeout=DEFAULT_REQUEST_TIMEOUT)
    exception: Exception = None
    ret = None
    for _ in range(6):
        try:
            ret = v1.create_namespaced_job(namespace, job, _request_timeout=DEFAULT_REQUEST_TIMEOUT)
        except ApiException as ex:
            exception = ex
            time.sleep(1)
        else:
            exception = None
            break

    if exception:
        raise exception # pylint: disable=raising-bad-type

    log.info('run job %s/%s successful', namespace, jobname)
    return ret




def restart_deployment(kubeconfig_path, namespace, deployment_name):
    # 指定配置文件路径
    config.load_kube_config(config_file=kubeconfig_path)
    # 创建 Kubernetes API 客户端
    v1 = client.AppsV1Api()
    # v1.patch_namespaced_deployment(deployment_name, namespace, body={'spec': {'replicas': 0}})
    # time.sleep(3)
    v1.patch_namespaced_deployment(deployment_name, namespace, body={'spec': {'replicas': 1}})
    time.sleep(5)
    # 指定要重启的Deployment的名称和命名空间
    # namespace = "default" # 或者是你的deployment所在的其他命名空间

def stop_deployment(kubeconfig_path, namespace, deployment_name):
    # 指定配置文件路径
    config.load_kube_config(config_file=kubeconfig_path)
    # 创建 Kubernetes API 客户端
    v1 = client.AppsV1Api()
    v1.patch_namespaced_deployment(deployment_name, namespace, body={'spec': {'replicas': 0}})
    time.sleep(5)
    # 指定要重启的Deployment的名称和命名空间
    # namespace = "default" # 或者是你的deployment所在的其他命名空间