from threading import Thread
import importlib
import json
import time
from json import JSONDecodeError
from worker_base import DecisionMaker, DataLoader
import logging
logger = logging.getLogger(__name__)

from utils import RedisMaxin, load_node, load_operator, load_threadPod, load_graph, record_cost_time


class ThreadPod(Thread, RedisMaxin):

    def __init__(self, init_info):
        Thread.__init__(self)

        self.heartbeat_interval = init_info.get('heartbeat_interval', 5)
        self.name = self.redis_queue = init_info.get('redis_queue')
        self.modules = []
        for m in init_info.get("operators"):
            worker = load_operator(m)
            package = "workers." + ".".join(worker.get('worker_class').split(".")[:-1])
            work_class = worker.get('worker_class').split(".")[-1]
            module = importlib.import_module(package)
            module = getattr(module, work_class)
            operator = module({"name": worker['name']})
            self.modules.append(operator)

        self.upstreams = init_info.get('upstreams')
        if self.upstreams:
            self.upstreams_len = len(self.upstreams)
            self.upstream_hash_key = "upstream_"+self.name

    def read_task(self):
        while True:
            tasks = self.r.blpop(self.redis_queue, timeout=1)
            if tasks is None:
                continue
            # todo send I am idle singal!
            key, message = tasks
            try:
                message = json.loads(message)
            except JSONDecodeError:
                logger.info(message)
                message = {"error": "JSONDecodeError"}
                return message

            if not self.upstreams:
                return message
            task_id = message.get("task_id")
            from_node = message.get("from")
            if (not from_node) or (from_node not in self.upstreams):
                return {"error": "noupstreams"}
            else:
                current_cnt = self.r.hincrby(self.upstream_hash_key, task_id, 1)
                if current_cnt == self.upstreams_len:
                    self.r.hdel(self.upstream_hash_key, task_id)
                    return message
                else:
                    continue




    def send_result(self, task_result):
        result = json.dumps(task_result)
        self.r.rpush(self.redis_reply_key, result)

    def run(self):
        while True:
            message = self.read_task()
            command_type = message.get("command_type", 'work')
            if command_type == 'exit':
                # print(self.name, 'run, exit')
                break

            task_id = message.get('task_id')
            result = {
                "task_id": task_id,
                "pod_id": self.name,
            }

            if not task_id:
                result.update({"result": "exception"})
                self.send_result(result)
                continue

            logger.info("[threadPod] [%s] task_id: [%s], [%s]", self.redis_queue, task_id, message)
            start_time = time.time()
            for operator in self.modules:
                try:
                    work_result = operator.run(task_id)
                except Exception as e:
                    logger.error(e)
                    work_result = {"result": "exception"}
                    break
            end_time = time.time()
            record_cost_time(self.redis_queue, round(end_time-start_time, 4))
            if isinstance(operator, DataLoader) and (work_result is None):
                work_result = {"result": "done"}

            result.update(work_result)
            self.send_result(result)


class Node:

    def __init__(self, node_id):
        node_config = load_node(node_id)
        self.id = node_config.get('id')
        self.name = node_config.get('name')
        self.pods_config = node_config.get('thread_pods')
        self.upstreams = node_config.get('upstreams')

        self.heartbeat_interval = node_config.get('heartbeat_interval', 5)
        self.name = self.redis_queue = node_config.get('redis_queue')
        self.modules = []
        for m in node_config.get("operators"):
            worker = load_operator(m)
            package = "workers." + ".".join(worker.get('worker_class').split(".")[:-1])
            work_class = worker.get('worker_class').split(".")[-1]
            module = importlib.import_module(package)
            module = getattr(module, work_class)
            operator = module({"name": worker['name']})
            self.modules.append(operator)

        self.upstreams = node_config.get('upstreams')
        if self.upstreams:
            self.upstreams_len = len(self.upstreams)
            self.upstream_hash_key = "upstream_"+self.name


        # 生成pods
        self.pods = []
        for pod_id, pod_config in self.pods_config.items():
            pod_num = pod_config.get("defalut_num", 1)
            self.pods += [self.make_thread_pod(pod_id) for i in range(pod_num)]
        # 生成下一个pod 的字典
        self.pod_redis_name = [",".join([self.id, pod_id]) for pod_id in self.pods_order]
        k_pod = self.pod_redis_name.copy()
        k_pod.pop()
        v_pod = self.pod_redis_name.copy()
        v_pod.pop(0)
        self.next_pod = {k: v for k, v in zip(k_pod, v_pod)}

    def get_next_threadpod(self, queue_key):
        """
        获取下一个pod . 如果传入是nodeID 则说明是第一个.
        如果返回的是最后一个, 则返回None
        :param queue_key:
        :return:
        """
        if queue_key == self.id:
            return self.pod_redis_name[0]
        return self.next_pod.get(queue_key)

    def make_thread_pod(self, pod_id):
        """
        生成pod
        :param pod_id:
        :return:
        """
        pod_info = dict()
        redis_queue = ",".join([self.id, pod_id])
        pod_info['redis_queue'] = redis_queue
        pod_info['operators'] = self.pods_config[pod_id]['operators']
        if self.upstreams and len(self.upstreams) > 1 and self.pods_order[0] == pod_id:
            pod_info['upstreams'] = self.upstreams
        else:
            pod_info['upstreams'] = False
        return ThreadPod(pod_info)

    def run(self):
        for p in self.pods:
            p.start()
            # todo emit singal tell thread had start


class Graph(RedisMaxin, Thread):

    def __init__(self, graph_id):
        Thread.__init__(self)
        graph_config = load_graph(graph_id)
        self.name = graph_config.get('name')
        self.redis_graph = "graph_{}".format(self.name)
        self.graph = graph_config.get("graph")
        self.nodes = dict()
        self.nodes_info = self.load_nodes()

        for n in self.nodes_info:
            node_id = n['id']
            node = Node(node_id)
            self.nodes[node_id] = node

    def get_node_by_id(self, node_id):
        return self.nodes[node_id]

    def load_nodes(self):
        nodes = []
        for n in self.graph:
            node = load_node(n)
            nodes.append(node)
        return nodes

    def start_nodes(self):
        for node_id, node in self.nodes.items():
            node.run()

    def dispense_to_downstream(self, nodes_id, task):
        for n in nodes_id:
            node = self.nodes[n]
            next_pod_key = node.get_next_threadpod(n)
            self.send_task_info(next_pod_key,task)

    def read_reply_info(self):
        result = self.r.blpop(self.redis_graph, 0)
        if result is None:
            return None
        key, message = result
        try:
            message = json.loads(message)
        except JSONDecodeError:
            message = {"error": "JSONDecodeError"}
        return message

    def send_task_info(self, queue_key, task_info):
        result = json.dumps(task_info)
        self.r.rpush(queue_key, result)

    def handle_exception(self,pod_id,exception_info):
        pass

    def get_next_queue(self, pod_id):
        node_id, pod = pod_id.split(",", 1)
        node = self.nodes.get(node_id)
        next_pod_key = node.get_next_threadpod(pod_id)
        return next_pod_key

    def get_downstreams(self,pod_id,option):
        node_id, pod = pod_id.split(",", 1)
        node_config = self.graph.get(node_id)

        if node_config:
            next_downstreams = node_config.get("downstreams")
            return next_downstreams.get(option)

    def run(self):
        while True:
            task_info = self.read_reply_info()
            if task_info is None:
                continue
            task_id = task_info.get('task_id')
            pod_id = task_info.get('pod_id')
            result = task_info.get("result")
            if result == 'exception':
                self.handle_exception(pod_id, task_info)
                # todo
                continue
            logger.info("[graph] [%s] task_id: [%s], [%s]", self.redis_graph, task_id, task_info)
            next_pod_queue = self.get_next_queue(pod_id)
            if next_pod_queue:
                # 本node内部的threed pod 尚未处理完成.
               task_info = {"task_id": task_id}
               self.send_task_info(next_pod_queue,task_info)
            else:
                option = task_info.get('option')
                # 没有获取到下一个queue 说明应该到下一个Node了.
                downstreams = self.get_downstreams(pod_id,option)
                if downstreams:
                    task_info = {'task_id': task_id}
                    self.dispense_to_downstream(downstreams,task_info)
                else:
                    pass
                    # todo graph over logging





