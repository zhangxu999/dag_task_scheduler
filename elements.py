from threading import Thread
import importlib
import json
import time
from json import JSONDecodeError
import traceback
import logging
logger = logging.getLogger(__name__)
from utils import RedisMaxin, load_node, load_operator, load_graph, load_redis, record_cost_time, dag_validator


class Node(Thread, RedisMaxin):

    def __init__(self, node_id):
        Thread.__init__(self)
        node_config = load_node(node_id)
        self.id = node_config.get('id')
        # self.name = node_config.get('name')
        self.name = self.redis_queue = "node_" + self.id
        self.upstreams = node_config.get('upstreams')

        self.heartbeat_interval = node_config.get('heartbeat_interval', 5)
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
        exception_handler = node_config.get('exception_handler') or "exceptions.default_node_exception"
        
        package = "workers." + ".".join(exception_handler.split(".")[:-1])
        work_class = exception_handler.split(".")[-1]
        module = importlib.import_module(package)
        self.exception_handler = getattr(module, work_class)


        

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
                "node_id": self.name,
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
                    tb = traceback.format_exc()
                    self.exception_handler(task_id, self.name, operator, tb)
                    work_result = {"result": "exception"}
                    break
            end_time = time.time()
            record_cost_time(self.redis_queue, round(end_time-start_time, 4))
            if work_result is None:
                work_result = {}
            result.update(work_result)
            self.send_result(result)


class Graph(RedisMaxin, Thread):

    def __init__(self, graph_id):
        Thread.__init__(self)
        graph_config = load_graph(graph_id)
        dag_validator(graph_config)
        self.name = graph_config.get('name')
        self.redis_graph = "graph_{}".format(self.name)
        self.graph = graph_config.get("graph")
        self.root_node = graph_config.get("root_node")
        self.nodes = dict()
        self.nodes_info = self.load_nodes()

        for n in self.nodes_info:
            node_id = n['id']
            node = Node(node_id)
            self.nodes[node_id] = node
        self.set_update_graph_flag()

    def set_update_graph_flag(self):
        self.redis_log.set('update_graph_flag', 1)


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
            node.start()

    def dispense_to_downstream(self, nodes_list, task):
        for n in nodes_list:
            next_node_key = "node_"+n
            self.send_task_info(next_node_key, task)

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

    def handle_exception(self, pod_id, exception_info):
        pass

    def get_downstreams(self, node_id, option):
        nodestr, node_id = node_id.split("_", 1)
        node_config = self.graph.get(node_id)
        next_downstreams = node_config.get("downstreams")
        if next_downstreams is None:
            return None
        elif isinstance(next_downstreams, str):
            return [next_downstreams]
        else:
            return next_downstreams.get(option)

    def run(self):
        while True:
            task_info = self.read_reply_info()
            if task_info is None:
                continue
            task_id = task_info.get('task_id')
            node_id = task_info.get('node_id')
            result = task_info.get("result")
            if result == 'exception':
                self.handle_exception(node_id, task_info)
                # todo
                continue
            logger.info("[graph] [%s] task_id: [%s], [%s]", self.redis_graph, task_id, task_info)
            option = task_info.get('option')
            downstreams = self.get_downstreams(node_id, option)
            if downstreams:
                task_info = {'task_id': task_id}
                self.dispense_to_downstream(downstreams, task_info)
            else:
                pass
                # todo graph over logging





