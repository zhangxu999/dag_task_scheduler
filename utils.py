import redis
import threading
from threading import Thread
import importlib
import json
from json import JSONDecodeError
from worker_base import DecisionMaker,DataLoader
import logging
logger = logging.getLogger(__name__)
r3 = redis.StrictRedis(host='localhost', port=6379, db=3, charset="utf-8", decode_responses=True)

def load_node(node_id):
    with open('elements_config/node.json') as f:
        all_nodes = json.load(f)
    for n in all_nodes:
        if n['id'] == node_id:
            return n


def load_operator(loader_id):
    with open('elements_config/operator.json') as f:
        all_nodes = json.load(f)
    for n in all_nodes:
        if n['id'] == loader_id:
            return n


def load_graph(graph_id):
    with open('elements_config/dag.json') as f:
        all_nodes = json.load(f)
    for n in all_nodes:
        if n['id'] == graph_id:
            return n


def load_threadPod(node_id,pod_id):
    node = load_node(node_id)
    pod = node.get("thread_pods").get(pod_id)


class RedisMaxin:
    r = redis.StrictRedis(host='localhost', port=6379, db=1, charset="utf-8", decode_responses=True)
    redis_log = redis.StrictRedis(host='localhost', port=6379, db=3, charset="utf-8", decode_responses=True)
    redis_reply_key = "graph_dag1"


def record_cost_time(pod_id, cost):
    if cost < 0.1:
        return
    cost_key, cnt_key, lastround_key = "cost_" + pod_id, "cnt_" + pod_id, 'last_' + pod_id
    total_cost = r3.hincrbyfloat('operator_timecost', cost_key, cost)
    count = r3.hincrby("operator_timecost", cnt_key, 1)
    if int(count) > 100:
        cost = round(int(total_cost)/int(count), 4)
        r3.hmset('operator_timecost', {cost_key: 0, cnt_key: 0, lastround_key: cost})

