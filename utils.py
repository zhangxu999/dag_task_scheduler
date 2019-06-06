import redis
import json
import numpy as np
import logging
logger = logging.getLogger(__name__)
from functools import reduce
with open('config/config.json') as f:
    config = json.load(f)


def load_redis(redis_name):
    redis_config = config.get(redis_name)
    host = redis_config.get("host")
    port = redis_config.get("port")
    db = redis_config.get("db")
    redis_client = redis.StrictRedis(host=host, port=port, db=db, charset='utf-8',decode_responses=True)
    return redis_client

with open('elements_config/node.json') as f:
    all_nodes = json.load(f)


def load_node(node_id):
    for n in all_nodes:
        if n['id'] == node_id:
            return n
    if n:
        raise Exception('not found node_id {} in all_nodes'.format(node_id))


with open('elements_config/operator.json') as f:
    all_operator = json.load(f)


def load_operator(loader_id):
    for n in all_operator:
        if n['id'] == loader_id:
            return n


with open('elements_config/dag.json') as f:
    all_dags = json.load(f)


def load_graph(graph_id):
    for n in all_dags:
        if n['id'] == graph_id:
            return n


class RedisMaxin:
    r = load_redis("queue_redis")
    redis_log = load_redis("stat_redis")
    redis_reply_key = "graph_dag1"


stat_redis = load_redis('stat_redis')

def record_cost_time(pod_id, cost):
    if cost < 0.1:
        return
    cost_key, cnt_key, lastround_key = "cost_" + pod_id, "cnt_" + pod_id, 'last_' + pod_id
    total_cost = stat_redis.hincrbyfloat('operator_timecost', cost_key, cost)
    count = stat_redis.hincrby("operator_timecost", cnt_key, 1)
    if int(count) > 100:
        cost = round(int(total_cost)/int(count), 4)
        stat_redis.hmset('operator_timecost', {cost_key: 0, cnt_key: 0, lastround_key: cost})


def trans_graph_2_adjmatrix(graph):
    adj_mat_mapping = {}
    data_point = []
    current_number = 0
    node_stack = []
    is_visited = {}

    root_node = graph.get("root_node")
    node_edges = graph.get("graph")
    curr_index = current_number
    adj_mat_mapping[root_node] = curr_index
    node_stack.append(root_node)
    while True:
        if not node_stack:
            break
        else:
            curr_node_id = node_stack.pop()
            curr_index = adj_mat_mapping.get(curr_node_id)
            curr_node = node_edges.get(curr_node_id)
            if curr_node is None:
                raise Exception("there is a node not include in graph {}".format(curr_node))
            edges = curr_node.get("downstreams")
            if edges is None:
                continue

            if isinstance(edges, str):
                edges =  [edges]
            else:
                edges = set(reduce(lambda x, y: x + y, edges.values()))

            for i in edges:
                index_i = adj_mat_mapping.get(i)
                if not index_i:
                    current_number += 1
                    index_i = current_number
                    adj_mat_mapping[i] = index_i
                data_point.append((curr_index, index_i))
                if not is_visited.get(i):
                    node_stack.append(i)
            is_visited[curr_node_id] = True
    mat_shape = current_number+1
    adj_mat = np.zeros((mat_shape, mat_shape))
    for i, j in data_point:
        adj_mat[i][j] = 1
    return adj_mat


def dag_validator(dag):
    """
    检查图中是否有环.
    :param dag:
    :return:
    """
    adj_mat = trans_graph_2_adjmatrix(dag)
    while True:
        arr_in = adj_mat.sum(axis=0)
        arr_out = adj_mat.sum(axis=1)
        candicate = np.argwhere(np.logical_and(np.logical_not(arr_in), arr_out))
        if not candicate.shape[0]:
            break
        candicate = candicate.reshape(candicate.shape[0]).tolist()
        curr = candicate.pop()
        adj_mat[curr] = 0

    assert  adj_mat.sum() == 0, "have circle.{}".format(adj_mat.sum())



