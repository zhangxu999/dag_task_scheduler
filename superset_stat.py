import datetime
import time
from collections import Counter
from functools import reduce
import copy
import logging.config
import json
import traceback
from sqlalchemy import create_engine, MetaData, select, \
    String, Integer, BigInteger, Table, Column, func, text, and_, or_, desc
from utils import load_graph, load_redis

with open("logging.json") as f:
    config = json.load(f)

logging.config.dictConfig(config)
logger = logging.getLogger(__name__)


sqlite_url = "sqlite:///data/graph.sqlite"
engine = create_engine(sqlite_url)

redis_queue_length = Table('redis_queue_length', MetaData(), autoload=True, autoload_with=engine)
graph_table = Table('graph_info', MetaData(), autoload=True, autoload_with=engine)

queue_redis = load_redis("queue_redis")
stat_redis = load_redis("stat_redis")

data_insert_base = {
    'dag': 'dag1',
    'update_flag': None,
}
insert_sql = redis_queue_length.insert()
insert__graph = graph_table.insert()


def update_graph():
    update_graph_flag = stat_redis.get("update_graph_flag")
    if not update_graph_flag:
        return
    dag = load_graph("1").get("graph")
    data_insert = []
    now = datetime.datetime.now()
    print("update_graph_flag")

    template = {"graph": "dag1", "source": 0, 'target': 0, "value": 1, "update_flag": now}

    for k, v in dag.items():
        down = v.get('downstreams')
        if not down:
            continue
        if isinstance(down, str):
            edge = copy.copy(template)
            edge.update({"source": k, "target": down})
            data_insert.append(edge)
        else:
            result = Counter(reduce(lambda x, y: x + y, down.values()))
            for a, b in result.items():
                edge = copy.copy(template)
                edge.update({"source": k, "target": a, "value": b})
                data_insert.append(edge)
    try:
        with engine.connect() as conn:
            conn.execute(insert__graph, data_insert)
            stat_redis.delete("update_graph_flag")
    except Exception as e:
        tb = traceback.format_exc()
        logger.error(tb)

    return True


def update_queue():
    all_queue = queue_redis.keys("node_*")
    if not all_queue:
        return
    print(all_queue)
    now = datetime.datetime.now()
    data_insert_list = []
    for k in all_queue:
        data_insert = {
                       'length': queue_redis.llen(k),
                       'node': k,
                       'update_flag': now,
                       'dag': 'dag1'
                       }
        data_insert_list.append(data_insert)
    with engine.connect() as conn:
        conn.execute(insert_sql, data_insert_list)
        logger.debug("insert one batch")

    return True

while True:
    time.sleep(5)
    update_queue()
    update_graph()









