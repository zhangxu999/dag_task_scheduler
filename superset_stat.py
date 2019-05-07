import datetime
import time
from sqlalchemy import create_engine, MetaData, select, \
    String, Integer, BigInteger, Table, Column, func, text, and_, or_, desc

import redis
postgres_url = "postgresql+psycopg2://dbuser:password@127.0.0.1:5432/exampledb"

engine = create_engine(postgres_url)

redis_queue_length = Table('redis_queue_length', MetaData(), autoload=True, autoload_with=engine)

r1 = redis.StrictRedis(host='localhost', port=6379, db=1, charset="utf-8", decode_responses=True)
data_insert_base = {
    'dag': 'dag1',
    'update_flag': None,

}
insert_sql = redis_queue_length.insert()

from utils import load_graph, load_node

# dag = load_graph("1")
#
# def get_pods(node_id):
#     node = load_node(node_id)
#     pods_order = node.get('pods_order')
#     return [ node_id+','+k for k in pods_order]
#
# def get_all_nodes(node_id):
#     node = dag.get('graph').get(node_id)
#     downstreams = node.get('downstreams')
#     pods = get_pods(node_id)
#
#     if downstreams:
#         for k, v in downstreams.items():
#             pods +
#     else
#         return []
#
# def read_all_queue():




all_queue = [
    '1,1',
    '1,2',
    '1,3',
    '2,1',
    '2,2',
    '2,3',
    '3,1',
    '3,2',
    '3,3',
    '4,1',
    '4,2',
    '4,3'
]
import random
flag = True
while True:
    now = datetime.datetime.now()
    data_insert_list = []
    for k in all_queue:
        data_insert = {'thread_pod': k,
                       'length': random.randint(10,30),
                       'node': k.split(',')[0],
                       'update_flag': now,
                       'dag': 'dag1'
                       }
        data_insert_list.append(data_insert)
    with engine.connect() as conn:
        conn.execute(insert_sql, data_insert_list)
    time.sleep(0.001)


