import redis
import time
import json
import sys

r = redis.StrictRedis(host='localhost', port=6379, db=1, charset="utf-8", decode_responses=True)
r2 = redis.StrictRedis(host='localhost', port=6379, db=2, charset="utf-8", decode_responses=True)

interval = 0.01

def send_message():
    send_times = int(sys.argv[1])
    r2.delete('interval')
    for i in range(1000, 1000+send_times):
        r2.delete("task_"+str(i))
        task_info = {"task_id": "task_"+str(i)}
        r.rpush('node_3', json.dumps(task_info))
        time.sleep(interval)

send_message()

