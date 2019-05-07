import redis
import time
import json
r = redis.StrictRedis(host='localhost', port=6379, db=1, charset="utf-8", decode_responses=True)
r2 = redis.StrictRedis(host='localhost', port=6379, db=2, charset="utf-8", decode_responses=True)

root_pod = '1,1'
interval = 0.1
def send_message():
    r2.delete('interval')
    for i in range(1000, 1050):
        r2.delete("task_"+str(i))
        task_info = {"task_id": "task_"+str(i)}
        r.rpush('1,1', json.dumps(task_info))
        time.sleep(interval)

while True:
    out_interval = 5
    time.sleep(out_interval)
    send_message()

