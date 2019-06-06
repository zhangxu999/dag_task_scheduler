import redis
import time
r = redis.StrictRedis(host='localhost', port=6379, db=1, charset="utf-8", decode_responses=True)


def show_redis():
    all_keys = r.keys("*")
    for k in (all_keys or []):
        print(k, "::", r.llen(k))
    print("--------------------")
    time.sleep(1)
while True:
    show_redis()