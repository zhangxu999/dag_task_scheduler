from utils import RedisMaxin
import threading
from threading import Thread
import time
import logging
import json
from collections import Counter, defaultdict
logger = logging.getLogger(__name__)


class ManagerMaxin(RedisMaxin):
    def __init__(self, graph):
        Thread.__init__(self)
        self.graph = graph

    def send_exit_command(self, pod_id, kill_num):
        command_info = [json.dumps({'command_type':'exit'}) for i in range(kill_num)]
        self.r.rpush(pod_id, *command_info)
        print("manager exit", pod_id, kill_num)

    def start_new_pod(self, node_id, pod_id, start_num=1):
        node = self.graph.get_node_by_id(node_id)
        for i in range(start_num):
            pod = node.make_thread_pod(pod_id)
            pod.start()
            print("[manager]start pod,", node_id, pod_id)
            logger.info("[manager]start pod, %s, %s", node_id, pod_id)

    def stat_queue_and_thread(self, key_length):
        thread_count = Counter([(t.__class__.__name__, t.name) for t in threading.enumerate() \
                                if t.__class__.__name__ == "ThreadPod"])
        print('-----------------------------')
        for t, c in thread_count.items():
            print(t, c)
        print(key_length)

    def get_unuse_slot_number(self):
        thread_count = [0 for t in threading.enumerate() if t.__class__.__name__ == "ThreadPod"]
        return 200 - len(thread_count)





    def get_all_keys_length(self):
        keys = self.r.keys("*")
        key_length = [(k, self.r.llen(k)) for k in keys if self.r.type(k) == 'list']
        key_length.sort(key=lambda x: x[1], reverse=True)
        return key_length


class minimum_total_costManager(ManagerMaxin, Thread):
    def __init__(self, graph):
        ManagerMaxin.__init__(self, graph)
        self.name = "minimum_total_cost_manager_" + self.graph.name


    def get_operator_cost_time(self):
        total_average = defaultdict(dict)
        raw_info = self.redis_log.hgetall('operator_timecost')
        for k, v in raw_info.items():
            base, queue_key = k.split('_')
            total_average[queue_key][base] = v
        final_stat = defaultdict(int)
        for k, v in total_average.items():
            average = (float(v['cost'])+float(v.get('last', 0))*100)/(100+int(v['cnt']))
            final_stat[k] = average
        return final_stat

    def get_queue_lentgh(self):
        keys = self.r.keys("*,*")
        key_length = {k: self.r.llen(k) for k in keys if self.r.type(k) == 'list'}
        return key_length

    def get_thread_number(self):
        thread_count = Counter([t.name for t in threading.enumerate() if t.__class__.__name__ == "ThreadPod"])
        return thread_count

    def get_total_waiting_time(self, queue_lentgh, threads_num, operator_time):
        total_time = []
        for queue_key, queue_length in queue_lentgh.items():
            print("get_total_waiting_time", queue_key, queue_length)
            t_num = threads_num.get(queue_key, 0)
            o_time = operator_time.get(queue_key, 0)
            if t_num == 0:
                print(t_num)
            J = queue_length // (t_num+0.1)
            K = queue_length % (t_num+0.1)
            if o_time is None:
                print(o_time)
            cost = o_time * (J + 1) * (t_num * J / 2 + K)

            total_time.append((queue_key, cost, queue_length, t_num))
        total_time.sort(key=lambda x: x[1], reverse=False)
        return total_time

    def run(self):
        while True:
            time.sleep(1)
            unuse_slot_num = self.get_unuse_slot_number()
            queue_lentgh = self.get_queue_lentgh()
            threads_num = self.get_thread_number()
            operator_time = self.get_operator_cost_time()
            print('-----------------------------')
            print('-queue_lentgh---')
            for t, c in queue_lentgh.items():
                print(t, c)
            print('--threads_num---')
            for t, c in threads_num.items():
                print(t, c)
            print('--operator_time--')
            for t, c in operator_time.items():
                print(t, c)

            total_cost_time = self.get_total_waiting_time(queue_lentgh, threads_num, operator_time)
            done_flag = True
            for worst_queue, cost_time, q_length, t_num in total_cost_time:
                if q_length > t_num:
                    done_flag = False
                    break
            if done_flag:
                continue

            if q_length <= unuse_slot_num:
                node_id, pod_id = worst_queue.split(",")
                self.start_new_pod(node_id, pod_id, q_length)
            else:
                need_kill_threads_num = q_length - unuse_slot_num
                threads_can_killnum = [(queue, t_num) for queue, t_num in threads_num.items() if t_num - queue_lentgh.get(queue, 0)>2]
                for queue, can_kill_num in threads_can_killnum:
                    can_kill_num = min(can_kill_num, need_kill_threads_num)
                    self.send_exit_command(queue, can_kill_num)
                    need_kill_threads_num -= can_kill_num
                    if need_kill_threads_num <= 0:
                        break


class PodManager(ManagerMaxin, Thread):
    def __init__(self, graph):
        ManagerMaxin.__init__(graph)
        self.name = "manager_" + self.graph.name

    def run(self):
        while True:
            time.sleep(1)
            key_length = self.get_all_keys_length()
            self.stat_queue_and_thread(key_length)
            if not key_length:
                continue
            for k, length in key_length:
                if length > 5:
                    node_id, pod_id = k.split(",")
                    self.start_new_pod(node_id, pod_id)
