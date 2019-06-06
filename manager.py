import pandas as pd
from utils import RedisMaxin
import threading
from threading import Thread
import time
import logging
import json
from collections import Counter, defaultdict
from elements import Node
logger = logging.getLogger(__name__)


class ManagerMaxin(RedisMaxin):
    def __init__(self, graph):
        Thread.__init__(self)
        self.graph = graph
        self.name = "manager_" + self.graph.name

    def send_exit_command(self, node_id, kill_num):
        print(node_id, kill_num)
        if kill_num == 0:
            print(kill_num)
        command_info = [json.dumps({'command_type': 'exit'}) for i in range(kill_num)]
        print()
        self.r.rpush(node_id, *command_info)
        print("manager exit", node_id, kill_num)

    def start_new_node(self, node_id, start_num=1):
        # node = self.graph.get_node_by_id(node_id)
        nodestr, node_id = node_id.split('_')
        for i in range(start_num):
            new_node = Node(node_id)
            new_node.start()
        print("[manager]start pod,", node_id, start_num)
        logger.info("[manager]start node, %s, %s", start_num, node_id)

    def stat_queue_and_thread(self, key_length):
        thread_count = Counter([(t.__class__.__name__, t.name) for t in threading.enumerate() \
                                if t.__class__.__name__ == "Node"])
        print('-----------------------------')
        for t, c in thread_count.items():
            print(t, c)
        print(key_length)

    def get_unuse_slot_number(self):
        thread_count = [0 for t in threading.enumerate() if t.__class__.__name__ == "Node"]
        return 70 - len(thread_count)

    def get_queue_lentgh(self):
        keys = self.r.keys("node_*")
        key_length = {k: self.r.llen(k) for k in keys if self.r.type(k) == 'list'}
        return key_length




    def get_all_keys_length(self):
        keys = self.r.keys("node_*")
        key_length = [(k, self.r.llen(k)) for k in keys if self.r.type(k) == 'list']
        key_length.sort(key=lambda x: x[1], reverse=True)
        return key_length


    def get_operator_cost_time(self):
        total_average = defaultdict(dict)
        raw_info = self.redis_log.hgetall('operator_timecost')
        for k, v in raw_info.items():
            base, queue_key = k.split('_', 1)
            total_average[queue_key][base] = v
        final_stat = defaultdict(int)
        for k, v in total_average.items():
            average = (float(v['cost']) + float(v.get('last', 0)) * 100) / (100 + int(v['cnt']))
            final_stat[k] = average
        return final_stat


    def get_thread_number(self):
        thread_count = Counter([t.name for t in threading.enumerate() if t.__class__.__name__ == "Node"])
        return thread_count


class minimum_total_costManager(ManagerMaxin, Thread):
    def __init__(self, graph):
        ManagerMaxin.__init__(self, graph)
        self.name = "minimum_total_cost_manager_" + self.graph.name


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

    def can_kill_threads(self, q_length, th_num):
        if q_length > th_num:
            return 0
        if th_num < 3:
            return 0
        return th_num - 3

    def run(self):
        nodes_name = ['node_'+k for k in self.graph.nodes]
        df = pd.DataFrame(columns=['q_len', 'th_num', 'ope_time'], index=nodes_name)
        df['q_len'] = 0
        df['th_num'] = 1
        while True:
            time.sleep(1)

            unuse_slot_num = self.get_unuse_slot_number()
            queue_lentgh = self.get_queue_lentgh()
            threads_num = self.get_thread_number()
            operator_time = self.get_operator_cost_time()
            df['q_len'].update(pd.Series(queue_lentgh))
            df['th_num'].update(pd.Series(threads_num))
            df['ope_time'].update(pd.Series(operator_time))
            print(df)
            total_cost_time = self.get_total_waiting_time(queue_lentgh, threads_num, operator_time)
            done_flag = True
            for worst_queue, cost_time, q_length, t_num in total_cost_time:
                if q_length > t_num:
                    done_flag = False
                    break
            if done_flag:
                continue

            if q_length <= unuse_slot_num:
                node_str, node_id = worst_queue.split("_")
                self.start_new_node(node_id, q_length)
            else:
                need_kill_threads_num = q_length - unuse_slot_num
                threads_can_killnum = [(queue, t_num) for queue, t_num in threads_num.items() if t_num - queue_lentgh.get(queue, 0)>2]
                for queue, can_kill_num in threads_can_killnum:
                    can_kill_num = min(can_kill_num, need_kill_threads_num)
                    self.send_exit_command(queue, can_kill_num)
                    need_kill_threads_num -= can_kill_num
                    if need_kill_threads_num <= 0:
                        break


class MaximumManager(ManagerMaxin, Thread):
    minimum_threads = 3


    def collect_status_info(self):
        self.unuse_slot_num = self.get_unuse_slot_number()
        self.queue_lentgh = self.get_queue_lentgh()
        self.threads_num = self.get_thread_number()
        self.operator_time = self.get_operator_cost_time()
        return self.unuse_slot_num, self.queue_lentgh, self.threads_num, self.operator_time

    def get_kill_thread_num(self, exclude_node, kill_num):
        can_kill_num = {}
        for node_name in self.nodes_name:
            if node_name == exclude_node:
                continue
            th_num = self.threads_num.get(node_name, 0)
            q_length = self.queue_lentgh.get(node_name, 0)
            if th_num > q_length * 1.2 and (th_num > self.minimum_threads) :
                ki_num = round(th_num - max(q_length * 1.2, self.minimum_threads))
                ki_num = min(kill_num, ki_num)
                if ki_num > 0:
                    can_kill_num[node_name] =  ki_num
                    kill_num -= ki_num
            if kill_num <= 0:
                break

        return can_kill_num, kill_num;

    def kill_threads(self, can_kill_num):
        if not can_kill_num:
            return
        for node_name, num in can_kill_num.items():
            self.send_exit_command(node_name, num)




    def run(self):
        self.nodes_name = ['node_'+k for k in self.graph.nodes]
        df = pd.DataFrame(columns=['q_len', 'th_num', 'ope_time'], index=self.nodes_name)
        df['q_len'] = 0
        df['th_num'] = 1
        self.df = df

        while True:
            time.sleep(1)
            unuse_slot_num, queue_lentgh, threads_num, operator_time  = self.collect_status_info()
            self.df['q_len'].update(pd.Series({k: queue_lentgh.get(k, 0) for k in self.nodes_name}))
            self.df['th_num'].update(pd.Series({k: threads_num.get(k, 0) for k in self.nodes_name}))
            self.df['ope_time'].update(pd.Series({k: operator_time.get(k, 0) for k in self.nodes_name}))
            print(self.df)

            for node_name in self.nodes_name:
                q_length = queue_lentgh.get(node_name, 0)
                th_num = threads_num.get(node_name, 0)
                if q_length > 0 and th_num <= q_length*1.2:
                    add_num = round(q_length*1.2 - th_num)
                    if unuse_slot_num > add_num:
                        self.start_new_node(node_name, add_num)
                    else:
                        self.start_new_node(node_name, unuse_slot_num)
                        kill_num = add_num-unuse_slot_num
                        can_kill_num, left_num = self.get_kill_thread_num(node_name, kill_num)
                        self.kill_threads(can_kill_num)
                        self.start_new_node(node_name, kill_num-left_num)
                        self.threads_num = self.get_thread_number()
                        if left_num > 0 :
                            break








class NodeManager(ManagerMaxin, Thread):
    def __init__(self, graph):
        ManagerMaxin.__init__(self, graph)
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
                    _, node_id = k.split('_')
                    self.start_new_node(node_id, length)
