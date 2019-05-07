from worker_base import DataLoader
import time
import logging
import random
logger = logging.getLogger(__name__)
interval = 1


class StartTiming(DataLoader):
    def run(self, task_id):
        self.r.hset('timing', task_id, time.time())



class EndTiming(DataLoader):
    def run(self, task_id):
        time_start = self.r.hget('timing', task_id) or 0
        time_start = float(time_start)
        cost_time = time.time() - time_start
        self.r.hset("cost_time", task_id, cost_time)


class dl1(DataLoader):

    def run(self, task_id):
        # interval = self.r.hget("interval", 'dl1') or 1
        # interval = int(interval)
        # # self.r.hset("interval", 'dl1', interval+5)
        time.sleep(interval)
        logger.info("[worker] [%s] task_id: [%s],", 'dl1', task_id)
        value = self.r.hget(task_id, 'dl1') or "init"
        self.r.hset(task_id, "dl1", value + "_" + task_id)

class dl2(DataLoader):

    def run(self, task_id):
        # interval = self.r.hget("interval", 'dl2') or 2
        # interval = int(interval)
        # # self.r.hset("interval", 'dl2', interval+5)
        time.sleep(interval)
        logger.info("[worker] [%s] task_id: [%s],", 'dl2', task_id)
        value = self.r.hget(task_id, 'dl2') or "init"
        self.r.hset(task_id, "dl2", value + "_" + task_id)


class dl3(DataLoader):

    def run(self, task_id):
        # interval = self.r.hget("interval", 'dl3') or 3
        # interval = int(interval)
        # # self.r.hset("interval", 'dl3', interval+5)
        time.sleep(interval)
        logger.info("[worker] [%s] task_id: [%s],", 'dl3', task_id)
        value = self.r.hget(task_id, 'dl3') or "init"
        self.r.hset(task_id, "dl3", value + "_" + task_id)


class dl4(DataLoader):

    def run(self, task_id):
        time.sleep(interval)
        logger.info("%s  --- %s", self.__class__, task_id)
        seed = self.r.hget('seed', 'seed3')
        seed = int(seed) + 5
        self.r.hmset('seed', {"seed4": seed})


class dl5(DataLoader):

    def run(self, task_id):
        time.sleep(4)
        logger.info("%s  --- %s", self.__class__, task_id)
        seed = self.r.hget('seed', 'seed4')
        seed = int(seed) + 5
        self.r.hmset('seed', {"seed5": seed})
