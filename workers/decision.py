from worker_base import DecisionMaker
import logging
import random
import time
logger = logging.getLogger(__name__)
interval = 1
class da1(DecisionMaker):

    def run(self, task_id):

        return '1'

class da2(DecisionMaker):

    def run(self, task_id):
        if task_id.count(".") > 3:
            return '3'
        else:
            return '1'


class da3(DecisionMaker):

    def run(self, task_id):
        if len(task_id)>3:
            return '1'
        else:
            return '2'

class da4(DecisionMaker):
    def run(self, task_id):
        # interval = self.r.hget("interval", 'da4') or 4
        # interval = int(interval)
        # # self.r.hset("interval", 'da4', interval+5)
        time.sleep(interval)
        value = self.r.hget(task_id, 'da4') or "init"
        self.r.hset(task_id, "da4", value + "_" + task_id)
        option = '2' if len(value) > 10 else '2'
        logger.info("[worker] [%s] task_id: [%s],", 'da4', task_id)
        return {"result": "done", "option": option}

class da5(DecisionMaker):

    def run(self, task_id):

        return '3'
