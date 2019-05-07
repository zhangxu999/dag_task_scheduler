import redis

class WorkerBase:
    """
    用于worker文件的基类.
    """
    def __init__(self):
        self.r = redis.StrictRedis(host='localhost', port=6379, db=2, charset="utf-8", decode_responses=True)

    def init_db(self):
        pass

    def run(self, task_id):
        raise NotImplementedError


class DataLoader(WorkerBase):

    def __init__(self, init_info):
        self.name = init_info.get('name')
        super().__init__()

    @property
    def options(self):
        return None


class DecisionMaker(WorkerBase):
    def __init__(self, init_info):
        self.options = init_info.get('options')
        super().__init__()
        # super.__init__(name)

    def run(self, task_id):
        raise NotImplementedError
