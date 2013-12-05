import redis

HOST = "127.0.0.1"
PORT = 6379
APP_NAME = 'cola'

r = redis.Redis(host=HOST, port=PORT)


class Task:
    def __init__(self, **kwargs):
        self.id = kwargs['id']
        self.method = kwargs['method']
        self.params = kwargs.get('params', {})
        self.state = kwargs.get('state', 'created')
        self.expire = kwargs.get('expire', 'None')
        self.result = None
        self.__dict__.update(kwargs)

    def enqueue(self, app_name):
        pipe = r.pipeline()
        if pipe.rpush('%s:task_queue' % app_name, self.id):
            self.state = 'submitted'
            pipe.set(self.id, self.__dict__)
            pipe.execute()
            return True
        else:
            return False

    def put_result(self, app_name, as_dict = False):
        if r.sismember('%s:result_set' % app_name, self.id):
            _dict = eval(r.get(self.id))
            self.__dict__.update(_dict)
            if as_dict:
                return self.__dict__
            else:
                return self
        else:
            return None


    def get_result(self, app_name, as_dict = False):
        if r.sismember('%s:result_set' % app_name, self.id):
            _dict = eval(r.get(self.id))
            self.__dict__.update(_dict)
            if as_dict:
                return self.__dict__
            else:
                return self
        else:
            return None

    def __repr__(self):
        return self.id +" method:"+ str(self.method) +", params:" + str( self.params)

    def as_dict(self):
        return self.__dict__


class Cola:
    def __init__(self, name = APP_NAME  ):
        self.app_name = name
        self.task_counter = self.app_name+':task_counter'
        self.pending_set = self.app_name+':pending_set'
        self.task_queue = self.app_name+':task_queue'
        self.result_set = self.app_name+':result_set'

    def initialize(self):
        r.flushall()
        r.setnx(self.task_counter,0)

    def enqueue(self, **kwargs ):
        if kwargs['id'] is None:
            kwargs['id'] = "%s:task:%s" % (self.app_name, r.incr(self.task_counter))
        t = Task(**kwargs)
        t.enqueue(self.app_name)

    def dequeue(self):
        print self.task_queue
        task = r.blpop(self.task_queue)[1]
        print "task id",task
        r.sadd(self.pending_set,task)
        _task = r.get(task)
        print "task",_task
        return Task(**eval(_task))


if __name__ == "__main__":
    def suma(a,b):
        return a+b

    server = Cola("curso")
    server.initialize()
    task = {"id": None,"method": "suma","params": {"a": 12,"b": 10}}
    for i in range(100):
        server.enqueue(**task)


    for i in range(105):
        t = server.dequeue()
        t.result = suma(**t.params)
        #t.put_result()

        print t













