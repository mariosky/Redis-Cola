__author__ = 'mariosky'


import Redis_Cola
import uuid
import time

def suma(a,b):
        return a+b


server = Redis_Cola.Cola("curso")


## Each Worker
worker = Redis_Cola.Worker(uuid.uuid4(), server)

for i in range(105):
    t = worker.pull_task()
    if (t):
        if i not in [12,15,50]:

            t.result = suma(**t.params)
            print time.sleep(10)
            t.put_result(worker)

    else:
        pass

    worker.send_heartbeat()



