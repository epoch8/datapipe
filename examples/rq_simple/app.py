import time
from rq import Queue
from redis import Redis
from simple_functions import stepAB, stepBD, stepBE, stepCE


class Step:
    def __init__(self, func, input_dt, output_dt):
        self.func = func
        self.input_dt = input_dt
        self.output_dt = output_dt


g = {
    "dtA": [Step(stepAB, "dtA", "dtB")],
    "dtB": [Step(stepBD, "dtB", "dtD"), Step(stepBE, "dtB", "dtE")],
    "dtC": [Step(stepCE, "dtC", "dtE")],
}


redis_conn = Redis()
queue = Queue(connection=redis_conn)


def run_changelist_realtime(dt_changed):
    if dt_changed in g:
        for step in g[dt_changed]:
            queue.enqueue(step.func, step.input_dt, step.output_dt)
            run_changelist_realtime(step.output_dt)


run_changelist_realtime("dtA")
