# Solution to define a Pipeline that can have multiple Stages, where
#  each Stage is a function that can be executed in 1 or more threads 
#  or processes.
# Author: Fernando Ancona.

# Imports.
import threading, multiprocessing

# Classes.
class Pipeline:
    def __init__(self):
        super().__init__()
        self._stages = []
        self._stages_meta = []
        self._initialized = False
        # self._workers = []
        # self._in_queue = mp.JoinableQueue(in_q_size)
    
    @property
    def stage_count(self):
        return len(self._stages)
    @property
    def initialized(self):
        return self._initialized
    
    def add_stage(self, stage, same_cpu=True, count=1):
        self._stages.append(stage)
        self._stages_meta.append(
            {
                'same_cpu': same_cpu,
                'count': count
            }
        )
    def start_workers(self):
        if len(self._stages) > 0:
            self._start_stage(0)
        if len(self._stages) > 1:
            # self._start_stage(1)
            # self._stages[0].connect(self._stages[1])
            for i in range(1, len(self._stages)):
                self._start_stage(i)
                self._stages[i-1].connect(self._stages[i])
        self._initialized = True
    
    def _start_stage(self, index):
        stage = self._stages[index]
        same_cpu = self._stages_meta[index]['same_cpu']
        count = self._stages_meta[index]['count']
        
        if same_cpu:
            self._start_threads(stage, count)
        else:
            self._start_processes(stage, count)
    
    def _start_threads(self, stage, count):
        for _ in range(count):
            thread = threading.Thread(target=stage.start, args=(stage, ))
            thread.daemon = True
            thread.start()
    
    def _start_processes(self, stage, count):
        for _ in range(count):
            process = multiprocessing.Process(target=stage.start, args=(stage, ))
            process.daemon = True
            process.start()
    
    def join(self):
        for stage in self._stages:
            stage.join()
    
    def input_data(self, data):
        try:
            self._stages[0].in_queue.put(data)
        except IndexError:
            print('Cannot input data: Pipeline is empty.')
            raise
  
class Stage:
    def __init__(self, func, in_q_size=100, setup_args={}):
        self._func = func
        self._in_queue = multiprocessing.JoinableQueue(in_q_size)
        self._setup_args = setup_args
        self._out_queue = None
        
    @property
    def in_queue(self):
        return self._in_queue
    @property
    def out_queue(self):
        return self._out_queue
    @out_queue.setter
    def out_queue(self, out_queue):
        self._out_queue = out_queue
    
    def join(self):
        self._in_queue.join()
    
    def connect(self, other_stage):
        self._out_queue = other_stage.in_queue
    
    def start(self, *args, **kwargs):
        while True:
            q_input = self._in_queue.get()
            q_output = self._func(q_input, self._setup_args)
            if self._out_queue:
                self._out_queue.put(q_output)
            self._in_queue.task_done()

# Test.
def main():
    pipeline = Pipeline()
    
    sleep_stage = Stage(sleep_func)
    pipeline.add_stage(sleep_stage, same_cpu=True, count=2)
    
    sleep_stage_2 = Stage(sleep_func)
    pipeline.add_stage(sleep_stage_2, same_cpu=False, count=2)
    
    pipeline.start_workers()
    for _ in range(4):
        pipeline.input_data(2)
    
    pipeline.join()

import time
def sleep_func(sleep_time, setup_args):
    print('Sleeping for {} secs...'.format(sleep_time))
    time.sleep(sleep_time)

if __name__ == '__main__':
    main()