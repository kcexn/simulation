import logging
from queue import PriorityQueue

from processes import ArrivalProcess,CompletionProcess

class Simulation(object):
    INITIAL_TIME=0
    def __init__(self,num_tasks):
        self.num_tasks = num_tasks
        self.tasks = []
        self.time = Simulation.INITIAL_TIME
        self.arrival_process = ArrivalProcess(self)
        self.completion_process = CompletionProcess(self)
        self.event_queue = PriorityQueue()

    def get_simulation_time(self):
        return self.time
    
    def set_simulation_time(self, time):
        self.time = time

    def generate_task_arrivals(self):
        for _ in range(num_tasks):
            self.event_queue.put(self.arrival_process.get_task())

    def run(self):
        logging.debug('running...')
        self.generate_task_arrivals()
        while not self.event_queue.empty():
            time,event = self.event_queue.get()
            self.set_simulation_time(time)
            event.resolve()
            

if __name__ == "__main__":
    logging.basicConfig(level='DEBUG')
    num_tasks=10
    simulation = Simulation(num_tasks)
    simulation.run()
