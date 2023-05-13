import logging
from queue import PriorityQueue

from processes import ArrivalProcess,CompletionProcess

class Simulation(object):
    INITIAL_TIME=0
    NUM_JOBS=1
    SIMULATION_TIME=100000
    def __init__(self):
        self.work = []
        self.time = Simulation.INITIAL_TIME
        self.arrival_process = ArrivalProcess(self)
        self.completion_process = CompletionProcess(self)
        self.event_queue = PriorityQueue()

    def get_simulation_time(self):
        return self.time
    
    def set_simulation_time(self, time):
        self.time = time

    def generate_task_arrivals(self):
        for _ in range(Simulation.NUM_JOBS):
            self.event_queue.put(self.arrival_process.get_task())

    def generate_job_arrivals(self):
        for _ in range(Simulation.NUM_JOBS):
            self.event_queue.put(self.arrival_process.get_job())

    def run(self):
        logging.debug('running...')
        # self.generate_task_arrivals()
        self.generate_job_arrivals()
        while not self.event_queue.empty():
            time,event = self.event_queue.get()
            if time > Simulation.SIMULATION_TIME:
                break
            self.set_simulation_time(time)
            event.resolve()
            

if __name__ == "__main__":
    logging.basicConfig(level='DEBUG')
    simulation = Simulation()
    simulation.run()
