import logging
from queue import PriorityQueue

from computer import Scheduler
class Simulation(object):
    INITIAL_TIME=0
    NUM_JOBS=1
    SIMULATION_TIME=100000
    def __init__(self):
        self.work = []
        self.time = Simulation.INITIAL_TIME
        self.event_queue = PriorityQueue()
        self.scheduler = Scheduler(self)

    def get_simulation_time(self):
        return self.time
    
    def set_simulation_time(self, time):
        self.time = time

    def run(self):
        logging.debug('running...')
        self.scheduler.generate_job_arrivals()
        while not self.event_queue.empty():
            time,event = self.event_queue.get()
            if time > Simulation.SIMULATION_TIME:
                break
            self.set_simulation_time(time)
            event.resolve()
        logging.debug(f'total simulation time: {self.get_simulation_time()}')
            

if __name__ == "__main__":
    logging.basicConfig(level='DEBUG')
    simulation = Simulation()
    simulation.run()
