import logging
from queue import PriorityQueue
import csv

from computer import Scheduler
from work import Job
class Simulation(object):
    INITIAL_TIME=0
    NUM_JOBS=36000
    SIMULATION_TIME=100000
    def __init__(self):
        self.work = []
        self._time = Simulation.INITIAL_TIME
        self.event_queue = PriorityQueue()
        self.scheduler = Scheduler(self)

    @property
    def time(self):
        return self._time
    
    @time.setter
    def time(self, time):
        self._time = time

    def run(self):
        logging.debug('running...')
        self.scheduler.generate_job_arrivals()
        while not self.event_queue.empty():
            event = self.event_queue.get()
            if event.arrival_time > Simulation.SIMULATION_TIME:
                break
            self.time = event.arrival_time
            event.resolve()
        logging.debug(f'total simulation time: {self.time}')
            

if __name__ == "__main__":
    logging.basicConfig(filename='logging.log', filemode='w', level='INFO')
    simulation = Simulation()
    simulation.run()
    jobs = [work for work in simulation.work if work.__class__ == Job]
    sim_data = [
        [job.start_time, job.finish_time, len(job.tasks), job.finish_time-job.start_time] for 
        job in jobs if job.start_time>0
    ]
    with open('sim_data.csv', 'w', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerow(['start time', 'finish time', 'num tasks', 'job latency'])
        writer.writerows(sim_data)
    latencies = [data[3] for data in sim_data]
    start_time = sim_data[0][0]
    finish_time = sim_data[-1][1]
    total_time = finish_time - start_time
    print(f'Average Job Latency: {sum(latencies)/Simulation.NUM_JOBS}')
    print(f'Average time per job: {total_time/Simulation.NUM_JOBS}')