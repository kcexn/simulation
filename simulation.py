import logging
from queue import PriorityQueue
import csv

from computer import Scheduler
from work import Job
class Simulation(object):
    INITIAL_TIME=0
    NUM_JOBS=5000
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
            event = self.event_queue.get()
            if event.get_arrival_time() > Simulation.SIMULATION_TIME:
                break
            self.set_simulation_time(event.get_arrival_time())
            event.resolve()
        logging.debug(f'total simulation time: {self.get_simulation_time()}')
            

if __name__ == "__main__":
    logging.basicConfig(filename='logging.log', filemode='w', level='INFO')
    simulation = Simulation()
    simulation.run()
    jobs = [work for work in simulation.work if work.__class__ == Job]
    sim_data = [
        [job.get_start_time(), job.get_finish_time(), len(job.get_tasks()), job.get_finish_time()-job.get_start_time()] for 
        job in jobs if job.get_start_time()>200
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