import logging
import csv
from queue import PriorityQueue

if not __package__:
    from work import Job
    from computer import Scheduler
    import configure
else:
    from .work import Job
    from .computer import Scheduler
    from . import configure

class Simulation(object):
    CONFIGURATION = configure.configuration()
    INITIAL_TIME=0
    NUM_JOBS=int(CONFIGURATION['Simulation']['NUM_JOBS'])
    SIMULATION_TIME=float(CONFIGURATION['Simulation']['SIMULATION_TIME'])
    def __init__(self, filename=None):
        if filename is not None:
            self.CONFIGURATION = configure.configuration(filename=filename)
            self.NUM_JOBS=int(self.CONFIGURATION['Simulation']['NUM_JOBS'])
            self.SIMULATION_TIME=float(self.CONFIGURATION['Simulation']['SIMULATION_TIME'])
        logging.debug(f'NUM_JOBS: {self.NUM_JOBS}, SIMULATION_TIME: {self.SIMULATION_TIME}')
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
        self.scheduler.generate_arrivals()
        while not self.event_queue.empty():
            event = self.event_queue.get()
            if event.arrival_time > self.SIMULATION_TIME:
                break
            self.time = event.arrival_time
            event.resolve()
        logging.debug(f'total simulation time: {self.time}')


__all__ = ['Simulation']

if __name__ == '__main__':
    logging.basicConfig(filename='logging.log', filemode='w', level='DEBUG')
    simulation = Simulation()
    simulation.run()
    jobs = [job for job in simulation.work if job.__class__ == Job and job.start_time>0]
    sim_data = [
        [job.start_time, job.finish_time, len(job.tasks), job.finish_time-job.start_time] for 
        job in jobs
    ]
    with open('sim_data.csv', 'w', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerow(['start time', 'finish time', 'num tasks', 'job latency'])
        writer.writerows(sim_data)
    latencies = [data[3] for data in sim_data]
    start_time = sim_data[0][0]
    finish_time = sim_data[-1][1]
    total_time = finish_time - start_time
    avg_task_latencies = [
        sum([task.finish_time - task.start_time for task in job.tasks])/int(simulation.CONFIGURATION["Work.Job"]["NUM_TASKS"]) for job in jobs
    ]

    print(f'Average Job Latency: {sum(latencies)/Simulation.NUM_JOBS}')
    print(f'Average Average Task Latency: {sum(avg_task_latencies)/simulation.NUM_JOBS}')
    print(f'Average time per job: {total_time/Simulation.NUM_JOBS}')