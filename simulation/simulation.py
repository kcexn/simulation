import logging
import csv
from queue import PriorityQueue

if not __package__:
    from work import Job
    from computer import *
    import configure
else:
    from .work import Job
    from .computer import *
    from . import configure

logger = logging.getLogger('Simulation')
class Simulation(object):
    """
    Simulation([configuration=configparser.ConfigParser()]) -> Simulation

    Constructs a cluster computing simulation that is configured either by 
    a ConfigParser configuration object that is passed in, or 
    if no configuration is provided checks for a file called configuration.ini 
    either in the current directory '.', or in a subdirectory './simulation'.
    In case, a configuration.ini file exists in both '.' and './simulation', 
    the configuration.ini file in the current directory is selected.
    """
    import os
    CONFIGURATION_PATH = ['./simulation', '.']
    CONFIGURATION = None
    for p in CONFIGURATION_PATH:
        if os.path.isfile(f'{p}/configuration.ini'):
            CONFIGURATION = configure.configuration(f'{p}/configuration.ini')
    INITIAL_TIME=0
    NUM_JOBS=int(CONFIGURATION['Simulation']['NUM_JOBS'])
    SIMULATION_TIME=float(CONFIGURATION['Simulation']['SIMULATION_TIME'])
    logger = logging.getLogger('Simulation')
    def __init__(self, configuration=None):
        if configuration is not None:
            self.CONFIGURATION = configuration
            self.NUM_JOBS = int(self.CONFIGURATION['Simulation']['NUM_JOBS'])
            self.SIMULATION_TIME=float(self.CONFIGURATION['Simulation']['SIMULATION_TIME'])
        self.logger.info(f'NUM_JOBS: {self.NUM_JOBS}, SIMULATION_TIME: {self.SIMULATION_TIME}')
        self.work = []
        self._time = Simulation.INITIAL_TIME
        self.event_queue = PriorityQueue()
        self.cluster = Cluster(self)

    @property
    def time(self):
        return self._time
    
    @time.setter
    def time(self, time):
        self._time = time

    def run(self):
        self.logger.info('running...')
        self.cluster.generate_arrivals()
        while not self.event_queue.empty():
            event = self.event_queue.get()
            if event.arrival_time > self.SIMULATION_TIME:
                break
            self.time = event.arrival_time
            event.resolve()
        self.logger.info(f'total simulation time: {self.time}')


__all__ = ['Simulation']

if __name__ == '__main__':
    import datetime
    logging.basicConfig(filename='logging.log', filemode='w', level='CRITICAL')
    start_time = datetime.datetime.now()
    print(f'start time: {start_time}')
    simulation = Simulation()
    simulation.run()
    finish_time = datetime.datetime.now()
    print(f'finish time: {finish_time}')
    print(f'running time: {finish_time - start_time}')
    unfinished_jobs = [job for job in simulation.work if job.__class__ == Job and not job.is_finished]
    logging.debug(f'unfinished jobs: {[job.id for job in unfinished_jobs]}')
    logging.debug(f'unfinished tasks: {[task.id for job in unfinished_jobs for task in job.tasks if not task.is_finished]}')
    jobs = [job for job in simulation.work if job.__class__ == Job and job.start_time>0 and job.is_finished]
    print(len(jobs))
    sim_data = [
        [job.start_time, job.finish_time, len(job.tasks), job.finish_time-job.start_time] for 
        job in jobs
    ]

    with open('./simulation/sim_data.csv', 'w', newline='') as f:
        writer = csv.writer(f, dialect='excel')
        writer.writerow(['start time', 'finish time', 'num tasks', 'job latency'])
        writer.writerows(sim_data)

    latencies = [data[3] for data in sim_data]
    start_time = sim_data[0][0]
    finish_time = sim_data[-1][1]
    total_time = finish_time - start_time
    task_latencies = [
        task.finish_time - task.start_time for job in jobs for task in job.tasks
    ]
    avg_task_latency = sum(task_latencies)/len(task_latencies)

    server_cum_times = [[server.cumulative_idle_time, server.cumulative_busy_time] for server in simulation.cluster.servers]
    total_idle_time = sum(times[0] for times in server_cum_times)
    total_busy_time = sum(times[1] for times in server_cum_times)
    average_availability = total_idle_time/(total_idle_time + total_busy_time)
    print(f'{average_availability}')

    print(f'Average Job Latency: {sum(latencies)/simulation.NUM_JOBS}')
    print(f'Average Task Latency: {avg_task_latency}')
    print(f'Average time per job: {total_time/simulation.NUM_JOBS}')