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

logger = logging.getLogger('Simulation')
class Simulation(object):
    CONFIGURATION = configure.configuration()
    INITIAL_TIME=0
    NUM_JOBS=int(CONFIGURATION['Simulation']['NUM_JOBS'])
    SIMULATION_TIME=float(CONFIGURATION['Simulation']['SIMULATION_TIME'])
    logger = logging.getLogger('Simulation')
    def __init__(self):
        self.logger.info(f'NUM_JOBS: {self.NUM_JOBS}, SIMULATION_TIME: {self.SIMULATION_TIME}')
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
        self.logger.info('running...')
        self.scheduler.generate_arrivals()
        while not self.event_queue.empty():
            event = self.event_queue.get()
            if event.arrival_time > self.SIMULATION_TIME:
                break
            self.time = event.arrival_time
            event.resolve()
        self.logger.info(f'total simulation time: {self.time}')


__all__ = ['Simulation']

if __name__ == '__main__':
    logging.basicConfig(filename='logging.log', filemode='w', level='INFO')
    simulation = Simulation()
    simulation.run()
    unfinished_jobs = [job for job in simulation.work if job.__class__ == Job and not job.is_finished]
    logging.debug(f'unfinished tasks: {[task.id for job in unfinished_jobs for task in job.tasks]}')
    jobs = [job for job in simulation.work if job.__class__ == Job and job.start_time>0 and job.is_finished]
    print(len(jobs))
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
    task_latencies = [
        task.finish_time - task.start_time for job in jobs for task in job.tasks
    ]
    avg_task_latency = sum(task_latencies)/len(task_latencies)

    server_cum_times = [[server.cumulative_idle_time, server.cumulative_busy_time] for server in simulation.scheduler.cluster.servers]
    print(server_cum_times)
    print(f'{[server.idle_time_triggers["idle_start_time"] for server in simulation.scheduler.cluster.servers]}')
    print(f'{[[times[0]/sum(times), times[1]/sum(times)] for times in server_cum_times]}')

    print(f'Average Job Latency: {sum(latencies)/simulation.NUM_JOBS}')
    print(f'Average Task Latency: {avg_task_latency}')
    print(f'Average time per job: {total_time/simulation.NUM_JOBS}')