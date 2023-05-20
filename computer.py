import logging
from math import ceil
from numpy import array_split, array

from processes import CompletionProcess, ArrivalProcess


class Server(object):
    """Class to keep track of server status"""
    def __init__(self, simulation):
        self.simulation = simulation
        self.tasks = {}
        self.completion_process = CompletionProcess(simulation)

    @property
    def busy_until(self):
        events = [self.tasks[task] for task in list(self.tasks)]
        if len(events) == 0:
            logging.debug(f'server: {self.id} is busy until: {self.simulation.time}')
            return self.simulation.time
        else:
            max_event = max(events)
            logging.debug(f'server: {self.id} is busy until: {max_event.arrival_time}')
            return max_event.arrival_time
    
    def enqueue_task(self, task, interrenewal_time=0):
        offset = self.busy_until - self.simulation.time
        event = self.completion_process.get_task_completion(task, offset=offset, interrenewal_time=interrenewal_time)
        logging.debug(f'completion time for task {task.id} is {event.arrival_time}, executing on server {self.id}')
        self.tasks[task] = event
        return event


    def complete_task(self, task):
        """Task completion is idempotent"""
        try:
            event = self.tasks.pop(task)
        except KeyError:
            pass
        else:
            time = event.arrival_time
            if time > self.simulation.time:
                event.cancel()
            if task.job.is_finished:
                # Preempt the job, and move all subsequent tasks up in the queue.
                reschedule_tasks = [
                    task for task in list(self.tasks) if self.tasks[task].arrival_time > time
                ]
                logging.debug(f'reschedule tasks: {reschedule_tasks} on server: {self.id}.')
                if len(reschedule_tasks)>0:
                    events = sorted([self.tasks.pop(task) for task in reschedule_tasks])
                    for event,task in zip(events,reschedule_tasks):
                        event.cancel()
                        if not task.job.is_finished:
                            self.simulation.event_queue.put(
                                self.enqueue_task(task, interrenewal_time=event.interrenewal_time)
                            )

    @property
    def id(self):
        return id(self)


class Cluster(object):
    """A Collection of Servers"""
    NUM_SERVERS = 6
    def __init__(self,simulation):
        self._servers = [Server(simulation) for _ in range(Cluster.NUM_SERVERS)]
        logging.debug(f'servers have ids: {[server.id for server in self.servers]}')

    @property
    def servers(self):
        return self._servers
    
    @property
    def num_servers(self):
        return Cluster.NUM_SERVERS


class Scheduler(object):
    POLICY = 'LatinSquare' # Currently Support RoundRobin, FullRepetition, LatinSquare
    LATIN_SQUARE = array([
        [0,1,2,3,4,5],
        [1,2,3,4,5,0],
        [2,3,4,5,0,1],
        [3,4,5,0,1,2],
        [4,5,0,1,2,3],
        [5,0,1,2,3,4]
    ])
    def __init__(self, simulation):
        self.simulation = simulation
        self.cluster = Cluster(simulation)
        self.arrival_process = ArrivalProcess(simulation)
        self.completion_process = CompletionProcess(simulation)
        self.counter = 0

    def generate_job_arrivals(self):
        for idx,job in enumerate(self.arrival_process.jobs):
            if idx < self.simulation.NUM_JOBS:
                self.simulation.event_queue.put(job)
            else:
                break

    def schedule_job_completion(self,job):
        self.simulation.event_queue.put(self.completion_process.get_job_completion(job))

    def schedule_task(self, task, server):
        """Enqueue the task and return a task completion time"""
        try:
            task.job
        except AttributeError:
            self.simulation.work.append(task)
        self.simulation.event_queue.put(
            server.enqueue_task(task)
        )

    def schedule_batch(self,batch):
        """Enqueue batches of tasks round robin scheduling"""
        server = self.cluster.servers[self.counter]
        self.counter = (self.counter+1)%self.cluster.num_servers
        for task in batch:
            self.schedule_task(task, server)

    def complete_task(self, task):
        """Complete a task and dequeue from server"""
        try:
            task.finish_time = self.simulation.time
        except AttributeError:
            pass
        except ValueError:
            pass
        for server in self.cluster.servers:
            server.complete_task(task)


    def schedule_job(self, job):
        """Schedule the tasks in the job"""
        self.simulation.work.append(job)
        tasks = job.tasks

        if Scheduler.POLICY == 'RoundRobin':
            batch_size = 1
        elif Scheduler.POLICY == 'FullRepetition':
            batch_size = len(tasks)
        elif Scheduler.POLICY == 'LatinSquare':
            logging.debug(f'Latin Square order is {Scheduler.LATIN_SQUARE.shape[0]}')
            batch_size = Scheduler.LATIN_SQUARE.shape[0]

        work = [el.tolist() for el in array_split(tasks, ceil(len(tasks)/batch_size))]
        logging.debug(f'work batches to be scheduled are {work}')
        for batch in work:
            for i in range(len(batch)):
                if Scheduler.POLICY == 'LatinSquare':
                    scheduled_order = [batch[Scheduler.LATIN_SQUARE[i][j]] for j in range(len(batch))]
                else:
                    scheduled_order = batch
                self.schedule_batch(scheduled_order)
                

    def complete_job(self, job):
        time = self.simulation.time
        try:
            job.set_finish_time(self.simulation.time)
            logging.debug(f'setting job finishing time to {time} for job: {job.id}')
        except ValueError:
            pass
