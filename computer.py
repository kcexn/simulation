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

    def get_busy_until(self):
        events = [self.tasks[task] for task in list(self.tasks)]
        if len(events) == 0:
            logging.debug(f'server: {self.get_id()} is busy until: {self.simulation.get_simulation_time()}')
            return self.simulation.get_simulation_time()
        else:
            max_event = max(events)
            logging.debug(f'server: {self.get_id()} is busy until: {max_event.get_arrival_time()}')
            return max_event.get_arrival_time()
    
    def enqueue_task(self, task, interrenewal_time=0):
        offset = self.get_busy_until() - self.simulation.get_simulation_time()
        event = self.completion_process.get_task_completion(task, offset=offset, interrenewal_time=interrenewal_time)
        logging.debug(f'completion time for task {task.get_id()} is {event.get_arrival_time()}, executing on server {self.get_id()}')
        self.tasks[task] = event
        return event


    def complete_task(self, task):
        """Task completion is idempotent"""
        try:
            event = self.tasks.pop(task)
        except KeyError:
            pass
        else:
            time = event.get_arrival_time()
            if time > self.simulation.get_simulation_time():
                event.cancel()
            if task.get_job().is_finished():
                # Preempt the job, and move all subsequent tasks up in the queue.
                reschedule_tasks = [
                    task for task in list(self.tasks) if self.tasks[task].get_arrival_time() > time
                ]
                logging.debug(f'reschedule tasks: {reschedule_tasks} on server: {self.get_id()}.')
                if len(reschedule_tasks)>0:
                    events = sorted([self.tasks.pop(task) for task in reschedule_tasks])
                    for event,task in zip(events,reschedule_tasks):
                        event.cancel()
                        if not task.get_job().is_finished():
                            interrenewal_time = event.get_interrenewal_time()
                            self.simulation.event_queue.put(
                                self.enqueue_task(task, interrenewal_time=interrenewal_time)
                            )

    def get_id(self):
        return id(self)


class Cluster(object):
    """A Collection of Servers"""
    NUM_SERVERS = 6
    def __init__(self,simulation):
        self.servers = [Server(simulation) for _ in range(Cluster.NUM_SERVERS)]
        logging.debug(f'servers have ids: {[server.get_id() for server in self.servers]}')

    def get_servers(self):
        return self.servers
    
    def get_num_servers(self):
        return Cluster.NUM_SERVERS


class Scheduler(object):
    POLICY = 'LatinSquare' # Currently Support RoundRobin, FullRepetition, LatinSquare
    LATIN_SQUARE = array([
        [0,1,2],
        [1,2,0],
        [2,0,1]
    ])
    def __init__(self, simulation):
        self.simulation = simulation
        self.cluster = Cluster(simulation)
        self.arrival_process = ArrivalProcess(simulation)
        self.completion_process = CompletionProcess(simulation)
        self.counter = 0

    def generate_job_arrivals(self):
        for _ in range(self.simulation.NUM_JOBS):
            self.simulation.event_queue.put(self.arrival_process.get_job())

    def schedule_job_completion(self,job):
        self.simulation.event_queue.put(self.completion_process.get_job_completion(job))

    def schedule_task(self, task, server):
        """Enqueue the task and return a task completion time"""
        try:
            task.get_job()
        except AttributeError:
            self.simulation.work.append(task)
        self.simulation.event_queue.put(
            server.enqueue_task(task)
        )

    def schedule_batch(self,batch):
        """Enqueue batches of tasks round robin scheduling"""
        server = self.cluster.get_servers()[self.counter]
        self.counter = (self.counter+1)%self.cluster.get_num_servers()
        for task in batch:
            self.schedule_task(task, server)

    def complete_task(self, task):
        """Complete a task and dequeue from server"""
        try:
            task.set_finish_time(self.simulation.get_simulation_time())
        except AttributeError:
            pass
        except ValueError:
            pass
        for server in self.cluster.get_servers():
            server.complete_task(task)


    def schedule_job(self, job):
        """Schedule the tasks in the job"""
        self.simulation.work.append(job)
        tasks = job.get_tasks()

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
        time = self.simulation.get_simulation_time()
        try:
            job.set_finish_time(self.simulation.get_simulation_time())
            logging.debug(f'setting job finishing time to {time} for job: {job.get_id()}')
        except ValueError:
            pass
