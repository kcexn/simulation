import logging
from copy import copy
from math import ceil
from numpy import array_split, array

if not __package__:
    from processes import *
    from configure import latin_square
else:
    from .processes import *
    from .configure import latin_square
class Server(object):
    """Class to keep track of server status"""
    def __init__(self, simulation, network):
        self.simulation = simulation
        self.network = network
        self.tasks = {}
        self.completion_process = CompletionProcess(simulation)

    @property
    def busy_until(self):
        events = [self.tasks[task] for task in list(self.tasks)]
        if len(events) == 0:
            logging.debug(f'server: {self.id} is currently idle. Simulation Time: {self.simulation.time}')
            return self.simulation.time
        else:
            max_event = max(events)
            logging.debug(f'server: {self.id} is busy until: {max_event.arrival_time}, simulation time: {self.simulation.time}')
            return max_event.arrival_time
    
    def enqueue_task(self, task, interrenewal_time=0):
        offset = self.busy_until - self.simulation.time
        event = self.completion_process.get_task_completion(task, offset=offset, interrenewal_time=interrenewal_time)
        self.tasks[task] = event
        logging.debug(f'completion time for task, {task.id}, is {event.arrival_time}, executing on server: {self.id}')
        return event


    def complete_task(self, task):
        """Task completion is idempotent"""
        try:
            logging.debug(f'server: {self.id}, checking if task: {task.id} is in the queue. Simulation Time: {self.simulation.time}')
            event = self.tasks.pop(task)
            logging.debug(f'server: {self.id}, clearing task: {task.id} from queue, at time: {self.simulation.time}')
        except KeyError:
            policy = self.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']
            if policy == 'FullRepetition' or policy == 'LatinSquare':
                logging.debug(f'task: {task.id} is not in the queue of server: {self.id}. Simulation time: {self.simulation.time}')
        else:
            time = event.arrival_time
            event.cancel()
            if task.job.is_finished and time > self.simulation.time:
                # Preempt the job, and move all subsequent tasks up in the queue.
                reschedule_tasks = [
                    task for task in list(self.tasks) if self.tasks[task].arrival_time > time
                ]
                if len(reschedule_tasks)>0:
                    logging.debug(f'reschedule tasks: {reschedule_tasks} on server: {self.id}.')
                    events = sorted([self.tasks.pop(task) for task in reschedule_tasks])
                    for event,task in zip(events,reschedule_tasks):
                        if not task.job.is_finished:
                            new_event = copy(event)
                            new_event.arrival_time = event.arrival_time - time + self.simulation.time
                            logging.debug(f'task {task.id} rescheduled to complete on server: {self.id} at time: {new_event.arrival_time}')
                            self.simulation.event_queue.put(
                                new_event
                            )
                            self.tasks[task] = new_event
                        else:
                            logging.debug(f'task {task.id} is already associated to a job that is complete, at time:{event.arrival_time}')
                        event.cancel()


    @property
    def id(self):
        return id(self)


class Cluster(object):
    """A Collection of Servers"""
    NUM_SERVERS = 6
    def __init__(self,simulation):
        self.network = Network(simulation)
        self.NUM_SERVERS = int(simulation.CONFIGURATION['Computer.Cluster']['NUM_SERVERS'])
        logging.debug(f'NUM_SERVERS: {self.NUM_SERVERS}')
        self._servers = [Server(simulation, self.network) for _ in range(self.NUM_SERVERS)]
        logging.debug(f'servers have ids: {[server.id for server in self.servers]}')

    @property
    def servers(self):
        return self._servers
    
    @property
    def num_servers(self):
        return self.NUM_SERVERS
    

class Network(object):
    """A Collection of Network Parameters and Functions"""
    def __init__(self,simulation):
        logging.debug(f'Initialize Network.')
        self.simulation = simulation
        self.delay_process = NetworkDelayProcess(simulation)

    def delay(self, callback, *args):
        return self.delay_process.delay(callback, *args)


class Scheduler(object):
    POLICY = 'LatinSquare' # Currently Support RoundRobin, FullRepetition, LatinSquare
    # LATIN_SQUARE = array([
    #     [0,1],
    #     [1,0]
    # ])
    # LATIN_SQUARE = array([
    #     [0,1,2],
    #     [1,2,0],
    #     [2,0,1]
    # ])
    LATIN_SQUARE = array(latin_square(6))
    def __init__(self, simulation):
        self.simulation = simulation
        self.unassigned_tasks = []
        self.NUM_TASKS = int(self.simulation.CONFIGURATION['Work.Job']['NUM_TASKS'])
        self.POLICY = simulation.CONFIGURATION['Computer.Scheduler']['POLICY']
        self.LATIN_SQUARE = array(latin_square(int(simulation.CONFIGURATION['Computer.Scheduler']['LATIN_SQUARE_ORDER'])))
        logging.debug(f'Scheduling Policy: {self.POLICY}, Latin Square: {self.LATIN_SQUARE}')
        self.cluster = Cluster(simulation)
        self.arrival_process = ArrivalProcess(simulation)
        self.completion_process = CompletionProcess(simulation)
        self.counter = 0

    def generate_arrivals(self):
        for idx,job in enumerate(self.arrival_process.jobs):
            if idx < self.simulation.NUM_JOBS:
                self.simulation.event_queue.put(job)
            else:
                break

    def schedule_job_completion(self,job):
        self.simulation.event_queue.put(self.completion_process.get_job_completion(job))

    def schedule_task(self, task, server=None):
        """Enqueue the task and return a task completion time"""
        try:
            job = task.job
        except AttributeError:
            # If task is not assigned to a job, batch this task with other unassigned tasks, and schedule a job arrival.
            self.unassigned_tasks.append(task)
            if len(self.unassigned_tasks) == self.NUM_TASKS:
                self.simulation.event_queue.put(
                    self.arrival_process.job(self.unassigned_tasks)
                )
                self.unassigned_tasks = []
        else:
            if not server:
                # If no server is provided assume the current one.
                server = self.cluster.servers[self.counter]
                self.simulation.event_queue.put(
                    self.cluster.network.delay(
                        server.enqueue_task, task
                    )
                )
            else:
                self.simulation.event_queue.put(
                    self.cluster.network.delay(
                        server.enqueue_task, task
                    )
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
        else:
            logging.debug(f'finish time: {task.finish_time}, for task: {task.id}')
            for server in self.cluster.servers:
                self.simulation.event_queue.put(
                    self.cluster.network.delay(
                        server.complete_task,task
                    )
                )

    def schedule_job(self, job):
        """Schedule the tasks in the job"""
        self.simulation.work.append(job)
        tasks = job.tasks

        if self.POLICY == 'RoundRobin':
            batch_size = 1
        elif self.POLICY == 'FullRepetition':
            batch_size = len(tasks)
        elif self.POLICY == 'LatinSquare':
            logging.debug(f'Latin Square order is {self.LATIN_SQUARE.shape[0]}')
            batch_size = self.LATIN_SQUARE.shape[0]

        work = [el.tolist() for el in array_split(tasks, ceil(len(tasks)/batch_size))]
        logging.debug(f'work batches to be scheduled are {work}')
        for batch in work:
            for i in range(len(batch)):
                if self.POLICY == 'RoundRobin':
                    self.schedule_batch(batch)
                elif self.POLICY == 'LatinSquare':
                    scheduled_order = [batch[self.LATIN_SQUARE[i][j]] for j in range(len(batch))]
                    self.schedule_batch(scheduled_order)
                elif self.POLICY == 'FullRepetition':
                    for _ in range(self.cluster.num_servers):
                        self.schedule_batch(batch)
                

    def complete_job(self, job):
        time = self.simulation.time
        try:
            job.set_finish_time(self.simulation.time)
            logging.debug(f'setting job finishing time to {time} for job: {job.id}')
        except ValueError:
            pass

__all__ = ['Server','Cluster','Scheduler']