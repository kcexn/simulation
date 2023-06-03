import logging
from collections import deque
from numpy import array

from processes import *
from work import *
from .control import *
from .abstract_base_classes import ServerClass, SchedulerClass
from configure import latin_square

logger = logging.getLogger('Computer')

class Server(ServerClass):
    """Class to keep track of server status"""
    from configure import ServerTaskExecutionPolicies
    logger = logging.getLogger('Computer.Server')
    def __init__(self, simulation, network):
        self.simulation = simulation
        self.network = network
        self.tasks = {}
        self.blocker = BlockingDelayProcess(simulation)
        self.completion_process = CompletionProcess(simulation)
        self.max_queue_length = int(simulation.CONFIGURATION['Computer.Cluster.Server']['MAX_QUEUE_LENGTH'])
        self.controls = deque()

    @property
    def busy_until(self):
        events = [self.tasks[task] for task in list(self.tasks)]
        if len(events) == 0:
            self.logger.debug(f'server: {self.id} is currently idle. Simulation Time: {self.simulation.time}')
            return self.simulation.time
        else:
            max_event = max(events)
            if max_event.arrival_time > self.simulation.time:
                time = self.simulation.time
                self.logger.debug(f'server: {self.id} is busy until: {time}, simulation time: {self.simulation.time}')
                return time
            else:
                self.tasks = {} # Server is idle, reinitialize the task list.
                time = self.simulation.time
                self.logger.debug(f'server: {self.id} is currently idle. Simulation time: {self.simulation.time}')
                return time
    
    @property
    def id(self):
        return id(self)

    def add_control(self, control):
        self.controls.append(control)

    def control(self):
        self.logger.debug(
            f'Entering control loop of server: {self.id}, registered controls are: {[(control, control.id) for control in self.controls]}. Simulation time: {self.simulation.time}'
            )
        num_controls = len(self.controls)
        for _ in range(num_controls):
            control = self.controls.popleft()
            control.control(self)

    def enqueue_task(self, task):
        offset = self.busy_until - self.simulation.time
        event = self.completion_process.get_task_completion(task, offset=offset, server=self)
        self.tasks[task] = event
        self.logger.debug(f'completion time for task, {task.id}, is {event.arrival_time}, executing on server: {self.id}')
        return event

    @ServerTaskExecutionPolicies.task_completion
    def complete_task(self, task):
        """Task completion is idempotent.
        Placeholder for configuration injection.
        """
        pass


class Cluster(object):
    """A Collection of Servers"""
    from configure import ServerSelectionPolicies
    logger = logging.getLogger('Computer.Cluster')
    NUM_SERVERS = 6
    def __init__(self,simulation):
        self.simulation = simulation
        self.network = Network(simulation)
        self.NUM_SERVERS = int(simulation.CONFIGURATION['Computer.Cluster']['NUM_SERVERS'])
        self.logger.info(f'NUM_SERVERS: {self.NUM_SERVERS}')
        self._servers = [Server(simulation, self.network) for _ in range(self.NUM_SERVERS)]
        self.logger.info(f'servers have ids: {[server.id for server in self._servers]}')
        self.controls = deque()
        self.random_choice = RandomChoice(simulation)

    @property
    def servers(self):
        return self._servers
    
    @property
    def num_servers(self):
        return self.NUM_SERVERS

    @ServerSelectionPolicies.server_selection
    def __iter__(self):
        return iter(self._servers)
    
    def __len__(self):
        return len(self._servers)
    
    def add_control(self, control):
        self.controls.append(control)

    def control(self):
        for _ in range(len(self.controls)):
            control = self.controls.popleft()
            control.control()
    

class Network(object):
    """A Collection of Network Parameters and Functions"""
    logger = logging.getLogger('Computer.Network')
    def __init__(self,simulation):
        self.logger.info(f'Initialize Network.')
        self.simulation = simulation
        self.delay_process = NetworkDelayProcess(simulation)
        self.controls = deque()

    def delay(self, callback, *args, logging_message = ''):
        self.logger.info(logging_message)
        return self.delay_process.delay(callback, *args)
    
    def add_control(self, control):
        self.controls.append(control)

    def control(self):
        for _ in range(len(self.controls)):
            control = self.controls.popleft()
            control.resolve()


class Scheduler(SchedulerClass):
    from configure import SchedulingPolicies, BlockingPolicies, SchedulerTaskCompletionPolicies
    POLICY = 'LatinSquare' # Currently Support RoundRobin, FullRepetition, LatinSquare
    LATIN_SQUARE = array(latin_square(6))
    logger = logging.getLogger('Computer.Scheduler')
    def __init__(self, simulation):
        self.simulation = simulation
        self.POLICY = simulation.CONFIGURATION['Computer.Scheduler']['POLICY']
        self.LATIN_SQUARE = array(latin_square(int(simulation.CONFIGURATION['Computer.Scheduler']['LATIN_SQUARE_ORDER'])))
        self.logger.info(f'Scheduling Policy: {self.POLICY}, Latin Square: {self.LATIN_SQUARE}')
        self.cluster = Cluster(simulation)
        self.arrival_process = ArrivalProcess(simulation)
        self.completion_process = CompletionProcess(simulation)
        self.servers = iter(self.cluster)
        self.controls = deque()

    @property
    def id(self):
        return id(self)
                    
    def generate_arrivals(self):
        for idx,job in enumerate(self.arrival_process.jobs):
            if idx < self.simulation.NUM_JOBS:
                self.simulation.event_queue.put(job)
            else:
                break

    @BlockingPolicies.blocking
    def schedule_task(self, task, server):
        """Enqueue the task and return a task completion time"""
        if not task.is_finished and not task.job.is_finished:
            self.simulation.event_queue.put(
                server.enqueue_task(task)
            )
        else:
            self.logger.debug(f'The task: {task.id}, does not need to be enqueued on server: {server.id}. Simulation Time: {self.simulation.time}.')

    @SchedulingPolicies.batch_schedule
    def schedule_batch(self,batch):
        """Enqueue batches of tasks scheduling. Placeholder for configuration injection."""
        pass

    @SchedulerTaskCompletionPolicies.task_complete
    def complete_task(self, task, server=None):
        """Complete a task and dequeue from server"""
        try:
            task.finish_time = self.simulation.time
        except AttributeError as e:
            self.logger.debug(f'task: {task.id}, Attribute Error: {e}')
        except ValueError as e:
            self.logger.debug(f'task: {task.id}, value error: {e}')


    @SchedulingPolicies.scheduler
    def schedule_job(self, job):
        """Schedule the tasks in the job"""
        self.simulation.work.append(job)

    def complete_job(self, job):
        time = self.simulation.time
        try:
            job.set_finish_time(self.simulation.time)
            self.logger.info(f'setting job finishing time to {time} for job: {job.id}')
        except ValueError:
            pass

    def add_control(self, control):
        self.controls.append(control)

    def control(self):
        self.logger.debug(f'Entering Scheduler Control Loop. Simulation Time {self.simulation.time}')
        for _ in range(len(self.controls)):
            control = self.controls.popleft()
            control.control(self)

__all__ = ['Server','Cluster','Scheduler']