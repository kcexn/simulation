import logging
import numpy as np
from numpy import random

if not __package__:
    from events import *
    from work import *
else:
    from .events import *
    from .work import *

logger = logging.getLogger('Process')

class Process(object):
    """Generate Events"""
    SEED_SEQUENCE = random.SeedSequence(12345)
    logger = logging.getLogger('Process')
    def __init__(self, simulation):
        self.simulation=simulation
        self.rng = random.default_rng(self.SEED_SEQUENCE.spawn(1)[0])

    @property
    def interrenewal_times(self):
        while True:
            yield self.rng.exponential(1)
    
    @property
    def id(self):
        return id(self)
    
class RandomChoice(Process):
    """Used for np random choice"""
    logger = logging.getLogger('Process.RandomChoice')
    def __init__(self, simulation):
        super(RandomChoice, self).__init__(simulation)

    def choose_from(self, array, num_choices):
        indices = np.arange(len(array))
        while True:
            yield from (array[i] for i in self.rng.choice(indices, num_choices, replace=False, shuffle=False))

class NetworkDelayProcess(Process):
    """Generate Network Delays"""
    logger = logging.getLogger('Process.NetworkDelayProcess')
    def __init__(self, simulation):
        super(NetworkDelayProcess,self).__init__(simulation)
        self.mean = float(simulation.CONFIGURATION['Computer.Network']['MEAN_DELAY'])
        self.variance = float(simulation.CONFIGURATION['Computer.Network']['DELAY_STD'])**2
        self.scale = self.variance/self.mean
        self.shape = self.mean/self.scale
        self.logger.info(f'Delay Parameters: Gamma Distribution, Scale: {self.scale}, Shape: {self.shape}')

    @property
    def interrenewal_times(self):
        while True:
            yield self.rng.gamma(self.shape,scale=self.scale)

    def delay(self, callback, *args):
        arrival_time = self.simulation.time + next(self.interrenewal_times)
        return NetworkDelay(self.simulation, arrival_time, callback, *args)
    
class BlockingDelayProcess(Process):
    """Generate Exponential Wait Times when Blocked."""
    logger = logging.getLogger('Process.BlockingDelayProcess')
    def __init__(self,simulation):
        super(BlockingDelayProcess,self).__init__(simulation)
        self._scale = float(self.simulation.CONFIGURATION['Computer.Cluster.Server']['BLOCK_DELAY_SCALE'])

    @property
    def interrenewal_times(self):
        while True:
            yield self.rng.exponential(self._scale)

    def delay(self, callback, *args):
        arrival_time = self.simulation.time + next(self.interrenewal_times)
        return BlockingDelay(self.simulation, arrival_time, callback, *args)
    
class ArrivalProcess(Process):
    """Generates Arrival Events"""
    INITIAL_TIME=0
    logger = logging.getLogger('Process.ArrivalProcess')
    def __init__(self,simulation):
        super(ArrivalProcess, self).__init__(simulation)
        self.NUM_TASKS = int(self.simulation.CONFIGURATION['Work.Job']['NUM_TASKS'])
        self._arrival_time = float(self.simulation.CONFIGURATION['Processes.Arrival']['INITIAL_TIME'])
        self._SCALE = float(self.simulation.CONFIGURATION['Processes.Arrival']['SCALE'])
        self.logger.debug(f'Initial Arrival Time: {self._arrival_time}, num_tasks: {self.NUM_TASKS}, scale: {self._SCALE}')

    def job(self, tasks=[]):
        return JobArrival(self.simulation, self.simulation.time, tasks)
    
    @property
    def jobs(self):
        while True:
            tasks = []
            for _ in range(self.NUM_TASKS):
                self._arrival_time = self._arrival_time + next(self.interrenewal_times)
                task = Task(self.simulation)
                task.start_time = self._arrival_time
                tasks.append(task)
            yield JobArrival(self.simulation, self._arrival_time, tasks=tasks)
 

    @property
    def tasks(self):
        while True:
            self._arrival_time = self._arrival_time + next(self.interrenewal_times)
            yield TaskArrival(self.simulation, self._arrival_time)

    @property
    def interrenewal_times(self):
        while True:
            yield self.rng.exponential(self._SCALE)

    @property
    def arrival_time(self):
        return self._arrival_time
    
    @arrival_time.setter
    def arrival_time(self, time):
        self._arrival_time = time
    

class CompletionProcess(Process):
    """Generates Completion Events"""
    logger = logging.getLogger('Process.CompletionProcess')
    job_task_service_times = {}
    def __init__(self,simulation):
        super(CompletionProcess,self).__init__(simulation)

    def get_task_completion(self, task, offset=0, server=None):
        completion_time=None
        match self.simulation.CONFIGURATION['Processes.Completion.Task']['POLICY']:
            case 'RandomJob':
                if task.job in self.job_task_service_times:
                    self.logger.debug(f'job: {task.job} has a registered constant service time: {self.job_task_service_times[task.job]}')
                    completion_time = self.simulation.time + self.job_task_service_times[task.job] + offset
                else:
                    self.logger.debug(f'job: {task.job} does not have a registered constant service time.')
                    interrenewal_time = next(self.interrenewal_times)
                    self.job_task_service_times[task.job] = interrenewal_time
                    completion_time = self.simulation.time + interrenewal_time + offset
            case 'RandomTask':
                completion_time = self.simulation.time + next(self.interrenewal_times) + offset
        event = TaskCompletion(self.simulation, task, completion_time, offset=offset, server=server)
        return event
    
    def get_job_completion(self,job):
        """Job completion only get scheduled once all tasks in a job are complete"""
        return JobCompletion(self.simulation, job, self.simulation.time)


__all__ = ['ArrivalProcess', 'CompletionProcess', 'NetworkDelayProcess', 'BlockingDelayProcess', 'RandomChoice']