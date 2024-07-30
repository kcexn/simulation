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
    

    @property
    def jobs(self):
        if not __package__:
            from configure import ArrivalProcessPolicies
        else:
            from .configure import ArrivalProcessPolicies
        while True:
            tasks = ArrivalProcessPolicies.JobArrivalPolicies.job_arrival_policy(self)
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
    service_time_params = {}
    def __init__(self,simulation):
        super(CompletionProcess,self).__init__(simulation)
        self.service_time_config = tuple(True if param=='true' else False for param in
            (self.simulation.CONFIGURATION['Processes.Completion.Task']['CORRELATED_TASKS'].lower(), 
             self.simulation.CONFIGURATION['Processes.Completion.Task']['HOMOGENEOUS_TASKS'].lower())
        )
        
    def estimated_service_time(self, task):
        # Some schedulers assume that accurate service time estimates can be made.
        # We implement an approximation of service time estimation with this property.
        # We assume that for correlated task servicing, extremely accurate estimates can be made from aggregate statistics, and so we return the exact service time value.
        # We assume that for uncorrelated task servicing, task service time estimates will simply return the mean or expected value of the task service time (which for us is unit).
        match self.service_time_config:
            case (True, True):
                # Correlated and Homogeneous
                if task.job in self.service_time_params:
                    self.logger.debug(f'job: {task.job.id} has a registered constant service time: {self.service_time_params[task.job]}')
                    return self.service_time_params[task.job]
                else:
                    self.logger.debug(f'job: {task.job.id} does not have a registered constant service time.')
                    service_time = next(self.interrenewal_times)
                    self.service_time_params[task.job] = service_time
                    return service_time
            case (True, False):
                # Correlated and Heterogeneous
                if task in self.service_time_params:
                    self.logger.debug(f'task: {task.id} has a registered service time: {self.service_time_params[task]}')
                    return self.service_time_params[task]
                else:
                    self.logger.debug(f'task: {task.id} does not have a registered scale parameter.')
                    service_time = next(self.interrenewal_times)
                    self.service_time_params[task] = service_time
                    return service_time
            case (False, True):
                # Uncorrelated and Homogeneous
                return 1
            case (False, False):
                # Correlated and Homogeneous
                return 1
        
    def get_task_completion(self, task, offset=0, server=None):
        completion_time=None
        if self.service_time_config == (True, True):
            # Correlated and Homogeneous
            if task.job in self.service_time_params:
                self.logger.debug(f'job: {task.job.id} has a registered constant service time: {self.service_time_params[task.job]}')
                completion_time = self.simulation.time + self.service_time_params[task.job] + offset
            else:
                self.logger.debug(f'job: {task.job.id} does not have a registered constant service time.')
                interrenewal_time = next(self.interrenewal_times)
                self.service_time_params[task.job] = interrenewal_time
                completion_time = self.simulation.time + interrenewal_time + offset
        elif self.service_time_config == (True, False):
            # Correlated and Heterogeneous
            if task in self.service_time_params:
                self.logger.debug(f'task: {task.id} has a registered service time: {self.service_time_params[task]}')
                completion_time = self.simulation.time + self.service_time_params[task] + offset
            else:
                self.logger.debug(f'task: {task.id} does not have a registered scale parameter.')
                interrenewal_time = next(self.interrenewal_times)
                self.service_time_params[task] = interrenewal_time
                completion_time = self.simulation.time + interrenewal_time + offset
        elif self.service_time_config == (False, True):
            # Uncorrelated and Homogeneous
            completion_time = self.simulation.time + next(self.interrenewal_times) + offset
        else:
            # Correlated and Homogeneous
            if task in self.service_time_params:
                self.logger.debug(f'task: {task.id} has a registered scale parameter.')
                completion_time = self.simulation.time + self.rng.exponential(self.service_time_params[task]) + offset
            else:
                self.logger.debug(f'task: {task.id} does not have a registered scale parameter.')
                scale_param = next(self.interrenewal_times)
                self.service_time_params[task] = scale_param
                completion_time = self.simulation.time + self.rng.exponential(scale_param) + offset
        event = TaskCompletion(self.simulation, task, completion_time, offset=offset, server=server)
        return event
    
    def get_job_completion(self,job):
        """Job completion only get scheduled once all tasks in a job are complete"""
        return JobCompletion(self.simulation, job, self.simulation.time)


__all__ = ['ArrivalProcess', 'CompletionProcess', 'NetworkDelayProcess', 'BlockingDelayProcess', 'RandomChoice']