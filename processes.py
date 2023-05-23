import logging
from numpy import random

if not __package__:
    from events import *
else:
    from .events import *

class Process(object):
    """Generate Events"""
    SEED_SEQUENCE = random.SeedSequence(12345)
    def __init__(self, simulation):
        self.simulation=simulation
        self.rng = random.default_rng(self.SEED_SEQUENCE.spawn(1)[0])

    @property
    def interrenewal_times(self):
        yield self.rng.exponential(1)
    
    @property
    def id(self):
        return id(self)
    
class ArrivalProcess(Process):
    """Generates Arrival Events"""
    INITIAL_TIME=0
    def __init__(self,simulation):
        super(ArrivalProcess, self).__init__(simulation)
        self._arrival_time = float(self.simulation.CONFIGURATION['Processes.Arrival']['INITIAL_TIME'])
        self._SHAPE = float(self.simulation.CONFIGURATION['Processes.Arrival']['SHAPE'])
        self._SCALE = float(self.simulation.CONFIGURATION['Processes.Arrival']['SCALE'])
        logging.debug(f'Initial Arrival Time: {self._arrival_time}, shape: {self._SHAPE}, scale: {self._SCALE}')
    
    @property
    def jobs(self):
        while True:
            self._arrival_time = self._arrival_time + next(self.interrenewal_times)
            yield JobArrival(self.simulation, self._arrival_time)
    
    @property
    def interrenewal_times(self):
        while True:
            yield self.rng.gamma(self._SHAPE,scale=self._SCALE)

    @property
    def arrival_time(self):
        return self._arrival_time
    
    @arrival_time.setter
    def arrival_time(self, time):
        self._arrival_time = time
    

class CompletionProcess(Process):
    """Generates Completion Events"""
    def __init__(self,simulation):
        super(CompletionProcess,self).__init__(simulation)
    
    def get_task_completion(self, task, offset=0, interrenewal_time=0):
        if interrenewal_time > 0:
            completion_time = self.simulation.time + interrenewal_time + offset
        else:
            completion_time = self.simulation.time + next(self.interrenewal_times) + offset
        event = TaskCompletion(self.simulation, task, completion_time, offset=offset)
        return event
    
    def get_job_completion(self,job):
        """Job completion only get scheduled once all tasks in a job are complete"""
        return JobCompletion(self.simulation, job, self.simulation.time)



__all__ = ['ArrivalProcess', 'CompletionProcess']