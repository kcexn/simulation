import logging
from numpy import random

from events import TaskArrival,TaskCompletion,JobArrival,JobCompletion


class Process(object):
    """Generate Events"""
    def __init__(self, simulation):
        self.simulation=simulation
        self.rng = random.default_rng(seed=1)
    
    def get_interrenewal_time(self):
        return self.rng.exponential(1)
    
    def get_id(self):
        return id(self)
    
class ArrivalProcess(Process):
    """Generates Arrival Events"""
    INITIAL_TIME=0
    def __init__(self,simulation):
        super(ArrivalProcess, self).__init__(simulation)
        self.arrival_time = ArrivalProcess.INITIAL_TIME

    def get_task(self):
        self.arrival_time = self.arrival_time + self.get_interrenewal_time()
        event = TaskArrival(self.simulation, self.arrival_time)
        return event
    
    def get_job(self):
        self.arrival_time = self.arrival_time + self.get_interrenewal_time()
        event = JobArrival(self.simulation, self.arrival_time)
        return event
    
    def get_interrenewal_time(self):
        return self.rng.exponential(1/2)

    def get_arrival_time(self):
        return self.arrival_time
    

class CompletionProcess(Process):
    """Generates Completion Events"""
    def __init__(self,simulation):
        super(CompletionProcess,self).__init__(simulation)
    
    def get_task_completion(self, task, offset=0, interrenewal_time=0):
        if interrenewal_time > 0:
            completion_time = self.simulation.get_simulation_time() + interrenewal_time + offset
        else:
            completion_time = self.simulation.get_simulation_time() + self.get_interrenewal_time() + offset
        event = TaskCompletion(self.simulation, task, completion_time, offset=offset)
        return event
    
    def get_job_completion(self,job):
        """Job completion only get scheduled once all tasks in a job are complete"""
        return JobCompletion(self.simulation, job, self.simulation.get_simulation_time())



