import logging
from numpy import random

from events import TaskArrival,TaskCompletion,JobArrival,JobCompletion


class Process(object):
    """Generate Events"""
    def __init__(self, simulation):
        self.simulation=simulation
        self.rng = random.default_rng()
    
    def get_interrenewal_time(self):
        return self.rng.exponential(
            self.rng.exponential(1)
        )
    
class ArrivalProcess(Process):
    """Generates Arrival Events"""
    INITIAL_TIME=0
    def __init__(self,simulation):
        super(ArrivalProcess, self).__init__(simulation)
        self.arrival_time = ArrivalProcess.INITIAL_TIME

    def get_task(self):
        self.arrival_time = self.arrival_time + self.get_interrenewal_time()
        event = TaskArrival(self.simulation)
        return (self.arrival_time, event)
    
    def get_job(self):
        self.arrival_time = self.arrival_time + self.get_interrenewal_time()
        event = JobArrival(self.simulation)
        return (self.arrival_time, event)

    def get_arrival_time(self):
        return self.arrival_time
    

class CompletionProcess(Process):
    """Execution Time Generation"""
    def __init__(self,simulation):
        super(CompletionProcess,self).__init__(simulation)
    
    def get_task_completion(self,task):
        completion_time = self.simulation.get_simulation_time() + self.get_interrenewal_time()
        event = TaskCompletion(self.simulation, task)
        return (completion_time, event)
    
    def get_job_completion(self,job):
        return JobCompletion(self.simulation, job)



