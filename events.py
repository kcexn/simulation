import logging

from .work import *

class Event(object):
    """Events are asynchronous simulation actions that are queued."""
    ORDER = 0 # default constant for event order comparisons
    def __init__(self, simulation, arrival_time):
        self.simulation=simulation
        self._canceled = False
        self._arrival_time = arrival_time
    
    @property
    def canceled(self):
        return self._canceled

    @property
    def id(self):
        return id(self)

    @property
    def arrival_time(self):
        return self._arrival_time 
    
    @arrival_time.setter
    def arrival_time(self, time):
        self._arrival_time = time

    @property
    def order(self):
        return self.ORDER
    
    def cancel(self):
        self._canceled = True
    
    def resolve(self):
        raise NotImplementedError()
    
    def __eq__(self, other):
        try:
            return self._arrival_time == other._arrival_time
        except TypeError:
            raise TypeError(f"Can't compare {self} with {other}. {other} doesn't extend Event")
    
    def __lt__(self, other):
        try:
            return self._arrival_time < other._arrival_time
        except TypeError:
            raise TypeError(f"Can't compare {self} with {other}. {other} doesn't extend Event")

    def __repr__(self):
        raise NotImplementedError()
    
class Arrival(Event):
    """An arrival event"""
    def __init__(self, simulation, arrival_time):
        super(Arrival,self).__init__(simulation, arrival_time)
    
class Completion(Event):
    """A Completion Event"""
    def __init__(self,simulation, completion_time):
        super(Completion,self).__init__(simulation, completion_time)

    def resolve(self):
        if self.canceled:
            logging.debug(f'Event {self.id} has already been completed. Simulation Time: {self.simulation.time}')
            raise RuntimeError(f'Event {self.id} has already been completed.')

class TaskArrival(Arrival):
    def __init__(self, simulation, arrival_time):
        super(TaskArrival, self).__init__(simulation, arrival_time)

    def resolve(self):
        task = Task(self.simulation)
        logging.debug(f'start time: {task.start_time}, task: {task.id}')
        self.simulation.scheduler.schedule_task(task)

    def __repr__(self):
        return "TaskArrival"
    
class TaskCompletion(Completion):
    ORDER=1
    def __init__(self, simulation, task, completion_time, offset=0):
        super(TaskCompletion, self).__init__(simulation, completion_time)
        self.task = task
        self._interrenewal_time = completion_time - simulation.time - offset

    @property
    def interrenewal_time(self):
        return self._interrenewal_time

    def resolve(self):
        try:
            super(TaskCompletion, self).resolve()
        except RuntimeError:
            pass
        else:
            self.simulation.scheduler.complete_task(self.task)
            logging.debug(f'finish time: {self.task.finish_time}, task: {self.task.id}')

    def __repr__(self):
        return "TaskCompletion"

class JobArrival(Arrival):
    def __init__(self, simulation, arrival_time):
        super(JobArrival, self).__init__(simulation, arrival_time)

    def resolve(self):
        job = Job(self.simulation)
        logging.debug(f'start time: {job.start_time}, job: {job.id}')
        self.simulation.scheduler.schedule_job(job)

    def __repr__(self):
        return "JobArrival"


class JobCompletion(Completion):
    ORDER=2
    def __init__(self,simulation, job, arrival_time):
        super(JobCompletion, self).__init__(simulation, arrival_time)
        self.job = job

    def resolve(self):
        try:
            super(JobCompletion,self).resolve()
        except RuntimeError:
            pass
        else:
            self.simulation.scheduler.complete_job(self.job)
            logging.debug(f'finish time: {self.job.finish_time}, job: {self.job.id}')

    def __repr__(self):
        return "JobCompletion"


__all__ = ['JobArrival', 'JobCompletion', 'TaskArrival', 'TaskCompletion']