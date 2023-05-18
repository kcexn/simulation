import logging

from work import Task,Job

class Event(object):
    """Events are asynchronous simulation actions that are queued."""
    ORDER = 0 # default constant for event order comparisons
    def __init__(self, simulation):
        self.simulation=simulation
        self.canceled = False
    
    def cancel(self):
        if self.canceled is False:
            self.canceled = True
        else:
            logging.debug('event {self.get_id()} is already canceled.')
            raise ValueError(f'event {self.get_id()} is already canceled.')

    def is_canceled(self):
        return self.canceled

    def resolve(self):
        raise NotImplementedError()

    def get_id(self):
        return id(self)
    
    def get_order(self):
        return self.ORDER
    
    def __eq__(self, other):
        return self.__class__ == other.__class__
    
    def __lt__(self, other):
        if other.__class__.__mro__[-2] == Event:
            return self.get_order() < other.get_order()
        else:
            raise TypeError(f"can't compare {self} with {other}. {other} doesn't extend Event.")

    def __repr__(self):
        raise NotImplementedError()
    
class Arrival(Event):
    """An arrival event"""
    def __init__(self, simulation):
        super(Arrival,self).__init__(simulation)
    
class Completion(Event):
    """A Completion Event"""
    def __init__(self,simulation):
        super(Completion,self).__init__(simulation)

    def resolve(self):
        if self.is_canceled():
            logging.debug(f'Event {self.get_id()} has already been completed.')
            raise RuntimeError(f'Event {self.get_id()} has already been completed.')

class TaskArrival(Arrival):
    def __init__(self, simulation):
        super(TaskArrival, self).__init__(simulation)

    def resolve(self):
        task = Task(self.simulation)
        logging.debug(f'start time: {task.get_start_time()}, task: {task.get_id()}')
        self.simulation.scheduler.schedule_task(task)

    def __repr__(self):
        return "TaskArrival"
    
class TaskCompletion(Completion):
    ORDER=1
    def __init__(self, simulation, task):
        super(TaskCompletion, self).__init__(simulation)
        self.task = task

    def resolve(self):
        try:
            super(TaskCompletion, self).resolve()
        except RuntimeError:
            pass
        else:
            self.simulation.scheduler.complete_task(self.task)
            logging.debug(f'finish time: {self.task.get_finish_time()}, task: {self.task.get_id()}')

    def __repr__(self):
        return "TaskCompletion"

class JobArrival(Arrival):
    def __init__(self, simulation):
        super(JobArrival, self).__init__(simulation)

    def resolve(self):
        job = Job(self.simulation)
        logging.debug(f'start time: {job.get_start_time()}, job: {job.get_id()}')
        self.simulation.scheduler.schedule_job(job)

    def __repr__(self):
        return "JobArrival"


class JobCompletion(Completion):
    ORDER=2
    def __init__(self,simulation, job):
        super(JobCompletion, self).__init__(simulation)
        self.job = job

    def resolve(self):
        try:
            super(JobCompletion,self).resolve()
        except RuntimeError:
            pass
        else:
            self.simulation.scheduler.complete_job(self.job)
            logging.debug(f'finish time: {self.job.get_finish_time()}, job: {self.job.get_id()}')

    def __repr__(self):
        return "JobCompletion"


