import logging

from work import Task,Job

class Event(object):
    """Events are asynchronous simulation actions that are queued."""
    def __init__(self, simulation):
        self.simulation=simulation
    
    def resolve(self):
        raise NotImplementedError()
    
    def get_id(self):
        return id(self)
    
    def __eq__(self, other):
        return self.__class__ == other.__class__
    
    def __lt__(self,other):
        if other.__class__.__mro__[-2] == Event:
            return self.__repr__() > other.__repr__()
        else:
            raise TypeError(f"can't compare {self} with {other}. {other} doesn't extend Event.")

    def __repr__(self):
        raise NotImplementedError()
    
class Arrival(Event):
    """An arrival event"""
    def __init__(self, simulation):
        super(Arrival,self).__init__(simulation)
    
    def resolve(self):
        raise NotImplementedError()
    
    def __repr__(self):
        raise NotImplementedError()
    
class Completion(Event):
    """A Completion Event"""
    def __init__(self,simulation):
        super(Completion,self).__init__(simulation)

    def resolve(self):
        raise NotImplementedError()
    
    def __repr__(self):
        raise NotImplementedError()
    
class TaskArrival(Arrival):
    def __init__(self, simulation):
        super(TaskArrival, self).__init__(simulation)

    def resolve(self):
        task = Task(self.simulation)
        logging.debug(f'start time: {task.get_start_time()}, task: {task.get_id()}')
        self.simulation.event_queue.put(self.simulation.completion_process.get_task_completion(task))
        self.simulation.work.append(task)

    def __repr__(self):
        return "TaskArrival"
    
class TaskCompletion(Completion):
    def __init__(self, simulation, task):
        super(TaskCompletion, self).__init__(simulation)
        self.task = task

    def resolve(self):
        self.task.set_finish_time(self.simulation.get_simulation_time())
        logging.debug(f'finish time: {self.task.get_finish_time()}, task: {self.task.get_id()}')

    def __repr__(self):
        return "TaskCompetion"

class JobArrival(Arrival):
    def __init__(self, simulation):
        super(JobArrival, self).__init__(simulation)

    def resolve(self):
        job = Job(self.simulation)
        logging.debug(f'start time: {job.get_start_time()}, job: {job.get_id()}')
        task_events = [self.simulation.completion_process.get_task_completion(task) for task in job.tasks]
        max_time = max(task_events)[0]
        for task_event in task_events:
            self.simulation.event_queue.put(task_event)
        self.simulation.event_queue.put((max_time, self.simulation.completion_process.get_job_completion(job)))
        self.simulation.work.append(job)

    def __repr__(self):
        return "JobArrival"


class JobCompletion(Completion):
    def __init__(self,simulation, job):
        super(JobCompletion, self).__init__(simulation)
        self.job = job

    def resolve(self):
        time = self.simulation.get_simulation_time()
        self.job.set_finish_time(self.simulation.get_simulation_time())
        logging.debug(f'finish time: {self.job.get_finish_time()}, job: {self.job.get_id()}')

    def __repr__(self):
        return "JobCompletion"


