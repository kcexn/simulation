import logging

from work import Task

class Event(object):
    """Events are asynchronous simulation actions that are queued."""
    def __init__(self, simulation):
        self.id = id(self)
        self.simulation=simulation
    
    def resolve(self):
        raise NotImplementedError()
    
    def get_id(self):
        raise NotImplementedError()
    
class Arrival(Event):
    """An arrival event"""
    def __init__(self, simulation):
        super(Arrival,self).__init__(simulation)
    
    def resolve(self):
        raise NotImplementedError()
    
    def get_id(self):
        raise NotImplementedError()
    
class Completion(Event):
    """A Completion Event"""
    def __init__(self,simulation):
        super(Completion,self).__init__(simulation)

    def resolve(self):
        raise NotImplementedError()
    
    def get_id(self):
        raise NotImplementedError
    
class TaskArrival(Arrival):
    def __init__(self, simulation):
        super(TaskArrival, self).__init__(simulation)

    def resolve(self):
        task = Task(self.simulation)
        logging.debug(f'start time: {task.get_start_time()}, task: {task.get_id()}')
        self.simulation.event_queue.put(self.simulation.completion_process.get_task_completion(task))
        self.simulation.tasks.append(task)

    def get_id(self):
        return self.id
    
class TaskCompletion(Completion):
    def __init__(self, simulation, task):
        super(TaskCompletion, self).__init__(simulation)
        self.task = task

    def resolve(self):
        self.task.set_finish_time(self.simulation.get_simulation_time())
        logging.debug(f'finish time: {self.task.get_finish_time()}, task: {self.task.get_id()}')

    def get_id(self):
        return self.id