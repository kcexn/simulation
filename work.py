import logging


class Work(object):
    def __init__(self,simulation):
        self.simulation = simulation
        self.id = id(self)
        self.start_time = None
        self.finish_time = None

    def get_id(self):
        return self.id


class Task(Work):
    def __init__(self, simulation):
        super(Task, self).__init__(simulation)
        self.set_start_time(self.simulation.get_simulation_time())

    def is_finished(self):
        return False if self.finish_time is None else True
    
    def is_started(self):
        return False if self.start_time is None else True
    
    def set_start_time(self, time):
        self.start_time = time

    def get_start_time(self):
        if self.is_started():
            return self.start_time
        else:
            raise ValueError(f'Task {self.get_id()} not yet started.')
        
    def set_finish_time(self, time):
        self.finish_time = time

    def get_finish_time(self):
        if self.is_finished():
            return self.finish_time
        else:
            raise ValueError(f'Task {self.get_id()} not yet finished.')
        

class Job(Work):
    """A Collection of Tasks"""
    def __init__(self,simulation):
        super(Job, self).__init__(simulation)