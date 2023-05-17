import logging


class Work(object):
    def __init__(self,simulation):
        self.simulation = simulation
        self.start_time = simulation.get_simulation_time()
        self.finish_time = None

    def get_id(self):
        return id(self)
    
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
    

class Task(Work):
    def __init__(self, simulation, job=None):
        super(Task, self).__init__(simulation)
        self.job = job

    def get_job(self):
        if self.job is not None:
            return self.job
        else:
            raise AttributeError(f'task {self.get_id()} does not belong to any jobs.')


class Job(Work):
    NUM_TASKS = 2
    """A Collection of Tasks"""
    def __init__(self,simulation):
        super(Job, self).__init__(simulation)
        self.tasks = [Task(simulation,job=self) for _ in range(Job.NUM_TASKS)]
    
    def get_tasks(self):
        return self.tasks