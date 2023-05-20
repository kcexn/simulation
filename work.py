import logging


class Work(object):
    def __init__(self,simulation):
        self.simulation = simulation
        self._start_time = simulation.time
        self._finish_time = None # finish time needs to lock after the first write.

    @property
    def id(self):
        return id(self)
    
    @property
    def is_finished(self):
        return False if self._finish_time is None else True
    
    @property
    def start_time(self):
        return self._start_time
    
    @property
    def finish_time(self):
        if self._finish_time is not None:
            return self._finish_time
        else:
            raise ValueError(f'Task {self.id} not yet finished.')
    
    @finish_time.setter
    def finish_time(self,time):
        if self._finish_time is None:
            self._finish_time = time
        else:
            logging.debug(f'work item: {self.id} attempted to update finish time more than once')
            raise ValueError(f'Finish time can only be written to once.')
        
    def __repr__(self):
        return f'{self.__class__}: {self.id}'
    

class Task(Work):
    def __init__(self, simulation, job=None):
        super(Task, self).__init__(simulation)
        self._job = job

    @property
    def job(self):
        if self._job is not None:
            return self._job
        else:
            raise AttributeError(f'task {self.id} does not belong to any jobs.')
        
    @property
    def finish_time(self):
        return self._finish_time
    
    @finish_time.setter
    def finish_time(self,time):
        self._finish_time = time
        tasks = [task for task in self.job.tasks]
        if False not in (task.is_finished for task in tasks):
            job = self._job
            logging.debug(f'completion time: {time} for job: {job.id}')
            job.finish_time = time

class Job(Work):
    NUM_TASKS = 6
    """A Collection of Tasks"""
    def __init__(self,simulation):
        super(Job, self).__init__(simulation)
        self._tasks = [Task(simulation,job=self) for _ in range(Job.NUM_TASKS)]

    @property
    def tasks(self):
        return self._tasks
