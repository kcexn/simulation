import logging

logger = logging.getLogger('Work')
class Work(object):
    logger = logging.getLogger('Work')
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
    
    @start_time.setter
    def start_time(self, time):
        self._start_time = time
    
    @property
    def finish_time(self):
        if self._finish_time is not None:
            return self._finish_time
        else:
            raise ValueError(f'Task {self.id} not yet finished.')
    
    @finish_time.setter
    def finish_time(self,time):
        if self._finish_time is None:
            self.logger.debug(f'{self}: {self.id} Finished at time: {time}. Simulation time: {self.simulation.time}.')
            self._finish_time = time
        else:
            self.logger.debug(f'work item: {self.id} attempted to update finish time more than once')
            raise ValueError(f'Finish time can only be written to once.')
        
    def __repr__(self):
        return f'{self.__class__}: {self.id}'
    

class Task(Work):
    logger = logging.getLogger('Work.Task')
    def __init__(self, simulation, job=None):
        super(Task, self).__init__(simulation)
        self._job = job

    @property
    def job(self):
        return self._job
    
    @job.setter
    def job(self, job):
        self._job = job
        
    @property
    def finish_time(self):
        return self._finish_time
    
    @finish_time.setter
    def finish_time(self,time):
        if self._finish_time is None:
            self._finish_time = time
        else:
            self.logger.debug(f'task: {self.id}, has already finished, and can not be updated twice, simulation time: {self.simulation.time}')
            raise ValueError(f'Finish time can only be written to once.')
        tasks = [task for task in self.job.tasks]
        if False not in (task.is_finished for task in tasks):
            job = self._job
            job.finish_time = time

class Job(Work):
    """A Collection of Tasks"""
    NUM_TASKS = 1
    logger = logging.getLogger('Work.Job')
    def __init__(self,simulation, tasks=[]):
        super(Job, self).__init__(simulation)
        self.NUM_TASKS = int(self.simulation.CONFIGURATION['Work.Job']['NUM_TASKS'])
        self.logger.debug(f'NUM_TASKS: {self.NUM_TASKS}')
        if len(tasks) == 0:
            self._tasks = [Task(simulation,job=self) for _ in range(self.NUM_TASKS)]
        elif len(tasks) == self.NUM_TASKS:
            self._tasks = tasks
            for task in self._tasks:
                task.job = self
        else:
            raise TypeError(f'A list of {len(tasks)} tasks was passed to the {self}, however, {self.NUM_TASKS} was expected.')

    @property
    def tasks(self):
        return self._tasks

__all__ = ['Task', 'Job']