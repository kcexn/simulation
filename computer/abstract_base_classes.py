from abc import ABC, abstractmethod

class ServerClass(ABC):
    @property
    @abstractmethod
    def busy_until(self):
        pass

    @property
    @abstractmethod
    def id(self):
        return id(self)
    
    @abstractmethod
    def add_control(self,control):
        pass

    @abstractmethod
    def control(self):
        pass

    @abstractmethod
    def enqueue_task(self, task, interrenewal_time=0):
        pass

    @abstractmethod
    def complete_task(self, task):
        pass


class SchedulerClass(ABC):
    @abstractmethod
    def generate_arrivals(self):
        pass

    @abstractmethod
    def schedule_task(self, task, server):
        pass

    @abstractmethod
    def schedule_batch(self,batch):
        pass

    @abstractmethod
    def complete_task(self, task):
        pass

    @abstractmethod
    def schedule_job(self, job):
        pass

    @abstractmethod
    def complete_job(self, job):
        pass

    @abstractmethod
    def add_control(self, control):
        pass

    @abstractmethod
    def control(self):
        pass


class ControlClass(ABC):
    """A Generic Computer Control"""
    def __init__(self, simulation):
        self.simulation = simulation
        self.bindings = set()

    @staticmethod
    def cleanup_control(fn):
        def func(*args):
            fn(*args)
            control, target = tuple(args)
            if target in control.bindings:
                target.add_control(control)
        return func

    @property
    def id(self):
        return id(self)

    @abstractmethod
    def control(self, target):
        pass
    
    @abstractmethod
    def bind(self, target):
        pass
    
    @abstractmethod
    def unbind(self, target):
        pass
