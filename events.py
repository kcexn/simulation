import logging

if not __package__:
    from work import *
else:
    from .work import *

logger = logging.getLogger('Event')

class Event(object):
    """Events are asynchronous simulation actions that are queued."""
    ORDER = 0 # default constant for event order comparisons
    logger = logging.getLogger('Event')
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

class SimulationEvent(Event):
    """Generic Simulation Event The callback should return an event."""
    logger = logging.getLogger('Event.SimulationEvent')
    def __init__(self, simulation, arrival_time, callback, *args):
        super(SimulationEvent, self).__init__(simulation, arrival_time)
        self._callback = callback
        self._args = args
    
    def resolve(self):
        event = self._callback(*self._args)
        if isinstance(event, Event):
            self.simulation.event_queue.put(
                event
            )

class ControlEvent(SimulationEvent):
    logger = logging.getLogger('Event.SimulationEvent.ControlEvent')
    def __init__(self,simulation, arrival_time, callback, *args):
        super(ControlEvent, self).__init__(simulation,arrival_time,callback,*args)

class NetworkEvent(SimulationEvent):
    logger = logging.getLogger('Event.SimulationEvent.NetworkEvent')
    def __init__(self, simulation, arrival_time, callback, *args):
        super(NetworkEvent, self).__init__(simulation,arrival_time,callback,*args)
    
class Arrival(Event):
    """An arrival event"""
    logger = logging.getLogger('Event.Arrival')
    def __init__(self, simulation, arrival_time):
        super(Arrival,self).__init__(simulation, arrival_time)
    
class Completion(Event):
    """A Completion Event"""
    logger = logging.getLogger('Event.Completion')
    def __init__(self,simulation, completion_time):
        super(Completion,self).__init__(simulation, completion_time)

    def resolve(self):
        if self.canceled:
            raise RuntimeError(f'Event {self.id} has already been completed.')

class TaskArrival(Arrival):
    """Task Arrivals Need to be Assigned to Jobs"""
    logger = logging.getLogger('Event.Arrival.TaskArrival')
    def __init__(self, simulation, arrival_time):
        super(TaskArrival, self).__init__(simulation, arrival_time)

    def resolve(self):
        task = Task(self.simulation)
        self.logger.info(f'Task: {task.id} Start Time: {task.start_time}.')
        self.simulation.scheduler.schedule_task(task)

    def __repr__(self):
        return "TaskArrival"
    
class TaskCompletion(Completion):
    ORDER=1
    logger = logging.getLogger('Event.Completion.TaskCompletion')
    def __init__(self, simulation, task, completion_time, offset=0, server=None):
        super(TaskCompletion, self).__init__(simulation, completion_time)
        self.task = task
        self._interrenewal_time = completion_time - simulation.time - offset
        self.server=server

    @property
    def interrenewal_time(self):
        return self._interrenewal_time

    def resolve(self):
        try:
            super(TaskCompletion, self).resolve()
        except RuntimeError:
            self.logger.debug(f'Task {self.id} has already been completed. Simulation Time: {self.simulation.time}')
        else:
            self.server.logger.debug(f'Task: {self.task.id} has finished at time: {self.simulation.time}.')
            def scheduler_complete_task(scheduler=self.simulation.scheduler, task=self.task, server=self.server):
                scheduler.complete_task(task, server=server)
            self.simulation.event_queue.put(
                self.server.network.delay(
                    scheduler_complete_task, logging_message=f'Send message to scheduler from server: {self.server.id}, to complete task: {self.task.id}. Simulation Time: {self.simulation.time}.'
                )
            )
            self.server.complete_task(self.task)
            # self.simulation.scheduler.complete_task(self.task)

    def __repr__(self):
        return "TaskCompletion"

class JobArrival(Arrival):
    logger = logging.getLogger('Event.Arrival.JobArrival')
    def __init__(self, simulation, arrival_time, tasks=[], task_service_time=None):
        super(JobArrival, self).__init__(simulation, arrival_time)
        self.tasks=tasks

    def resolve(self):
        job = Job(self.simulation, tasks=self.tasks)
        self.logger.info(f'Job: {job.id}, start time: {job.start_time}.')
        self.simulation.scheduler.schedule_job(job)

    def __repr__(self):
        return "JobArrival"


class JobCompletion(Completion):
    ORDER=2
    logger = logging.getLogger('Event.Completion.JobCompletion')
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
            self.logger.info(f'Job {self.job.id} finish time: {self.job.finish_time}.')

    def __repr__(self):
        return "JobCompletion"
    

class NetworkDelay(NetworkEvent):
    """e2e Network Delay"""
    logger = logging.getLogger('Event.SimulationEvent.NetworkEvent.NetworkDelay')
    def __init__(self,simulation,arrival_time, callback,*args):
        super(NetworkDelay,self).__init__(simulation,arrival_time,callback,*args)
        self.logger.debug(f'Method: {callback}, arrival time: {arrival_time}, simulation time: {simulation.time}')

class BlockingDelay(ControlEvent):
    """Blocking Delay"""
    logger = logging.getLogger('Event.SimulationEvent.ControlEvent.BlockingDelay')
    def __init__(self,simulation,arrival_time, callback, *args):
        super(BlockingDelay, self).__init__(simulation,arrival_time,callback,*args)
        self.logger.debug(f'Method: {callback}, will be retried at time: {arrival_time}, simulation time: {simulation.time}')

class SparrowProbeEvent(ControlEvent):
    """Sparrow Probe Event"""
    logger = logging.getLogger('Event.SimulationEVent.ControlEvent.SparrowProbeEvent')
    def __init__(self, simulation, arrival_time, callback, *args):
        super(SparrowProbeEvent, self).__init__(simulation, arrival_time, callback, *args)
        self.logger.debug(f'Method: {callback}, will be called at time: {arrival_time}, simulation time: {simulation.time}')


__all__ = ['JobArrival', 'JobCompletion', 'TaskArrival', 'TaskCompletion', 'NetworkDelay', 'BlockingDelay','SparrowProbeEvent']