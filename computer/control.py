import logging
from collections import deque

from events import *
from .abstract_base_classes import ServerClass, SchedulerClass

class Control(object):
    """A Generic Computer Control"""
    logger = logging.getLogger('Computer.Control')
    def __init__(self, simulation):
        self.simulation = simulation

    @property
    def id(self):
        return id(self)

    def control(self, target):
        raise NotImplementedError
    
    def bind(self, target):
        raise NotImplementedError
    
    def unbind(self, target):
        raise NotImplementedError


class SparrowBatch(Control):
    """Sparrow Scheduler Batch
    management object for a collection of sparrow probes.
    """
    logger = logging.getLogger('computer.Control.Probe')
    def __init__(self, simulation, batch):
        super(SparrowBatch, self).__init__(simulation)
        self.schedulers = set()
        self.batches = [batch]
        self.servers = {}
        self.probes = [SparrowProbe(simulation, task, batch_control=self) for task in batch]

    def control(self, target):
        pass
    
    def bind(self, target, batch=None):
        if isinstance(target, ServerClass):
            self.servers[target] = batch
        elif isinstance(target, SchedulerClass):
            self.schedulers.add(target)
            if self not in target.controls:
                target.add_control(self)
            for probe in self.probes:
                probe.bind(target)

    def unbind(self, target):
        if isinstance(target, SchedulerClass):
            self.schedulers.discard(target)
            while self in target.controls:
                target.controls.remove(self)

    def __del__(self, target):
        schedulers = [scheduler for scheduler in self.schedulers]
        for scheduler in schedulers:
            self.unbind(scheduler)



class SparrowProbe(Control):
    """Sparrow Scheduler Probe"""
    from configure import SparrowConfiguration
    logger = logging.getLogger('computer.Control.Probe')
    def __init__(self, simulation, task, batch_control=None):
        super(SparrowProbe, self).__init__(simulation)
        self.creation_time = self.simulation.time
        self.bindings = set()
        self.server_arrival_times = {}
        self.task = task
        self.batch = batch_control
        self.enqueued = False

    @staticmethod
    def cleanup_control(fn):
        def func(*args):
            probe = args[0]
            fn(*args)
            probe.logger.debug(f'Probe: {probe.id}. Entering probe control cleanup loop. Simulation Time: {probe.simulation.time}')
            target = args[1]
            if target in probe.bindings and probe not in target.controls:
                probe.logger.debug(f'Probe: {probe.id}, rebinding to {target}, {target.id}')
                target.add_control(probe)
        return func

    @cleanup_control
    def control(self, target):
        """
        Target can be a server or a scheduler.
        """
        if isinstance(target, ServerClass):
            # Control loop for enqueuing tasks.
            self.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {self.id}, simulation time: {self.simulation.time}')
            server = target
            if server.busy_until <= server.simulation.time:
                # Server must be idle for this control to execute.
                probes_on_server = [control for control in server.controls if isinstance(control, SparrowProbe)]
                earliest_probe = self
                if len(probes_on_server) > 0:
                    probe = min(probes_on_server, key=lambda probe: probe.server_arrival_times[server])
                    if probe.server_arrival_times[server] < self.server_arrival_times[server]:
                        earliest_probe=probe
                if earliest_probe is self:
                    event = server.network.delay(
                        self.server_enqueue_task, server, logging_message=f'Send message to scheduler, ready to enqueue task: {self.task.id} on server {server.id}. Simulation Time: {self.simulation.time}'
                    )
                    server.tasks[self.task] = event #Block subsequent probes. This will be overwritten by the server once the task is successfully enqueued.
                    self.simulation.event_queue.put(
                        event
                    )
        if isinstance(target, SchedulerClass):
            # Control loop for completing tasks.
            if self.task.is_finished:
                self.logger.debug(f'Task: {self.task.id} has finished at time: {self.simulation.time}')
                servers = [binding for binding in self.bindings if isinstance(binding, ServerClass)]
                for server in servers:
                    server.control()
                del(self)


    @SparrowConfiguration.enqueue_task
    def server_enqueue_task(self, server):
        scheduler, = tuple(binding for binding in self.bindings if isinstance(binding,SchedulerClass))
        self.logger.debug(f'Scheduler has received response from Sparrow Probe for task: {self.task.id}, for server: {server.id}. Simulation Time: {self.simulation.time}')
        if not self.enqueued:
            self.enqueued = True
            self.logger.debug(f'Task: {self.task.id}, has not been enqueued yet. Enqueuing on Server: {server.id}, Simulation Time: {self.simulation.time}')
            bindings = [target for target in self.bindings]
            for binding in bindings:
                # Binding and Unbinding happens when the scheduler is in control.
                if binding is not server and isinstance(binding,ServerClass):
                    scheduler.cluster.network.delay(
                        self.unbind, binding, logging_message=f'Send Message to Server: {binding.id}, to remove task: {self.task.id} from queue of Sparrow Probes.'
                    )
                    self.unbind(binding)
            event = scheduler.cluster.network.delay(
                server.enqueue_task, self.task, logging_message=f'Send Message to Server: {server.id} to enqueue task: {self.task.id}. Simulation Time: {self.simulation.time}'
            )
            server.tasks[self.task] = event #Block subsequent probes. This will be overwritten by the server once the task is successfully enqueued.
            self.simulation.event_queue.put(
                event
            )
        else:
            self.logger.debug(f'Task: {self.task.id}, has been enqueued already. Rejecting the response.')
            self.unbind(server)
            event = scheduler.cluster.network.delay(
                server.control, logging_message=f'Send Message to Server: {server.id} do not enqueue task: {self.task.id}. Simulation Time: {self.simulation.time}'
            )
            self.simulation.event_queue.put(
                event
            )

    def bind(self, target):
        """Add controls to the targets control list.
           Metadata required for control should be bound to the control object here."""
        if isinstance(target, ServerClass):
            if self not in target.controls:
                self.logger.debug(f'Server: {target.id}, does not have a binding to SparrowProbe: {self.id}, for task: {self.task.id}. Simulation Time: {self.simulation.time}')
                target.add_control(self)
                self.bindings.add(target)
                if target not in self.server_arrival_times:
                    self.logger.debug(f'Registering arrival time: {self.simulation.time}, to Server: {target.id}, for task {self.task.id}. Simulation Time: {self.simulation.time}')
                    self.server_arrival_times[target] = self.simulation.time
        elif isinstance(target, SchedulerClass):
            if self not in target.controls:
                self.logger.debug(f'Scheduler does not have a binding to SparrowProbe: {self.id}, for task: {self.task.id}. Simulation Time: {self.simulation.time}.')
                target.add_control(self)
                self.bindings.add(target)
        
    def unbind(self, target):
        """Remove controls from targets control list."""
        self.logger.debug(f'Unbinding, {target}: {target.id}, from Sparrow Probe: {self.id}')
        self.bindings.discard(target)
        while self in target.controls:
            target.controls.remove(self)

    def __del__(self):
        targets = [target for target in self.bindings]
        for target in targets:
            self.unbind(target)

__all__ = ['SparrowProbe']

