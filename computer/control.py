import logging
from collections import deque

from events import *
from .abstract_base_classes import ServerClass, SchedulerClass

class Control(object):
    """A Generic Computer Control"""
    logger = logging.getLogger('Computer.Control')
    def __init__(self, simulation):
        self.simulation = simulation
        self.bindings = set()

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
        self.batches = [batch]
        self.server_tasks = {}
        self.probes = set(SparrowProbe(simulation, task, batch_control=self) for task in batch)

    def control(self, target):
        raise NotImplementedError
    
    def bind(self, target, batch=None):
        if isinstance(target, ServerClass):
            self.server_tasks[target] = batch
        elif isinstance(target, SchedulerClass):
            self.bindings.add(target)
            if self not in target.controls:
                target.add_control(self)
            for probe in self.probes:
                probe.bind(target)

    def unbind(self, target):
        if isinstance(target, ServerClass):
            del self.server_tasks[target]
            for probe in self.probes:
                probe.unbind(target)
        elif isinstance(target, SchedulerClass):
            self.schedulers.discard(target)
            while self in target.controls:
                target.controls.remove(self)

    def __del__(self):
        self.probes.clear()


class SparrowProbe(Control):
    """Sparrow Scheduler Probe"""
    from configure import SparrowConfiguration
    logger = logging.getLogger('computer.Control.Probe')
    def __init__(self, simulation, task, batch_control=None):
        super(SparrowProbe, self).__init__(simulation)
        self.creation_time = self.simulation.time
        self.server_arrival_times = {}
        self.task = task
        self.batch_control = batch_control
        self.enqueued = False

    @staticmethod
    def cleanup_control(fn):
        def func(*args):
            probe = args[0]
            fn(*args)
            target = args[1]
            if target in probe.bindings and probe not in target.controls:
                probe.logger.debug(f'Probe: {probe.id}, Cleanup Loop: rebinding to {target}, {target.id}. Simulation Time: {probe.simulation.time}')
                target.add_control(probe)
        return func

    @cleanup_control
    def control(self, target):
        """
        Target can be a server or a scheduler.
        Controls are popped from the left of the target controls before entering this loop.
        """
        if isinstance(target, ServerClass):
            # Control loop for enqueuing tasks.
            self.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {self.id}, simulation time: {self.simulation.time}')
            server = target
            if server.busy_until == server.simulation.time:
                # Server must be idle for this control to execute.
                probes_on_server = [control for control in server.controls if isinstance(control, SparrowProbe)]
                earliest_probe = self
                if len(probes_on_server) > 0:
                    probe = min(probes_on_server, key=lambda probe: probe.server_arrival_times[server])
                    if probe.server_arrival_times[server] < self.server_arrival_times[server]:
                        # If there are any other probes in the server probes that arrived before the current probe.
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
            # Scheduler Control loop for completing tasks.
            pass

    @SparrowConfiguration.enqueue_task
    def server_enqueue_task(self, server):
        """Placeholder to have the scheduler task enqueuing policy injected from configuration."""
        pass

    def bind(self, target):
        """Add controls to the targets control list.
           Metadata required for control should be bound to the control object here."""
        if isinstance(target, ServerClass):
            if self not in target.controls:
                target.add_control(self)
                self.bindings.add(target)
                if target not in self.server_arrival_times:
                    self.server_arrival_times[target] = self.simulation.time
                self.logger.debug(
                    f'Server: {target.id} bound to SparrowProbe: {self.id}, for task {self.task.id}. Registering arrival time: {self.simulation.time}. Simulation Time: {self.simulation.time}'
                )
        elif isinstance(target, SchedulerClass):
            if self not in target.controls:
                target.add_control(self)
                self.bindings.add(target)
                self.logger.debug(f'Scheduler bound to SparrowProbe: {self.id}, for task: {self.task.id}. Simulation Time: {self.simulation.time}.')

        
    def unbind(self, target):
        """Remove controls from targets control list."""
        self.logger.debug(f'Unbinding, {target}: {target.id}, from Sparrow Probe: {self.id}. Simulation Time: {self.simulation.time}')
        self.bindings.discard(target)
        while self in target.controls:
            target.controls.remove(self)

    def __del__(self):
        targets = [target for target in self.bindings]
        for target in targets:
            self.unbind(target)

__all__ = ['SparrowProbe']

