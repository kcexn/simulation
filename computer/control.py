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
    logger = logging.getLogger('computer.Control.SparrowBatch')
    def __init__(self, simulation, batch):
        super(SparrowBatch, self).__init__(simulation)
        self.batches = [batch]
        self.server_tasks = {}
        self.probes = set(SparrowProbe(simulation, task, batch_control=self) for task in batch)
        self.logger.debug(f'Probes in batch: {[probe for probe in self.probes]}. Simulation time: {self.simulation.time}.')

    @staticmethod
    def cleanup_control(fn):
        def func(*args):
            control = args[0]
            fn(*args)
            target = args[1]
            if target in control.bindings and control not in target.controls:
                control.logger.debug(f'{control}: {control.id}, Cleanup Loop: rebinding to {target}, {target.id}. Simulation Time: {control.simulation.time}')
                target.add_control(control)
        return func

    @cleanup_control
    def control(self, target):
        pass
    
    def bind(self, target, batch=None):
        if isinstance(target, ServerClass):
            self.server_bind(target, batch)
        elif isinstance(target, SchedulerClass):
            self.scheduler_bind(target, batch)

    def server_bind(self, target, batch=None):
        self.server_tasks[target] = batch
    
    def scheduler_bind(self, target, batch=None):
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
    from configure import SparrowScheduler
    from enum import IntEnum
    class States(IntEnum):
        terminated = -1
        server_probed = 0
        server_ready = 1
        server_executing_task = 2
        task_finished = 3
    states = States
    logger = logging.getLogger('computer.Control.SparrowProbe')
    def __init__(self, simulation, task, batch_control=None):
        super(SparrowProbe, self).__init__(simulation)
        self.creation_time = self.simulation.time
        self.server_arrival_times = {}
        self.server_queue_lengths = {}
        self.target_states = {}
        self.task = task
        self.batch_control = batch_control
        self.logger.debug(f'Task: {task.id} bound to probe: {self.id}. Simulation time: {self.simulation.time}.')
        # Sparrow has 5 states,
        # 0: Intialize Scheduler - Probe Server - Awaiting Server Response
        # 1: Server Response - Ready to Enqueue Task - Awaiting Scheduler Response
        # 2: Scheduler Response - Task Notify Server - Awaiting Server Response
        # 3: Task Completion - Notify Scheduler
        # -1: Terminated, Target to remove probe from controls.

        # Task Notify will contain one of two messages:
        # 0: Enqueue Task - Server Selected to Enqueue Task - Scheduler awaiting server task Completion notification
        # 1: Reject Task - Server Rejected to Enqueue Task - Server to Remove probe from controls.

    @property
    def probe_state(self):
        if len(self.target_states) > 0:
            state = max(self.target_states, key=lambda key: self.target_states[key])
            return self.target_states[state]
        else:
            return 0

    @property
    def enqueued(self):
        return self.probe_state >= self.states['server_executing_task']
    
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
            self.server_control(target)
        if isinstance(target, SchedulerClass):
            self.scheduler_control(target)
        
    def server_control(self, target):
        server = target
        match self.target_states[server]:
            case self.states.server_probed:
                self.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {self.id}, state: {self.states.server_probed}, simulation time: {self.simulation.time}')
                if server.busy_until == server.simulation.time:
                    probes_on_server = [control for control in server.controls if isinstance(control, SparrowProbe)]
                    earliest_probe = self
                    if len(probes_on_server) > 0:
                        probe = min(probes_on_server, key=lambda probe: probe.server_arrival_times[server])
                        if probe.server_arrival_times[server] < self.server_arrival_times[server]:
                            earliest_probe = probe
                    if earliest_probe is self:
                        event = server.network.delay(
                            self.server_enqueue_task, server, logging_message=f'Send message to scheduler, ready to enqueue task: {self.task.id} on server {server.id}. Simulation Time: {self.simulation.time}'
                        )
                        server.tasks[self.task] = event #Block subsequent probes. This will be overwritten by the server once the task is successfully enqueued.
                        self.target_states[target] = self.states.server_ready
                        self.simulation.event_queue.put(
                            event
                        )
                        self.target_states[server] = self.states.server_ready
            case self.states.server_ready:
                self.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {self.id}, state: {self.states.server_ready}, simulation time: {self.simulation.time}')
                if server.busy_until == self.simulation.time:
                    event = server.enqueue_task(self.task)
                    server.tasks[self.task] = event
                    self.simulation.event_queue.put(
                        event
                    )
                    self.target_states[server] = self.states.server_executing_task
            case self.states.server_executing_task:
                self.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {self.id}, state: {self.states.server_executing_task}, simulation time: {self.simulation.time}')
                pass
            case self.states.task_finished:
                self.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {self.id}, state: {self.states.task_finished}, simulation time: {self.simulation.time}')
                self.unbind(server)
            case self.states.terminated:
                self.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {self.id}, state: {self.states.terminated}, simulation time: {self.simulation.time}')
                self.unbind(server)

    def scheduler_control(self, target):
        pass

    @SparrowScheduler.Scheduler.Enqueuing.enqueue_task
    def server_enqueue_task(self, server):
        """Placeholder to have the scheduler task enqueuing policy injected from configuration."""
        pass

    def bind(self, target):
        """Add controls to the targets control list.
           Metadata required for control should be bound to the control object here."""
        if isinstance(target, ServerClass):
            self.server_bind(target)
        elif isinstance(target, SchedulerClass):
            self.scheduler_bind(target)

    def server_bind(self,target):
        if self not in target.controls:
            target.add_control(self)
            self.bindings.add(target)
            self.target_states[target] = self.states.server_probed
            self.server_arrival_times[target] = self.simulation.time
            self.server_queue_lengths[target] = len(target.tasks)
            self.logger.debug(
                f'Server: {target.id} bound to SparrowProbe: {self.id}, for task {self.task.id}. Registering arrival time {self.simulation.time}. Simulation time: { self.simulation.time}.'
            )

    def scheduler_bind(self,target):
        if self not in target.controls:
            target.add_control(self)
            self.bindings.add(target)
            self.target_states[target] = self.states.server_probed
            self.logger.debug(f'Scheduler bound to SparrowProbe: {self.id}, for task: {self.task.id}. Simulation Time: {self.simulation.time}.')
        
    def unbind(self, target):
        """Remove controls from targets control list."""
        self.logger.debug(f'Unbinding, {target}: {target.id}, from Sparrow Probe: {self.id}. Simulation Time: {self.simulation.time}')
        self.bindings.discard(target)
        self.target_states[target] = self.states.terminated
        while self in target.controls:
            target.controls.remove(self)

    def __del__(self):
        targets = [target for target in self.bindings]
        for target in targets:
            self.unbind(target)

__all__ = ['SparrowProbe']

