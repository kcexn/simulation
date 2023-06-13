import logging

if __package__ == 'computer':
    from events import *
    from .abstract_base_classes import ServerClass, SchedulerClass, ControlClass
else:
    from ..events import *
    from .abstract_base_classes import ServerClass, SchedulerClass, ControlClass

class LatinSquareBatch(ControlClass):
    """Latin Square Batch Control Management for collections of tasks.
    """
    logger = logging.getLogger('computer.Control.LatinSquareBatch')
    def __init__(self, simulation, batch):
        super(LatinSquareBatch, self).__init__(simulation)
        self.batches = [batch]
        self.controls = set(LatinSquareControl(simulation, task, batch_control=self) for task in batch)
        self.logger.debug(f'Controls in batch: {[control for control in self.controls]}. Simulation time: {self.simulation.time}.')

    @ControlClass.cleanup_control
    def control(self, target):
        pass

    def bind(self, target, batch=None):
        if isinstance(target, ServerClass):
            self.bind_server(target, batch)
        elif isinstance(target, SchedulerClass):
            self.bind_scheduler(target, batch)

    def bind_server(self, target, batch=None):
        self.server_tasks[target] = batch

    def bind_scheduler(self, target, batch=None):
        self.bindings.add(target)
        if self not in target.controls:
            target.add_control(self)
        for control in self.controls:
            control.bind(target)

    def unbind(self, target):
        if isinstance(target, ServerClass):
            del self.server_tasks[target]
            for probe in self.probes:
                probe.unbind(target)
        elif isinstance(target, SchedulerClass):
            completed_tasks = [control.task.is_finished for control in self.controls]
            if False not in completed_tasks:
                self.bindings.discard(target)
                while self in target.controls:
                    target.controls.remove(self)

    def __del__(self):
        self.controls.clear()


class LatinSquareControl(ControlClass):
    """Latin Square Scheduler Control
    """
    from enum import IntEnum
    if __package__ == 'computer':
        from configure import LatinSquareScheduler
    else:
        from ..configure import LatinSquareScheduler
    class States(IntEnum):
        blocked = -2
        terminated = -1
        server_enqueued = 0
        server_executing_task = 1
        task_finished = 2
    states = States
    logger = logging.getLogger('computer.Control.LatinSquareControl')
    def __init__(self, simulation, task, batch_control=None):
        super(LatinSquareControl, self).__init__(simulation)
        self.creation_time = self.simulation.time
        self.server_arrival_times = {}
        self.server_queue_lengths = {}
        self.target_states = {}
        self.task = task
        self.batch_control = batch_control
        self.logger.debug(f'Task: {task.id} bound to Control: {self.id}. Simulation time: {self.simulation.time}.')

    @property
    def control_state(self):
        if len(self.target_states) > 0:
            state = max(self.target_states, key=lambda key: self.target_states[key])
            return self.target_states[state]
        else:
            return 0
        
    @property
    def enqueued(self):
        return self.control_state >= self.states.server_executing_task

    @ControlClass.cleanup_control
    def control(self, target):
        if isinstance(target, ServerClass):
            self.server_control(target)
        elif isinstance(target, SchedulerClass):
            self.scheduler_control(target)

    @LatinSquareScheduler.Server.Control.server_control_select
    def server_control(self,target):
        pass

    @LatinSquareScheduler.Scheduler.Enqueuing.enqueue_task
    def server_enqueue_task(self,target):
        pass

    @LatinSquareScheduler.Scheduler.Control.scheduler_control_select
    def scheduler_control(self,target):
        if len(self.bindings) == 1 and self.task.is_finished:
            binding = self.bindings.pop()
            if binding is target:
                self.unbind(target)
            else:
                self.bindings.add(binding)

    def bind(self,target):
        if isinstance(target, ServerClass):
            self.bind_server(target)
        elif isinstance(target, SchedulerClass):
            self.bind_scheduler(target)
    
    def bind_server(self, target):
        if self not in target.controls:
            target.add_control(self)
            self.bindings.add(target)
            self.target_states[target] = self.states.server_enqueued
            self.server_arrival_times[target] = self.simulation.time
            self.server_queue_lengths[target] = len(target.tasks)
            self.logger.debug(
                f'Server: {target.id} bound to LatinSquare Control: {self.id}, for task {self.task.id}. Registering arrival time {self.simulation.time}. Simulation time: { self.simulation.time}.'
            )   

    def bind_scheduler(self, target):
        if self not in target.controls:
            target.add_control(self)
            self.bindings.add(target)
            self.target_states[target] = self.states.server_enqueued
            self.logger.debug(f'Scheduler bound to LatinSquare Control: {self.id}, for task: {self.task.id}. Simulation Time: {self.simulation.time}.')

    def unbind(self, target):
        """Remove controls from targets control list."""
        self.logger.debug(f'Unbinding, {target}: {target.id}, from LatinSquare Control: {self.id}. Simulation Time: {self.simulation.time}')
        self.bindings.discard(target)
        self.target_states[target] = self.states.terminated
        while self in target.controls:
            target.controls.remove(self)
        if len(self.bindings) == 0 and isinstance(target, SchedulerClass):
            self.batch_control.unbind(target)


    def __del__(self):
        targets = [target for target in self.bindings]
        for target in targets:
            self.unbind(target)
