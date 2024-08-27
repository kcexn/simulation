import logging

class RoundRobinScheduler:
    """
    Round Robin Scheduling Methods
    """
    class Scheduler:
        class Queue:
            def round_robin(scheduler, job):
                from numpy import array_split
                tasks = job.tasks
                batches = [tuple(tasks.tolist()) for tasks in array_split(tasks,1)]
                for batch in batches:
                    batch_control = RoundRobinScheduler.Controls.RoundRobinBatch(scheduler.simulation, batch)
                    batch_control.bind(scheduler)
                scheduler.control()

        class Executor:
            def complete_task(scheduler, task, server=None):
                pass

        class Control:
            def scheduler_control(control, scheduler):
                match control.target_states[scheduler]:
                    case control.States.task_enqueued | control.States.task_executing:
                        return True
                    case control.States.task_finished | control.States.terminated:
                        control.unbind(scheduler)
                        return False
                    case _:
                        return True

                    
            def scheduler_batch_control(batch_control, scheduler):
                match batch_control.target_states[scheduler]:
                    case batch_control.States.blocked:
                        return True
                    case batch_control.States.terminated:
                        batch_control.unbind(scheduler)
                        return False
                    case batch_control.States.batch_unenqueued:
                        unenqueued_controls = [control for control in batch_control.controls if control.target_states[scheduler] == control.States.task_unenqueued]
                        for control in unenqueued_controls:
                            server = next(scheduler.servers)
                            def enqueue(server = server, control = control):
                                control.bind(server)
                                server.control()
                            event = scheduler.network.delay(
                                enqueue, logging_message = f'Send message to server: {server.id}, enqueue task: {control.task.id}. Simulation time: {scheduler.simulation.time}.'
                            )
                            scheduler.simulation.event_queue.put(
                                event
                            )
                            control.target_states[scheduler] = control.States.task_enqueued
                        batch_control.target_states[scheduler] = batch_control.States.batch_enqueued
                        return True
                    case batch_control.States.batch_enqueued:
                        return True
                
    class Server:
        class Queue:
            pass

        class Executor:
            def complete_task(server, task):
                control, = tuple(control for control in server.controls if task is control.task)
                control.target_states[server] = control.States.task_finished
                server.control()

        class Control:
            def server_control(control, server):
                match control.target_states[server]:
                    case control.States.blocked:
                        return True
                    case control.States.terminated:
                        return False
                    case control.States.task_unenqueued:
                        return True
                    case control.States.task_enqueued:
                        if server.busy_until == server.simulation.time:
                            server.logger.debug(f'Enqueuing task: {control.task.id}, on Server: {server.id}. Simulation time: {server.simulation.time}.')
                            event = server.enqueue_task(control.task)
                            server.simulation.event_queue.put(
                                event
                            )
                            control.target_states[server] = control.States.task_executing
                        return True              
                    case control.States.task_executing:
                        # Blocked on task completion event.
                        return True
                    case control.States.task_finished:
                        server.logger.debug(f'Finished executing task: {control.task.id}, on Server: {server.id}. Simulation time: {server.simulation.time}.')
                        server.stop_task_event(control.task)
                        control.unbind(server)
                        scheduler, = tuple(target for target in control.target_states if target.__class__.__name__ == 'Scheduler')
                        def scheduler_complete_task(scheduler=scheduler, task=control.task, server=server, control=control):
                            scheduler.logger.debug(f'Notified by server: {server.id}, that task: {task.id} is complete. Simulation time: {scheduler.simulation.time}.')
                            control.target_states[scheduler] = control.States.task_finished
                            scheduler.complete_task(task, server=server)
                        server.simulation.event_queue.put(
                            server.network.delay(
                                scheduler_complete_task, logging_message=f'Send message to scheduler from server: {server.id}, to complete task: {control.task.id}. Simulation Time: {server.simulation.time}.'
                            )
                        )
                        return False

    class Controls:
        if __package__ == 'schedulers':
            from computer import ControlClass
        else:
            from ..computer import ControlClass
        class RoundRobinTask(ControlClass):
            from enum import IntEnum
            if __package__ == 'schedulers':
                from computer import ControlClass
            else:
                from ..computer import ControlClass
            class States(IntEnum):
                blocked = -2
                terminated = -1
                task_unenqueued = 0
                task_enqueued = 1
                task_executing = 2
                task_finished = 3
            logger = logging.getLogger('Computer.Control.RoundRobinTask')
            def __init__(self, simulation, task, batch_control=None):
                super(RoundRobinScheduler.Controls.RoundRobinTask, self).__init__(simulation)
                self.batch_control = batch_control
                self.server_arrival_times = {}
                self.target_states = {}
                self.task = task

            @ControlClass.cleanup_control
            def control(self, target):
                match target.__class__.__name__:
                    case 'Server':
                        return RoundRobinScheduler.Server.Control.server_control(self, target)
                    case 'Scheduler':
                        return RoundRobinScheduler.Scheduler.Control.scheduler_control(self, target)

            def bind(self, target):
                match target.__class__.__name__:
                    case 'Server':
                        self.bind_server(target)
                    case 'Scheduler':
                        self.bind_scheduler(target)

            def bind_server(self, server):
                self.bindings.add(server)
                if self not in server.controls:
                    server.add_control(self)
                self.target_states[server] = self.States.task_enqueued

            def bind_scheduler(self, scheduler):
                self.bindings.add(scheduler)
                if self not in scheduler.controls:
                    scheduler.add_control(self)
                self.target_states[scheduler] = self.States.task_unenqueued

            def unbind(self, target):
                match target.__class__.__name__:
                    case 'Server':
                        self.unbind_server(target)
                    case 'Scheduler':
                        self.unbind_scheduler(target)

            def unbind_server(self, server):
                self.bindings.discard(server)
                while self in server.controls:
                    server.controls.remove(self)

            def unbind_scheduler(self, scheduler):
                self.bindings.discard(scheduler)
                while self in scheduler.controls:
                    scheduler.controls.remove(self)
                if not any(
                    'Server' in (target.__class__.__name__ for target in control.target_states) for control in self.batch_control.controls
                ):
                    self.batch_control.target_states[scheduler] = self.batch_control.States.terminated

        class RoundRobinBatch(ControlClass):
            from enum import IntEnum
            if __package__ == 'schedulers':
                from computer import ControlClass
            else:
                from ..computer import ControlClass
            class States(IntEnum):
                blocked = -2
                terminated = -1
                batch_unenqueued = 0 # When batches of tasks arrive at the scheduler, they are unenqueud
                batch_enqueued = 1 # After assigning batches of tasks to servers, they are enqueued.
            logger = logging.getLogger('Computer.Control.RoundRobinBatchControl')
            def __init__(self, simulation, batch):
                super(RoundRobinScheduler.Controls.RoundRobinBatch, self).__init__(simulation)
                self.batches = [batch]
                self.controls = set(
                    RoundRobinScheduler.Controls.RoundRobinTask(simulation, task, batch_control=self) for task in batch
                    )
                self.target_states = {}

            @ControlClass.cleanup_control
            def control(self, target):
                match target.__class__.__name__:
                    case 'Server':
                        return True
                    case 'Scheduler':
                        RoundRobinScheduler.Scheduler.Control.scheduler_batch_control(self, target)

            def bind(self, target):
                match target.__class__.__name__:
                    case 'Server':
                        pass
                    case 'Scheduler':
                        self.bind_scheduler(target)

            def bind_scheduler(self, scheduler):
                self.bindings.add(scheduler)
                if self not in scheduler.controls:
                    scheduler.add_control(self)
                for control in self.controls:
                    control.bind(scheduler)
                self.target_states[scheduler] = self.States.batch_unenqueued

            def unbind(self, target):
                match target.__class__.__name__:
                    case 'Server':
                        pass
                    case 'Scheduler':
                        self.unbind_scheduler(target)

            def unbind_scheduler(self, scheduler):
                self.bindings.discard(scheduler)
                while self in scheduler.controls:
                    scheduler.controls.remove(self)
                
