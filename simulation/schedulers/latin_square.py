import logging
class LatinSquareScheduler:
    class Scheduler:
        class Enqueuing:
            def latin_square(scheduler, job):
                from numpy import array_split
                from math import ceil
                from collections import deque
                tasks = job.tasks
                try:
                    scheduling_params = scheduler.SCHEDULING_PARAMS
                except AttributeError:
                    scheduling_params = {
                        'latin_square_order': scheduler.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['LATIN_SQUARE_ORDER'],
                        'num_probes_per_batch': scheduler.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['NUM_PROBES_PER_BATCH'],
                        'preemption': scheduler.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['PREEMPTION']
                    }
                    scheduler.SCHEDULING_PARAMS = scheduling_params
                else:
                    pass
                finally:
                    latin_square_order = int(scheduling_params['latin_square_order'])
                    batches = [tuple(tasks.tolist()) for tasks in array_split(tasks,ceil(len(tasks))/latin_square_order)]
                    scheduler.logger.info(f'Work batches to be scheduled are {[(task.id, task.start_time) for batch in batches for task in batch]}. Simulation Time: {scheduler.simulation.time}')
                    for batch in batches:
                        controls = [LatinSquareScheduler.Controls.LatinSquareControl(scheduler.simulation, task) for task in batch]
                        for i in range(latin_square_order):
                            batch_controls = deque(controls)
                            batch_controls.rotate(-i)
                            batch_control = LatinSquareScheduler.Controls.LatinSquareBatch(scheduler.simulation, batch_controls)
                            batch_control.bind(scheduler)
                            batch_control.control(scheduler)

        class Executor:
            def task_complete(scheduler, task, server=None):
                pass

        class Control:
            def scheduler_coroutine_control(control, scheduler, server=None):
                match control.target_states[server]:
                    case control.States.server_executing_task:
                        if control.task.is_finished:
                            def preempt_task(control = control, server = server):
                                server.logger.debug(f'Server: {server.id} has been notified that task: {control.task.id} has already been completed. Simulation time: {control.simulation.time}.')
                                control.target_states[server] = control.States.terminated
                                server.control()
                            event = scheduler.network.delay(
                                preempt_task, logging_message=f'Notify server: {server.id} that task: {control.task.id} has already been completed. Simulation time: {control.simulation.time}.'
                            )
                            scheduler.simulation.event_queue.put(
                                event
                            )
                        else:
                            preemption = scheduler.SCHEDULING_PARAMS['preemption']
                            if preemption.lower() == 'true':
                                server_idx = control.server_batch_index[server]
                                batch_control = control.batch_controls[server_idx]
                                if batch_control.target_states[scheduler] != batch_control.States.enqueued:
                                    servers = [serv for serv in control.server_batch_index if control.server_batch_index[serv] == server_idx and serv is not server]
                                    for server in servers:
                                        def preempt_tasks(batch_control = batch_control, server = server):
                                            server.logger.debug(f'Server: {server.id} has been notified that tasks: {[control.task.id for control in batch_control.controls]} have been enqueued. Simulation time: {batch_control.simulation.time}.')
                                            for control in batch_control.controls:
                                                if control.target_states[server] != control.States.terminated:
                                                    control.target_states[server] = control.States.terminated
                                            server.control()
                                        event = scheduler.network.delay(
                                            preempt_tasks, logging_message = f'Notify server: {server.id} that task: {control.task.id} has already been enqueued. Simulation time: {control.simulation.time}.'
                                        )
                                        scheduler.simulation.event_queue.put(
                                            event
                                        )
                                    batch_control.target_states[scheduler] = batch_control.States.enqueued
                            else:
                                batch_control.target_states[scheduler] = batch_control.States.enqueued
                    case control.States.task_finished:
                        if control.target_states[scheduler] in [control.States.task_finished, control.States.terminated]:
                            servers = [target for target in control.target_states if target.__class__.__name__ == 'Server' and target is not server]
                            for server in servers:
                                def preempt_task(control=control, server=server):
                                    server.logger.debug(f'Server: {server.id} has been notified that task: {control.task.id} has been completed. Simulation time: {control.simulation.time}.')
                                    if control.target_states[server] != control.States.task_finished:
                                        control.target_states[server] = control.States.terminated
                                        server.control()
                                event = scheduler.network.delay(
                                    preempt_task, logging_message=f'Notify server: {server.id} that task: {control.task.id} has been completed. Simulation time: {control.simulation.time}.'
                                )
                                scheduler.simulation.event_queue.put(
                                    event
                                )
                            control.unbind(scheduler)
                            control.target_states[scheduler] = control.States.terminated                    
                    case _:
                        pass
            
            def scheduler_control(control, scheduler):
                match control.target_states[scheduler]:
                    case control.States.terminated | control.States.task_finished:
                        return False
                    case _:
                        return True
                    
            def scheduler_batch_control(batch_control, scheduler):
                match batch_control.target_states[scheduler]:
                    case batch_control.States.unenqueued:
                        num_probes_per_batch = int(scheduler.SCHEDULING_PARAMS['num_probes_per_batch'])
                        idx = batch_control.controls[0].batch_control_indices[batch_control]
                        for _ in range(num_probes_per_batch):
                            server = next(scheduler.servers)
                            def enqueue_tasks(controls = batch_control.controls, server = server, idx=idx):
                                for control in controls:
                                    control.bind(server)
                                    control.server_batch_index[server] = idx
                                server.control()
                            event = scheduler.network.delay(
                                enqueue_tasks, logging_message=f'Enqueuing tasks: {[control.task.id for control in batch_control.controls]} on server: {server.id}.'
                            )
                            scheduler.simulation.event_queue.put(
                                event
                            )
                            batch_control.target_states[scheduler] = batch_control.States.blocked
                        return True
                    case batch_control.States.enqueued:
                        if all(len(control.bindings)==0 for control in batch_control.controls):
                            batch_control.unbind(scheduler)
                            return False
                        return True
                    case batch_control.States.blocked:
                        return True
                    case batch_control.States.terminated:
                        return False

    class Server:
        class Queue:
           def block(scheduler, task, server):
                """LatinSquare will block all task scheduling requests. 
                Instead task scheduling is handled by control signals in the LatinSquareControls."""
                try:
                    control, = tuple(control for control in scheduler.controls if (control.__class__.__name__ == 'LatinSquareControl') and control.task is task)
                except ValueError:
                    # New Control.
                    control = LatinSquareScheduler.Controls.LatinSquareControl(scheduler.simulation, task)
                    control.bind(scheduler)
                else:
                    # Control already bound to scheduler.
                    pass
                finally:
                    # Control already bound to scheduler.
                    control.bind(server)
                if server.busy_until == scheduler.simulation.time:
                    server.logger.debug(f'Server currently idle, respond to control instantly.')
                    server.control()
                    
        class Executor:
            def task_complete(server, task):
                """LatinSquare tasks are scheduled by RPC. So after
                tasks complete on the server, the server needs to enter the control loop.
                """
                try:
                    control, = tuple(control for control in server.controls if (control.__class__.__name__ == 'LatinSquareControl') and task is control.task)
                except ValueError:
                    server.logger.debug(f'Task: {task.id}, preempted on server: {server.id}. Simulation time: {server.simulation.time}.')
                else:
                    control.target_states[server] = control.States.task_finished
                finally:
                    server.control()

        class Control:

            def latin_square_server_control(control, server):
                if server.debug_log:
                    server.logger.debug(f'Server: {server.id}, entered control loop for task: {control.task.id}; currently in state: {control.target_states[server]}. Simulation time: {server.simulation.time}.')
                match control.target_states[server]:
                    case control.States.server_enqueued:
                        if server.is_idle:
                            scheduler, = tuple(target for target in control.target_states if target.__class__.__name__ == 'Scheduler')
                            def notify_scheduler(control=control, server=server, scheduler=scheduler):
                                scheduler.logger.debug(f'Received message from server: {server.id} that task: {control.task.id} has been enqueued. Simulation time: {control.simulation.time}.')
                                try:
                                    scheduler.control_coroutines[control].send(server)
                                except KeyError:
                                    # Task has been preempted already.
                                    pass                                   
                            event = server.network.delay(
                                notify_scheduler, logging_message=f'Send message to scheduler, server: {server.id}, has enqueued task: {control.task.id}. Simulation time: {control.simulation.time}.'
                            )
                            control.simulation.event_queue.put(
                                event
                            )
                            control.simulation.event_queue.put(
                                server.enqueue_task(control.task)
                            )
                            control.target_states[server] = control.States.server_executing_task
                        return True      
                    case control.States.task_finished:
                        try:
                            server.stop_task_event(control.task)
                        except KeyError:
                            pass
                        else:
                            pass
                        finally:
                            control.unbind(server)
                            scheduler, = tuple(target for target in control.target_states if target.__class__.__name__ == 'Scheduler')
                            def scheduler_complete_task(scheduler=scheduler, task=control.task, server=server, control=control):
                                scheduler.logger.debug(f'Notified by server: {server.id}, that task: {task.id} is complete. Simulation time: {scheduler.simulation.time}.')
                                scheduler.complete_task(task, server=server)
                                if control.target_states[scheduler] not in [control.States.task_finished, control.States.terminated]:
                                    control.target_states[scheduler] = control.States.task_finished
                                    scheduler.control_coroutines[control].send(server)
                            server.simulation.event_queue.put(
                                server.network.delay(
                                    scheduler_complete_task, logging_message=f'Server: {server.id} to notify scheduler that task: {control.task.id} is complete. Simulation Time: {server.simulation.time}.'
                                )
                            )
                            return False               
                    case control.States.terminated:
                        try:
                            server.stop_task_event(control.task)
                        except KeyError:
                            pass
                        else:
                            pass
                        finally:
                            control.unbind(server)
                            return False
                    case _:
                        return True

            def server_control(control, server):
                return LatinSquareScheduler.Server.Control.latin_square_server_control(control, server)
                  
    class Controls:
        if __package__ == 'schedulers':
            from computer import ControlClass
        else:
            from ..computer import ControlClass
        class LatinSquareBatch(ControlClass):
            """Latin Square Batch Control Management for collections of tasks.
            """
            from enum import IntEnum
            if __package__ == 'schedulers':
                from computer import ControlClass
            else:
                from ..computer import ControlClass
            logger = logging.getLogger('computer.Control.LatinSquareBatch')
            class States(IntEnum):
                blocked = -2
                terminated = -1
                unenqueued = 0
                enqueued = 1
            def __init__(self, simulation, controls):
                super(LatinSquareScheduler.Controls.LatinSquareBatch, self).__init__(simulation)
                self.batches = [control.task for control in controls]
                self.controls = [control for control in controls]
                self.target_states = {}
                for control in self.controls:
                    control.batch_controls.append(self)
                    control.batch_control_indices[self] = len(control.batch_controls)
                self.logger.debug(f'Controls in batch: {[control.id for control in self.controls]}. Simulation time: {self.simulation.time}.')

            @ControlClass.cleanup_control
            def control(self, target):
                match target.__class__.__name__:
                    case 'Scheduler':
                        return LatinSquareScheduler.Scheduler.Control.scheduler_batch_control(self, target)
                    case _:
                        pass

            def bind(self, target, batch=None):
                match target.__class__.__name__:
                    case 'Server':
                        self.bind_server(target, batch)
                    case 'Scheduler':
                        self.bind_scheduler(target, batch)

            def bind_server(self, target, batch=None):
                self.server_tasks[target] = batch

            def bind_scheduler(self, scheduler, batch=None):
                if scheduler not in self.bindings:
                    self.bindings.add(scheduler)
                    scheduler.add_control(self)
                    for control in self.controls:
                        def control_coroutine(control=control, scheduler=scheduler):
                            while True:
                                server = yield
                                LatinSquareScheduler.Scheduler.Control.scheduler_coroutine_control(control, scheduler, server=server)
                        try:
                            control_coroutines = scheduler.control_coroutines
                        except AttributeError:
                            control_coroutines = {
                                control: control_coroutine()
                            }
                            scheduler.control_coroutines = control_coroutines
                            next(control_coroutines[control]) # Prime the coroutine
                        else:
                            if control not in control_coroutines:
                                control_coroutines[control] = control_coroutine()
                                next(control_coroutines[control]) # Prime the coroutine
                        finally:
                            control.bind(scheduler)
                self.target_states[scheduler] = self.States.unenqueued

            def unbind(self, target):
                match target.__class__.__name__:
                    case 'Server':
                        self.unbind_server(target)
                    case 'Scheduler':
                        self.unbind_scheduler(target)
            
            def unbind_server(self, server):
                del self.server_tasks[server]
                for probe in self.probes:
                    probe.unbind(server)

            def unbind_scheduler(self, scheduler):
                self.bindings.discard(scheduler)

        class LatinSquareControl(ControlClass):
            """Latin Square Scheduler Control
            """
            from enum import IntEnum
            if __package__ == 'schedulers':
                from computer import ControlClass
            else:
                from ..computer import ControlClass
            class States(IntEnum):
                blocked = -2
                terminated = -1
                server_enqueued = 0
                server_executing_task = 1
                task_finished = 2
            states = States
            logger = logging.getLogger('computer.Control.LatinSquareControl')
            def __init__(self, simulation, task, batch_controls=[]):
                super(LatinSquareScheduler.Controls.LatinSquareControl, self).__init__(simulation)
                self.creation_time = self.simulation.time
                self.target_states = {}
                self.server_batch_index = {}
                self.task = task
                self.batch_controls = batch_controls
                self.batch_control_indices = {}
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
                match target.__class__.__name__:
                    case 'Server':
                        return LatinSquareScheduler.Server.Control.server_control(self, target)
                    case 'Scheduler':
                        return LatinSquareScheduler.Scheduler.Control.scheduler_control(self, target)

            def bind(self,target):
                match target.__class__.__name__:
                    case 'Server':
                        self.bind_server(target)
                    case 'Scheduler':
                        self.bind_scheduler(target)
            
            def bind_server(self, target):
                self.bindings.add(target)
                if self not in target.controls:
                    target.add_control(self)
                    self.target_states[target] = self.states.server_enqueued
                    self.logger.debug(
                        f'Server: {target.id} bound to LatinSquare Control: {self.id}, for task {self.task.id}. Registering arrival time {self.simulation.time}. Simulation time: { self.simulation.time}.'
                    )   

            def bind_scheduler(self, scheduler):
                if scheduler not in self.bindings:
                    scheduler.add_control(self)
                    self.target_states[scheduler] = self.States.server_enqueued
                    self.bindings.add(scheduler)
                    self.logger.debug(f'Scheduler bound to LatinSquare Control: {self.id}, for task: {self.task.id}. Simulation Time: {self.simulation.time}.')

            def unbind(self, target):
                """Remove controls from targets control list."""
                self.logger.debug(f'Unbinding, {target}: {target.id}, from LatinSquare Control: {self.id}. Simulation Time: {self.simulation.time}')
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
                self.bindings.remove(scheduler)
                del scheduler.control_coroutines[self]