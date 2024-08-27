import logging
class SparrowScheduler:
    class Scheduler:
        """These methods are logically linked to the scheduler."""
        class Enqueuing:
            def sparrow(scheduler, job):
                from numpy import array_split
                from math import ceil
                tasks = job.tasks
                try:
                    scheduler_enqueuing_params = scheduler.scheduler_enqueuing_params
                except AttributeError:
                    scheduler_enqueuing_params = {
                        'batch_size': int(scheduler.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['BATCH_SIZE']),
                        'num_probes': int(scheduler.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['NUM_SPARROW_PROBES']),
                        'late_binding': scheduler.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['LATE_BINDING']
                    }
                    scheduler.scheduler_enqueuing_params = scheduler_enqueuing_params
                else:
                    pass
                finally:
                    batch_size = scheduler_enqueuing_params['batch_size']
                    batches = [tuple(tasks.tolist()) for tasks in array_split(tasks,ceil(len(tasks)/batch_size))]
                    scheduler.logger.info(f'Work batches to be scheduled are {[(task.id, task.start_time) for batch in batches for task in batch]}. Simulation Time: {scheduler.simulation.time}')
                    for batch in batches:
                        batch_control = SparrowScheduler.Controls.SparrowBatch(scheduler.simulation, batch)
                        batch_control.bind(scheduler)
                        batch_control.control(scheduler)

        class Executor:
            def task_complete(scheduler, task, server=None):
                pass

            def enqueue_tasks_in_batch(scheduler, batch_control):
                """Enqueue batches of tasks scheduling"""
                try:
                    scheduler_enqueue_task_params = scheduler.SCHEDULER_ENQUEUE_TASK_PARAMS
                except AttributeError:
                    scheduler_enqueue_task_params = {
                        'num_probes': int(scheduler.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['NUM_SPARROW_PROBES'])
                    }
                    scheduler.SCHEDULER_ENQUEUE_TASK_PARAMS = scheduler_enqueue_task_params
                else:
                    pass
                finally:
                    num_probes = scheduler_enqueue_task_params['num_probes']
                    idx = 0
                    while idx < num_probes:
                        server = next(scheduler.servers)
                        probes = batch_control.probes
                        def enqueue_tasks(server=server, probes = probes):
                            for probe in probes:
                                probe.bind(server)
                            server.control()
                        scheduler.simulation.event_queue.put(
                            scheduler.network.delay(
                                enqueue_tasks, logging_message=f'Send batch: {[probe.task for probe in probes]} to be scheduled on server {server.id}. Simulation time: {scheduler.simulation.time}.'
                            )
                        )
                        idx += 1

            def preempt_task(probe, server, scheduler):
                other_servers = [target for target in probe.target_states if target.__class__.__name__ == 'Server' and target is not server]
                for other_server in other_servers:
                    def preempt(other_server=other_server, probe=probe):
                        other_server.logger.debug(f'server: {other_server.id}, having probe: {probe.id}, preempted. simulation time: {other_server.simulation.time}.')
                        if probe.target_states[other_server] != probe.States.server_ready:
                            # If the server probe is in the server_ready state AND the probe is being preempted on the server, then
                            # that implies that the server is requesting the next task in the batch from next_probe.
                            # next_probe will be terminated on the target server by the enqueue task response.
                            probe.target_states[other_server] = probe.States.terminated
                        other_server.control()
                    probe.simulation.event_queue.put(
                        scheduler.network.delay(
                            preempt, logging_message=f'Preempt probe: {probe.id}, task: {probe.task.id}, on server: {other_server.id}. Simulation time: {probe.simulation.time}.'
                        )
                    )
                            

        class Control:
            def scheduler_batch_control(batch_control, scheduler):
                match batch_control.target_states[scheduler]:
                    case batch_control.States.blocked | batch_control.States.batch_enqueued:
                        return True
                    case batch_control.States.terminated:
                        bindings = set(binding for probe in batch_control.probes for binding in probe.bindings)
                        if len(bindings) == 0:
                            batch_control.unbind(scheduler)
                            return False
                        else:
                            return True
                    case batch_control.States.batch_unenqueued:
                        SparrowScheduler.Scheduler.Executor.enqueue_tasks_in_batch(scheduler, batch_control)
                        batch_control.target_states[scheduler] = batch_control.States.batch_enqueued
                        return True
                    
            def scheduler_control(probe, scheduler):
                match probe.target_states[scheduler]:
                    case probe.States.server_probed | probe.States.server_ready | probe.States.server_executing_task | probe.States.blocked:
                        return True
                    case probe.States.task_finished | probe.States.terminated:
                        probe.unbind(scheduler)
                        tasks = [probe.task for probe in probe.batch_control.probes]
                        if all(task.is_finished for task in tasks):
                            probe.batch_control.target_states[scheduler] = probe.batch_control.States.terminated
                        return False

            def scheduler_late_binding_probe_control(probe, server, scheduler):
                match probe.target_states[server]:
                    case probe.States.terminated:
                        pass
                    case probe.States.server_probed:
                        pass
                    case probe.States.server_ready | probe.States.blocked:
                        notify_server = None
                        message = None
                        batch_control = probe.batch_control
                        next_probe = None
                        unenqueued_probes = [probe for probe in batch_control.probes if probe.target_states[scheduler] not in [probe.States.server_executing_task, probe.States.task_finished, probe.States.terminated, probe.States.blocked]]
                        if len(unenqueued_probes) == 0:
                            def notification(probe = probe, server = server):
                                batch_control = probe.batch_control 
                                server.logger.debug(f'Notified by scheduler that all tasks in batch: {[probe.task.id for probe in batch_control.probes]}, have already been enqueued. Simulation time: {probe.simulation.time}.')
                                for probe in batch_control.probes:
                                    probe.target_states[server] = probe.States.terminated
                                server.control()
                            message = f'Notify server: {server.id}, that all tasks in batch: {[probe.task.id for probe in batch_control.probes]} have already been enqueued. Simulation time: {probe.simulation.time}.'
                            notify_server = notification
                            next_probe = probe
                        else:
                            def notification(unenqueued_probes = unenqueued_probes, server = server):
                                all_probes = unenqueued_probes[0].batch_control.probes
                                preempted_probes = [probe for probe in all_probes if probe not in unenqueued_probes]
                                for probe in preempted_probes:
                                    probe.target_states[server] = probe.States.terminated
                                unenqueued_probes[0].target_states[server] = unenqueued_probes[0].States.server_ready
                                server.logger.debug(f'Notified by scheduler that task: {unenqueued_probes[0].task.id}, is to be enqueued on server: {server.id}. Simulation time: {unenqueued_probes[0].simulation.time}.')
                                server.control()
                            message = f'Notify server: {server.id}, to enqueue task: {unenqueued_probes[0].task.id}. Simulation time: {unenqueued_probes[0].simulation.time}.'
                            notify_server = notification
                            unenqueued_probes[0].target_states[scheduler] = unenqueued_probes[0].States.server_executing_task
                            next_probe = unenqueued_probes[0]
                            try:
                                PREEMPTION = scheduler.PREEMPTION
                            except AttributeError:
                                PREEMPTION = scheduler.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['PREEMPTION']
                                scheduler.PREEMPTION = PREEMPTION
                            else:
                                pass
                            finally:
                                if PREEMPTION.lower() == 'true':
                                    SparrowScheduler.Scheduler.Executor.preempt_task(unenqueued_probes[0], server, scheduler)
                        event = scheduler.network.delay(
                            notify_server, logging_message=message
                        )
                        server.start_task_event(next_probe.task, event) # Block execution loop on server.
                        probe.simulation.event_queue.put(
                            event
                        )
                    case probe.States.server_executing_task:
                        pass
                    case probe.States.task_finished:
                        pass

            def scheduler_sampling_probe_control(probe, server, scheduler):
                scheduler.logger.debug(f'Scheduler entering control loop for probe: {probe.id}, in response to a request from server: {server.id}. Simulation time: {scheduler.simulation.time}.')
                match probe.target_states[server]:
                    case probe.States.blocked:
                        num_probes = scheduler.scheduler_enqueuing_params['num_probes']
                        if len(probe.server_queue_lengths) == num_probes:
                            min_server = min(probe.server_queue_lengths, key = lambda server: probe.server_queue_lengths[server])
                            def enqueue_task(probe=probe, server=min_server):
                                server.logger.debug(f'Server: {server.id} notified by scheduler to enqueue task: {probe.task.id}. Simulation time: {server.simulation.time}.')
                                probe.target_states[server] = probe.States.server_ready
                                server.control()
                            event = scheduler.network.delay(
                                enqueue_task, logging_message = f'Notify server: {min_server.id} to enqueue task: {probe.task.id}. Simulation time: {scheduler.simulation.time}.'
                            )
                            scheduler.simulation.event_queue.put(
                                event
                            )
                            other_servers = [other_server for other_server in probe.server_queue_lengths if other_server is not min_server]
                            for other_server in other_servers:
                                def reject_task(probe = probe, server = other_server):
                                    server.logger.debug(f'Server: {server.id} notified by scheduler to reject task: {probe.task.id}. Simulation time: {server.simulation.time}.')
                                    probe.target_states[server] = probe.States.terminated
                                    server.control()
                                event = scheduler.network.delay(
                                    reject_task, logging_message = f'Notify server: {other_server.id} to reject task: {probe.task.id}. Simulation time: {scheduler.simulation.time}.'
                                )
                                scheduler.simulation.event_queue.put(
                                    event
                                )
                    case probe.States.terminated:
                        pass
                    case probe.States.server_probed:
                        pass
                    case probe.States.server_ready:
                        pass         
                    case probe.States.server_executing_task:
                        pass
                    case probe.States.task_finished:
                        pass

    class Server:
        class Queue:
            def block(scheduler, task, server):
                """Sparrow will block all task scheduling requests. 
                Instead task scheduling is handled by control signals in the SparrowProbes."""
                try:
                    probe, = tuple(probe for probe in scheduler.controls if probe.__class__.__name__ == 'SparrowProbe' and probe.task is task)
                except ValueError as e:
                    # New probe.
                    probe = SparrowScheduler.Controls.SparrowProbe(scheduler.simulation, task)
                    probe.bind(scheduler)
                else:
                    # Probe already bound to scheduler.
                    pass
                finally:
                    # Probe already bound to scheduler.
                    probe.bind(server)
                if server.busy_until == scheduler.simulation.time:
                    server.control()

        class Executor:
            def task_complete(server, task):
                """Sparrow tasks are scheduled by RPC. So after
                tasks complete on the server, the server needs to enter the control loop.
                """
                probe, = tuple(control for control in server.controls if control.__class__.__name__ == 'SparrowProbe' and task is control.task)
                probe.target_states[server] = probe.states.task_finished
                server.control()

        class Control:
            def sampling_server_control(probe, server):
                server.logger.debug(f'Server entered control loop for probe: {probe.id}, in state: {probe.target_states[server]}. Simulation time: {probe.simulation.time}.')
                match probe.target_states[server]:
                    case probe.States.blocked:
                        pass
                    case probe.States.terminated:
                        try:
                            server.stop_task_event(probe.task)
                        except KeyError:
                            pass
                        else:
                            pass
                        finally:
                            probe.unbind(server)
                            return False
                    case probe.States.server_probed:
                        def probe_reply(probe=probe, server=server, queue_length=len(server.tasks)):
                            scheduler, = tuple(target for target in probe.target_states if target.__class__.__name__ == 'Scheduler')
                            probe.server_queue_lengths[server] = queue_length
                            scheduler.probe_coroutines[probe].send(server)
                        event = server.network.delay(
                            probe_reply, logging_message=f'Reply to Sparrow Probe on server: {server.id}, current queue length: {len(server.tasks)}. Simulation time: {server.simulation.time}.'
                        )
                        server.simulation.event_queue.put(
                            event
                        )
                        probe.target_states[server] = probe.States.blocked
                        return True
                    case probe.States.server_ready:
                        if server.busy_until == server.simulation.time:
                            event = server.enqueue_task(probe.task)
                            probe.simulation.event_queue.put(
                                event
                            )
                            probe.target_states[server] = probe.States.server_executing_task
                            return True
                    case probe.States.server_executing_task:
                        pass
                    case probe.States.task_finished:
                        try:
                            server.stop_task_event(probe.task)
                        except KeyError:
                            pass
                        else:
                            pass
                        finally:
                            probe.unbind(server)
                            return False

            def late_binding_server_control(probe, server):
                probe.logger.debug(f'Server: {server.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.server_probed}, simulation time: {probe.simulation.time}')
                match probe.target_states[server]:
                    case probe.States.server_probed:
                        if server.is_idle:
                            def notify_scheduler(probe = probe, server = server):
                                scheduler, = tuple(target for target in probe.target_states if target.__class__.__name__ == 'Scheduler')
                                scheduler.logger.debug(f'Received message from server: {server.id}, that it has enqueued task: {probe.task.id}. Simulation time: {scheduler.simulation.time}.')
                                scheduler.probe_coroutines[probe].send(server)
                            event = server.network.delay(
                                notify_scheduler, logging_message=f'Send message to scheduler, ready to enqueue task: {probe.task.id} on server {server.id}. Simulation Time: {probe.simulation.time}'
                            )
                            server.start_task_event(probe.task, event) # Block server execution loop.
                            probe.target_states[server] = probe.states.server_ready # Block control state.
                            probe.simulation.event_queue.put(
                                event
                            )
                        return True
                    case probe.States.server_ready:
                        if server.is_idle:
                            event = server.enqueue_task(probe.task)
                            probe.simulation.event_queue.put(
                                event
                            )
                            probe.target_states[server] = probe.states.server_executing_task
                        return True
                    case probe.States.server_executing_task | probe.States.blocked:
                        return True
                    case probe.States.terminated:
                        try:
                            server.stop_task_event(probe.task)
                        except KeyError:
                            pass
                        else:
                            pass
                        finally:
                            probe.unbind(server)
                            return False
                    case probe.States.task_finished:
                        try:
                            # Although the event loop will ensure that the task finish time is measured properly.
                            # Server idle time accounting is handled by the server stop start methods.
                            server.stop_task_event(probe.task)
                        except KeyError:
                            pass
                        else:
                            pass
                        finally:
                            probe.unbind(server)
                            scheduler, = tuple(target for target in probe.target_states if target.__class__.__name__ == 'Scheduler')
                            def scheduler_complete_task(scheduler=scheduler, task=probe.task, server=server, probe=probe):
                                scheduler.logger.debug(f'Notified by server: {server.id}, that task: {task.id} is complete. Simulation time: {scheduler.simulation.time}.')
                                probe.target_states[scheduler] = probe.States.task_finished
                                scheduler.complete_task(task, server=server)
                            server.simulation.event_queue.put(
                                server.network.delay(
                                    scheduler_complete_task, logging_message=f'Server: {server.id} to notify scheduler that task: {probe.task.id} is complete. Simulation Time: {server.simulation.time}.'
                                )
                            )
                            return False
                    case _:
                        return True


            def server_control(probe, target):
                try:
                    server_control_params = probe.simulation.cluster.SERVER_CONTROL_PARAMS
                except AttributeError:
                    server_control_params = {
                        'late_binding': probe.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['LATE_BINDING']
                    }
                    probe.simulation.cluster.SERVER_CONTROL_PARAMS = server_control_params
                else:
                    pass
                finally:
                    if server_control_params['late_binding'].lower() == 'false':
                        return SparrowScheduler.Server.Control.sampling_server_control(probe,target)
                    else:
                        return SparrowScheduler.Server.Control.late_binding_server_control(probe,target)
            
    class Controls:
        if __package__ == 'schedulers':
            from computer import ControlClass
        else:
            from ..computer import ControlClass
        class SparrowBatch(ControlClass):
            """Sparrow Scheduler Batch
            management object for a collection of sparrow probes.
            """
            from enum import IntEnum
            if __package__ == 'schedulers':
                from computer import ControlClass
            else:
                from ..computer import ControlClass
            class States(IntEnum):
                blocked = -2
                terminated = -1
                batch_unenqueued = 0
                batch_enqueued = 1
            logger = logging.getLogger('computer.Control.SparrowBatch')
            def __init__(self, simulation, batch):
                super(SparrowScheduler.Controls.SparrowBatch, self).__init__(simulation)
                self.batches = [batch]
                self.server_tasks = {}
                self.target_states = {}
                self.probes = set(SparrowScheduler.Controls.SparrowProbe(simulation, task, batch_control=self) for task in batch)
                self.logger.debug(f'Probes in batch: {[probe for probe in self.probes]}. Simulation time: {self.simulation.time}.')

            @ControlClass.cleanup_control
            def control(self, target):
                match target.__class__.__name__:
                    case 'Server':
                        return True
                    case 'Scheduler':
                        return SparrowScheduler.Scheduler.Control.scheduler_batch_control(self, target)
            
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
                for probe in self.probes:
                    def probe_coroutine(probe = probe, scheduler = scheduler):
                        while True:
                            server = yield
                            late_binding = scheduler.scheduler_enqueuing_params['late_binding']
                            if late_binding.lower() == 'true':
                                SparrowScheduler.Scheduler.Control.scheduler_late_binding_probe_control(probe, server, scheduler)
                            else:
                                SparrowScheduler.Scheduler.Control.scheduler_sampling_probe_control(probe, server, scheduler)
                    try:
                        probe_coroutines = scheduler.probe_coroutines
                    except AttributeError:
                        probe_coroutines = {
                            probe: probe_coroutine()
                        }
                        scheduler.probe_coroutines = probe_coroutines
                    else:
                        probe_coroutines[probe] = probe_coroutine()
                    finally:
                        next(probe_coroutines[probe]) # Prime the coroutine
                        probe.bind(scheduler)
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

        class SparrowProbe(ControlClass):
            """Sparrow Scheduler Probe"""
            from enum import IntEnum
            if __package__ == 'schedulers':
                from computer import ControlClass
            else:
                from ..computer import ControlClass
            class States(IntEnum):
                blocked = -2
                terminated = -1
                server_probed = 0
                server_ready = 1
                server_executing_task = 2
                task_finished = 3
            states = States
            logger = logging.getLogger('computer.Control.SparrowProbe')
            def __init__(self, simulation, task, batch_control=None):
                super(SparrowScheduler.Controls.SparrowProbe, self).__init__(simulation)
                self.creation_time = self.simulation.time
                self.server_arrival_times = {}
                self.server_queue_lengths = {}
                self.target_states = {}
                self.task = task
                self.enqueued = False
                self.batch_control = batch_control
                self.logger.debug(f'Task: {task.id} bound to probe: {self.id}. Simulation time: {self.simulation.time}.')
                # Sparrow has 6 states,
                # 0: Intialize Scheduler - Probe Server - Awaiting Server Response
                # 1: Server Response - Ready to Enqueue Task - Awaiting Scheduler Response
                # 2: Scheduler Response - Task Notify Server - Awaiting Server Response
                # 3: Task Completion - Notify Scheduler
                # -1: Terminated, Target to remove probe from controls.
                # -2: Blocked, Target blocked on a control signal.

                # Task Notify will contain one of two messages:
                # 0: Enqueue Task - Server Selected to Enqueue Task - Scheduler awaiting server task Completion notification
                # 1: Reject Task - Server Rejected to Enqueue Task - Server to Remove probe from controls.

            @property
            def probe_state(self):
                if len(self.target_states) > 0:
                    target = max(self.target_states, key=lambda key: self.target_states[key])
                    return self.target_states[target]
                else:
                    return 0

            
            @ControlClass.cleanup_control
            def control(self, target):
                """
                Target can be a server or a scheduler.
                Controls are popped from the left of the target controls before entering this loop.
                """
                match target.__class__.__name__:
                    case 'Server':
                        return SparrowScheduler.Server.Control.server_control(self,target)
                    case 'Scheduler':
                        return SparrowScheduler.Scheduler.Control.scheduler_control(self,target)

            def bind(self, target):
                """Add controls to the targets control list.
                Metadata required for control should be bound to the control object here."""
                match target.__class__.__name__:
                    case 'Server':
                        self.server_bind(target)
                    case 'Scheduler':
                        self.scheduler_bind(target)

            def server_bind(self,target):
                if self not in target.controls:
                    target.add_control(self)
                    self.bindings.add(target)
                    self.target_states[target] = self.states.server_probed
                    self.server_arrival_times[target] = self.simulation.time
                    self.logger.debug(
                        f'Server: {target.id} bound to SparrowProbe: {self.id}, for task {self.task.id}. Registering arrival time {self.simulation.time}. Simulation time: { self.simulation.time}.'
                    )

            def scheduler_bind(self,scheduler):
                if scheduler not in self.bindings:
                    scheduler.add_control(self)
                    self.bindings.add(scheduler)
                    self.target_states[scheduler] = self.States.server_probed
                    self.logger.debug(f'Scheduler bound to SparrowProbe: {self.id}, for task: {self.task.id}. Simulation Time: {self.simulation.time}.')
                
            def unbind(self, target):
                """Remove controls from targets control list."""
                match target.__class__.__name__:
                    case 'Server':
                        self.unbind_server(target)
                    case 'Scheduler':
                        self.unbind_scheduler(target)

            def unbind_server(self, server):
                self.logger.debug(f'Unbinding server: {server.id}, from Sparrow Probe: {self.id}. Simulation time: {self.simulation.time}.')
                self.bindings.discard(server)
                while self in server.controls:
                    server.controls.remove(self)

            def unbind_scheduler(self, scheduler):
                self.logger.debug(f'Unbinding scheduler from sparrow probe: {self.id}. Simulation time: {self.simulation.time}.')
                self.bindings.discard(scheduler)
                while self in scheduler.controls:
                    scheduler.controls.remove(self)       

