import logging
class PeacockScheduler:
    class Scheduler:
        """These methods are logically linked to the scheduler."""
        class Enqueuing:
            def peacock(scheduler, job):
                tasks = job.tasks
                scheduler.logger.info(f'work to be scheduled is: {[(task.id, task.start_time) for task in tasks]}. Simulation Time: {scheduler.simulation.time}')
                for task in tasks:
                  control = PeacockScheduler.Controls.PeacockProbe(scheduler.simulation, task)
                  control.bind(scheduler)
                scheduler.control()

        class Executor:
            def task_complete(scheduler, task, server=None):
                pass
                
            def enqueue_task(scheduler, control):
                server = next(scheduler.servers)         
                def enqueue_task(server=server, control=control):
                    control.bind(server)
                    server.control()
                scheduler.simulation.event_queue.put(
                    scheduler.network.delay(
                        enqueue_task, logging_message=f'Send task: {control.task} to be scheduled on server {server.id}. Simulation time: {scheduler.simulation.time}.'
                    )
                )
                            

        class Control:                         
            def scheduler_control(control, scheduler):
                # Peacock has 8 states,
                # 0: server_init - scheduler has been initialized with task, probe needs to be sent to initial server.
                # 1: server_probed - task probe has been enqueued at the current server.
                # 2: server_start - task probe has reached the head of the queue, task data is pulled from inbox for execution.
                # 3: server_executing - task is being executed by the server.
                # 4: server_finished - task has finished being executed by the server, output data is pushed to outbox for storage.
                # 5: server_rotated - task probe is forwarded to the next server in the cycle.
                # -1: Terminated, Target to remove probe from controls.
                # -2: Blocked, Target blocked on a control signal.
                match control.target_states[scheduler]:
                    case control.States.server_init:
                        PeacockScheduler.Scheduler.Executor.enqueue_task(scheduler, control)
                        control.target_states[scheduler] = control.States.server_probed
                        return True
                    case control.States.server_probed | control.States.server_start | control.States.server_executing | control.States.blocked | control.States.server_rotated:
                        return True
                    case control.States.server_finished | control.States.terminated:
                        control.unbind(scheduler)
                        return False
                    case _:
                        raise Exception(f"Unhandled scheduler state")

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
                """Peacock tasks are scheduled by RPC. So after
                tasks complete on the server, the server needs to enter the control loop.
                """
                server.logger.debug(f'Server: {server.id},with tasks: {[control.task.id for control in server.controls if control.__class__.__name__ == "PeacockProbe"]}, has completed task: {task.id}')
                control, = tuple(control for control in server.controls if control.__class__.__name__ == 'PeacockProbe' and task is control.task)
                control.target_states[server] = control.States.server_finished
                server.control()
                
            def pull_task_and_start_executing(control, server):
                def pull_task(control = control, server=server):
                    scheduler, = tuple(target for target in control.target_states if target.__class__.__name__ == 'Scheduler')
                    scheduler.logger.debug(f'Received message from server: {server.id}, that it is ready to execute: {control.task.id}. Simulation time: {scheduler.simulation.time}.')
                    # scheduler.control_coroutines[control].send(server)
                    def start_task(control=control, server=server):
                        control.target_states[server] = control.States.server_start
                        server.control()
                    event = scheduler.network.delay(
                        start_task, logging_message=f'Notify server: {server}, to start task: {control.task}. Simulation time: {control.simulation.time}.'
                    )
                    server.start_task_event(control.task, event) # Server is not idle
                    return event
                event = server.network.delay(
                    pull_task, logging_message=f"Send message to scheduler, ready to start task: {control.task.id} on server {server.id}. Simulation time: {control.simulation.time}"
                )
                server.start_task_event(control.task, event) # Server is no longer idle.
                control.target_states[server] = control.States.blocked # Ready to begin task execution on scheduler response.
                control.simulation.event_queue.put(
                    event
                )
                return True
                
            def place_or_rotate(control, server, waiting_threshold):
                server.logger.debug(f'Place or rotate for task: {control.task.id} on server: {server.id}, simulation time: {server.simulation.time}.')
                cutoff_time = control.task.start_time + waiting_threshold - control.simulation.time
                if cutoff_time <= 0:
                    # the waiting threshold has passed, so just keep this task enqueued.
                    server.logger.debug(f'Server: {server.id} keeping task: {control.task.id} enqueued.')
                    return True
                else:
                    server.logger.debug(f'Estimate waiting time for Place or rotate for task: {control.task.id} on server: {server.id}, simulation time: {server.simulation.time}.')
                    # Estimate the waiting time of this task
                    estimated_waiting_time = 0
                    for cntrl in server.controls:
                        cntrl_service_time = server.completion_process.estimated_service_time(cntrl.task)
                        control_service_time = server.completion_process.estimated_service_time(control.task)
                        if cntrl_service_time > control_service_time or (cntrl_service_time == control_service_time and cntrl.server_arrival_times[server] > control.server_arrival_times[server]):
                            # the controls are sorted by estimated service time so we can just stop the search here.
                            break
                        # add the estimated service time
                        estimated_waiting_time += cntrl_service_time
                        if cntrl.target_states[server] == control.States.server_executing:
                            # add only the remaining estimated service time.
                            estimated_waiting_time += (cntrl.execution_start_time - control.simulation.time)
                        # if the estimated waiting time is greater than the cutoff time then we rotate
                        if estimated_waiting_time > cutoff_time:
                            # forward the probe to the next server in the cluster only if the next server in the cluster is not the first server in the cluster though.
                            _servers = control.simulation.cluster._servers
                            next_server = _servers[(_servers.index(server)+1)%len(_servers)]
                            if next_server is not server:
                                scheduler = server.simulation.cluster.scheduler
                                def forward_task(control=control, server=next_server):
                                    control.bind(server)
                                    server.control()
                                control.simulation.event_queue.put(
                                    scheduler.network.delay(
                                        forward_task, logging_message=f'Forward task: {control.task} to server: {next_server}. Simulation time: {control.simulation.time}.'
                                    )
                                )
                                control.target_states[server] = control.States.server_rotated
                                control.unbind(server)
                                server.logger.debug(f'Server: {server.id} forwarding task: {control.task.id} to server: {server.id}.')
                                return False
                            break
                    # else the total waiting time in this server does not exceed the cutoff and we can stay enqueued.
                    server.logger.debug(f'Server: {server.id} keeping task: {control.task.id} enqueued.')
                    return True

        class Control:
            def peacock_server_control(control, server):
                control.logger.debug(f'Server: {server.id}, control loop for PeacockProbe: {control.id} with task: {control.task.id}, state: {control.target_states[server].name}, simulation time: {control.simulation.time}')
                match control.target_states[server]:
                    case control.States.server_probed:
                        if server.is_idle:
                            # Begin executing task with the shortest estimated service time, because the deque is sorted by increasing service time estimates.
                            # We GUARANTEE that the server will only be in an idle state for the task at the head of the queue, therefore the shortest task.
                            return PeacockScheduler.Server.Executor.pull_task_and_start_executing(control, server)
                        else:
                            # Check to see if the probe needs to be rotated.
                            # The probe needs to be rotated if the total waiting time of the probe on this server would exceed the allowable waiting time threshold for this probe.
                            try:
                                waiting_threshold = server.peacock_waiting_threshold
                            except AttributeError:
                                waiting_threshold = float(server.simulation.CONFIGURATION['Computer.Scheduler.Peacock']['WAITING_THRESHOLD'])
                                server.peacock_waiting_threshold = waiting_threshold
                            else:
                                pass
                            finally:
                                return PeacockScheduler.Server.Executor.place_or_rotate(control, server, waiting_threshold)
                        raise Exception(f'State is not properly handled.')
                    case control.States.server_start:
                        control.execution_start_time = control.simulation.time
                        control.simulation.event_queue.put(
                            server.enqueue_task(control.task)
                        )
                        control.target_states[server] = control.States.server_executing
                        return True
                    case control.States.server_executing | control.States.blocked | control.States.server_init:
                        return True
                    case control.States.server_finished:
                        try:
                            # Although the event loop will ensure that the task finish time is measured properly.
                            # Server idle time accounting is handled by the server stop start methods.
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
                                control.target_states[scheduler] = control.States.server_finished
                                scheduler.complete_task(task, server=server)
                            server.simulation.event_queue.put(
                                server.network.delay(
                                    scheduler_complete_task, logging_message=f'Server: {server.id} to notify scheduler that task: {control.task.id} is complete. Simulation Time: {server.simulation.time}.'
                                )
                            )
                            return False
                    case control.States.terminated | control.States.server_rotated:
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
                        raise Exception(f"Unhandled state: {control.target_states[server]}, server: {server.id}")

            def server_control(control, target):
                return PeacockScheduler.Server.Control.peacock_server_control(control, target)
                              
    class Controls:
        if __package__ == 'schedulers':
            from computer import ControlClass
        else:
            from ..computer import ControlClass
        class PeacockProbe(ControlClass):
            """Peacock Scheduler Probe"""
            from enum import IntEnum
            if __package__ == 'schedulers':
                from computer import ControlClass
            else:
                from ..computer import ControlClass
            class States(IntEnum):
                blocked = -2
                terminated = -1
                server_init = 0
                server_probed = 1
                server_start = 2
                server_executing = 3
                server_finished = 4
                server_rotated = 5
            states = States
            logger = logging.getLogger('computer.Control.PeacockProbe')
            def __init__(self, simulation, task):
                super(PeacockScheduler.Controls.PeacockProbe, self).__init__(simulation)
                self.creation_time = self.simulation.time
                self.server_arrival_times = {}
                self.server_queue_lengths = {}
                self.target_states = {}
                self.task = task
                self.logger.debug(f'Task: {task.id} bound to probe: {self.id}. Simulation time: {self.simulation.time}.')
                # Peacock has 8 states,
                # 0: server_init - scheduler has been initialized with task, probe needs to be sent to initial server.
                # 1: server_probed - task probe has been enqueued at the current server.
                # 2: server_start - task probe has reached the head of the queue, task data is pulled from inbox for execution.
                # 3: server_executing - task is being executed by the server.
                # 4: server_finished - task has finished being executed by the server, output data is pushed to outbox for storage.
                # 5: server_rotated - task probe has been forwarded to the next server in the cycle.
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
                        return PeacockScheduler.Server.Control.server_control(self,target)
                    case 'Scheduler':
                        return PeacockScheduler.Scheduler.Control.scheduler_control(self,target)

            def bind(self, target):
                """Add controls to the targets control list.
                Metadata required for control should be bound to the control object here."""
                match target.__class__.__name__:
                    case 'Server':
                        self.server_bind(target)
                    case 'Scheduler':
                        self.scheduler_bind(target)

            def server_bind(self,target):
                from collections import deque
                if self not in target.controls:
                    target.add_control(self)
                    self.target_states[target] = self.States.server_init
                    # sort the probe queue on the server by estimated service time.
                    sort_states = [self.States.server_init, self.States.server_probed]
                    target.controls = deque(sorted(target.controls, key=lambda control: target.completion_process.estimated_service_time(control.task) if control.target_states[target] in sort_states else 0))
                    self.bindings.add(target)
                    self.target_states[target] = self.States.server_probed
                    self.server_arrival_times[target] = self.simulation.time
                    self.logger.debug(
                        f'Server: {target.id} bound to PeacockProbe: {self.id}, for task: {self.task}. Registering arrival time: {self.simulation.time}. Simulation time: {self.simulation.time}.'
                    )

            def scheduler_bind(self,scheduler):
                if scheduler not in self.bindings:
                    self.bindings.add(scheduler)
                    scheduler.add_control(self)
                    self.target_states[scheduler] = self.States.server_init
                    self.logger.debug(f'Scheduler bound to PeacockProbe: {self.id}, for task: {self.task.id}. Simulation Time: {self.simulation.time}.')
                
            def unbind(self, target):
                """Remove controls from targets control list."""
                match target.__class__.__name__:
                    case 'Server':
                        self.unbind_server(target)
                    case 'Scheduler':
                        self.unbind_scheduler(target)

            def unbind_server(self, server):
                self.logger.debug(f'Unbinding server: {server.id}, from PeacockProbe: {self.id} containing task: {self.task.id}. Simulation time: {self.simulation.time}.')
                self.bindings.discard(server)
                while self in server.controls:
                    server.controls.remove(self)

            def unbind_scheduler(self, scheduler):
                self.logger.debug(f'Unbinding scheduler from PeacockProbe: {self.id}. Simulation time: {self.simulation.time}.')
                self.bindings.discard(scheduler)
                while self in scheduler.controls:
                    scheduler.controls.remove(self)       
