import configparser
import logging

def latin_square(n):
    return [[(i+j)%n for i in range(n)] for j in range(n)]

def configuration(filename='./configuration.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config

class ArrivalProcessPolicies:
    class JobArrivalPolicies:
        def erlang_arrivals(arrival_process):
            if not __package__:
                from work import Task
            else:
                from .work import Task
            num_tasks = arrival_process.NUM_TASKS
            tasks = []
            for _ in range(num_tasks):
                arrival_process._arrival_time += next(arrival_process.interrenewal_times)
                task = Task(arrival_process.simulation)
                task.start_time = arrival_process._arrival_time
                tasks.append(task)
            return tasks

        def exponential_arrivals(arrival_process):
            if not __package__:
                from work import Task
            else:
                from .work import Task
            num_tasks = arrival_process.NUM_TASKS
            arrival_process._arrival_time += next(arrival_process.interrenewal_times)
            tasks = [Task(arrival_process.simulation) for _ in range(num_tasks)]
            for task in tasks:
                task.start_time = arrival_process._arrival_time
            return tasks

        def job_arrival_policy(arrival_process):
            try:
                arrival_process_params = arrival_process.PARAMS
            except AttributeError:
                arrival_process_params = {
                    'job_arrival_policy': arrival_process.simulation.CONFIGURATION['Processes.Arrival.Job']['POLICY']
                }
                arrival_process.PARAMS = arrival_process_params
            else:
                pass
            finally:
                match arrival_process_params['job_arrival_policy']:
                    case 'Erlang':
                        return ArrivalProcessPolicies.JobArrivalPolicies.erlang_arrivals(arrival_process)
                    case 'Exponential':
                        return ArrivalProcessPolicies.JobArrivalPolicies.exponential_arrivals(arrival_process)


class SchedulingPolicies:

    def full_repetition(scheduler, job):
        from math import ceil
        from numpy import array_split
        tasks = job.tasks
        batch_size = len(tasks)
        work = [work.tolist() for work in array_split(tasks, ceil(len(tasks)/batch_size))]
        scheduler.logger.info(f'work batches to be scheduled are {[(task.id, task.start_time) for batch in work for task in batch]}. Simulation Time: {scheduler.simulation.time}')
        for batch in work:
            for _ in range(len(batch)):
                for _ in range(scheduler.cluster.num_servers):
                    scheduler.schedule_batch(batch)       

    #Decorator to choose a scheduling policy:
    def scheduler(fn):
        """Decorator to Choose a scheduling Policy"""
        def func(*args):
            policy = args[0].POLICY
            match policy:
                case 'RoundRobin' | 'CompletelyRandom':
                    RoundRobinScheduler.Scheduler.Queue.round_robin(*args)
                    return fn(*args)
                case 'FullRepetition':
                    SchedulingPolicies.full_repetition(*args)
                    return fn(*args)
                case 'Sparrow':
                    SparrowScheduler.Scheduler.Enqueuing.sparrow(*args)
                    return fn(*args)
                case 'LatinSquare':
                    LatinSquareScheduler.Scheduler.Enqueuing.latin_square(*args)
                    return fn(*args)
        return func
    
    # Task Batching Policies
    def schedule_batch(scheduler,batch):
        """Enqueue batches of tasks scheduling"""
        server = next(scheduler.servers)
        def schedule_tasks(batch=batch, schedule_task=scheduler.schedule_task, server=server):
            for task in batch:
                schedule_task(task,server)
        scheduler.simulation.event_queue.put(
            scheduler.network.delay(
                schedule_tasks, logging_message=f'Send tasks {[task for task in batch]} to be scheduled on server {server.id}. Simulation Time: {scheduler.simulation.time}.'
            )
        )
    
    def batch_schedule(fn):
        """Decorator to choose a batching policy"""
        def func(*args):
            policy = args[0].POLICY
            match policy:
                case _:
                    SchedulingPolicies.schedule_batch(*args)
        return func
    

class ServerSelectionPolicies:
    def cycle(cluster):
        """Cycle Through servers in cluster in an infinite loop, default policy."""
        import itertools
        return itertools.cycle(cluster.servers)
    
    def random(cluster, num_samples):
        """Select a subset of servers randomly, and cycle through them."""
        while True:
            yield from cluster.random_choice.choose_from(cluster.servers, num_samples)

    def server_selection(fn):
        def func(*args): 
            cluster = args[0]
            match cluster.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']:
                case 'Sparrow':
                    try:
                        cluster_probe_config = cluster.PROBE_CONFIG
                    except AttributeError:
                        cluster_probe_config = {
                            'num_probes': int(cluster.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['NUM_SPARROW_PROBES']),
                            'server_selection': cluster.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['SERVER_SELECTION']
                        }
                        cluster.PROBE_CONFIG = cluster_probe_config
                    else:
                        pass
                    finally:
                        if cluster_probe_config['server_selection'].lower() == 'cycle':
                            return ServerSelectionPolicies.cycle(cluster)
                        else:
                            num_probes = cluster_probe_config['num_probes']
                            return ServerSelectionPolicies.random(cluster, num_probes)
                case 'LatinSquare':
                    try:
                        cluster_control_config = cluster.CONTROL_CONFIG
                    except AttributeError:
                        cluster_control_config = {
                            'latin_square_order': int(cluster.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['LATIN_SQUARE_ORDER']),
                            'num_probes_per_batch': int(cluster.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['NUM_PROBES_PER_BATCH']),
                            'server_selection': cluster.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['SERVER_SELECTION']
                        }
                        cluster.CONTROL_CONFIG = cluster_control_config
                    else:
                        pass
                    finally:
                        if cluster_control_config['server_selection'].lower() == 'cycle':
                            return ServerSelectionPolicies.cycle(cluster)
                        else:
                            latin_square_order = cluster_control_config['latin_square_order']
                            num_probes_per_batch = cluster_control_config['num_probes_per_batch']
                            return ServerSelectionPolicies.random(cluster, latin_square_order*num_probes_per_batch)
                case 'RoundRobin':
                    return ServerSelectionPolicies.cycle(cluster)
                case 'CompletelyRandom':
                    return ServerSelectionPolicies.random(cluster,1)
                case _:
                    return ServerSelectionPolicies.cycle(cluster)
        return func


class BlockingPolicies:
    def infinite_queue(scheduler, task, server):
        """No Blocking Do Nothing"""
        pass

    def random_retrying(scheduler, task, server):
        """Blocking Random Retrying"""
        if len(server.tasks) >= server.max_queue_length:
            scheduler.logger.debug(f'Server: {server.id} has a full queue, and so we must wait until the queue frees and try again.')
            scheduler.simulation.event_queue.put(
                server.blocker.delay(
                    scheduler.schedule_task, task, server
                )
            )

    def blocking(fn):
        """Decorator to Choose a Blocking Policy."""
        def func(*args):
            scheduler = args[0]
            match scheduler.POLICY:
                case 'RoundRobin' | 'CompletelyRandom' | 'Sparrow' | 'LatinSquare':
                    pass
                case _:
                    BlockingPolicies.infinite_queue(*args)
                    return fn(*args)
        return func
    
class ServerTaskExecutionPolicies:
    def default_task_completion(server, task):
        """Preempt the job, and move all subsequent tasks up in the queue.
        Task completion is idempotent so there is no need to remove or otherwise update all of the old events.
         they will just eventually clear from the event queue.
        """
        from copy import copy
        def debug_log(server=server,task=task):
            policy = server.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']
            if policy == 'FullRepetition' or policy == 'LatinSquare':
                server.logger.debug(f'task: {task.id} is not in the queue of server: {server.id}. Simulation time: {server.simulation.time}')
        server.logger.debug(f'server: {server.id}, checking if task: {task.id} is in the queue. Simulation Time: {server.simulation.time}')
        try:
            event=server.tasks.pop(task)
            server.logger.debug(f'server: {server.id}, clearing task: {task.id} from queue, at time: {server.simulation.time}')
        except KeyError:
            debug_log()
        else:
            time = event.arrival_time
            event.cancel()
            if task.is_finished and task.job.is_finished and time > server.simulation.time:
                reschedule_tasks = [
                    task for task in list(server.tasks) if server.tasks[task].arrival_time > time
                ]
                if len(reschedule_tasks)>0:
                    server.logger.debug(f'{len(reschedule_tasks)} tasks: {reschedule_tasks} on server: {server.id}, need to be rescheduled.')
                    events = sorted([server.tasks.pop(task) for task in reschedule_tasks])
                    delta = time - server.simulation.time
                    for event,task in zip(events,reschedule_tasks):
                        new_event = copy(event)
                        new_event.arrival_time = event.arrival_time - delta
                        server.logger.debug(f'task {task.id} rescheduled to complete on server: {server.id} at time: {new_event.arrival_time}. Simulation Time: {server.simulation.time}')
                        server.simulation.event_queue.put(
                            new_event
                        )
                        server.start_task_event(task, new_event)

    def task_completion(fn):
        def func(*args):
            server = args[0]
            match server.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']:
                case 'Sparrow':
                    SparrowScheduler.Server.Executor.task_complete(*args)
                case 'Peacock':
                    PeacockScheduler.Server.Executor.task_complete(*args)
                case 'LatinSquare':
                    LatinSquareScheduler.Server.Executor.task_complete(*args)
                case 'RoundRobin' | 'CompletelyRandom':
                    RoundRobinScheduler.Server.Executor.complete_task(*args)
                case _:
                    ServerTaskExecutionPolicies.default_task_completion(*args)
        return func


class SchedulerTaskCompletionPolicies:
    def default_task_completion(scheduler,task):
        scheduler.logger.info(f'Task {task.id}, finished at time: {task.finish_time}.')
        for server in scheduler.cluster.servers:
            scheduler.simulation.event_queue.put(
                scheduler.network.delay(
                    server.complete_task,task, logging_message=f'Send message to server: {server.id} to preempt task: {task.id}. Simulation Time: {scheduler.simulation.time}'
                )
            )

    def task_complete(fn):
        def func(*args, **kwargs):
            fn(*args)
            scheduler = args[0]
            policy = scheduler.POLICY
            match policy:
                case 'Sparrow':
                    SparrowScheduler.Scheduler.Executor.task_complete(*args,**kwargs)
                case 'Peacock':
                    PeacockScheduler.Scheduler.Executor.task_complete(*args, **kwargs)
                case 'LatinSquare':
                    LatinSquareScheduler.Scheduler.Executor.task_complete(*args, **kwargs)
                case 'RoundRobin' | 'CompletelyRandom':
                    RoundRobinScheduler.Scheduler.Executor.complete_task(*args, **kwargs)
                case _:
                    SchedulerTaskCompletionPolicies.default_task_completion(*args)
        return func

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
        if not __package__:
            from computer.abstract_base_classes import ControlClass
        else:
            from .computer.abstract_base_classes import ControlClass
        class SparrowBatch(ControlClass):
            """Sparrow Scheduler Batch
            management object for a collection of sparrow probes.
            """
            from enum import IntEnum
            if not __package__:
                from computer.abstract_base_classes import ControlClass
            else:
                from .computer.abstract_base_classes import ControlClass
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
            if not __package__:
                from computer.abstract_base_classes import ControlClass
            else:
                from .computer.abstract_base_classes import ControlClass
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
                control, = tuple(control for control in server.controls if control.__class__.__name__ == 'PeacockProbe' and task is control.task)
                control.target_states[server] = control.States.server_finished
                server.control()
                
            def pull_task_and_start_executing(control, server):
                def pull_task(control = control, server=server):
                    scheduler, = tuple(target, for target in control.target_states if target.__class__.__name__ == 'Scheduler')
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
                    pull_task, logging_message=f"Send message to scheduler, ready to start task: {control.task.id} on server {server.id}. Simulation time: {control.simulation.time}'
                )
                server.start_task_event(control.task, event) # Server is no longer idle.
                control.target_states[server] = control.States.blocked # Ready to begin task execution on scheduler response.
                control.simulation.event_queue.put(
                    event
                )
                return True
                
            def place_or_rotate(control, server, waiting_threshold):
                cutoff_time = control.task.start_time + waiting_threshold - control.simulation.time
                if cutoff_time <= 0:
                    # the waiting threshold has passed, so just keep this task enqueued.
                    return True
                else:
                    # Estimate the waiting time of this task
                    estimated_waiting_time = 0
                    for control in server.controls:
                        if control.target_states[server] == control.States.server_executing:
                            # add only the remaining estimated service time.
                            estimated_waiting_time += (control.execution_start_time + server.completion_process.estimated_service_time(control.task) - control.simulation.time)
                        else:
                            # add the estimated service time
                            estimated_waiting_time += server.completion_process.estimated_service_time(control.task)
                        # if the estimated waiting time is greater than the cutoff time then we rotate
                        if estimated_waiting_time > cutoff_time:
                            # forward the probe to the next server in the cluster
                            _servers = control.simulation.cluster._servers
                            next_server = _servers[(_servers.index(server)+1)%len(_servers)]
                            scheduler, = tuple(target, for target in control.target_states if target.__class__.__name__ == 'Scheduler')
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
                            return False
                    # else the total waiting time in this server does not exceed the cutoff and we can stay enqueued.
                    return True

        class Control:
            def peacock_server_control(control, server):
                control.logger.debug(f'Server: {server.id}, control loop for Sparrow Probe: {control.id}, state: {control.target_states[server]}, simulation time: {control.simulation.time}')
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
                                waiting_threshold = int(scheduler.simulation.CONFIGURATION['Computer.Scheduler.Peacock']['WAITING_THRESHOLD'])
                                server.peacock_waiting_threshold = waiting_threshold
                            else:
                                pass
                            finally:
                                return PeacockScheduler.Server.Executor.place_or_rotate(control, server, waiting_threshold)
                        return True
                    case control.States.server_start:
                        control.execution_start_time = control.simulation.time
                        control.simulation.event_queue.put(
                            server.enqueue_task(control.task)
                        )
                        control.target_states[server] = control.States.server_executing_task
                        return True
                    case control.States.server_executing_task | control.States.blocked | control.States.server_init:
                        pass
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
        if not __package__:
            from computer.abstract_base_classes import ControlClass
        else:
            from .computer.abstract_base_classes import ControlClass
        class PeacockProbe(ControlClass):
            """Peacock Scheduler Probe"""
            from enum import IntEnum
            if not __package__:
                from computer.abstract_base_classes import ControlClass
            else:
                from .computer.abstract_base_classes import ControlClass
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
                super(SparrowScheduler.Controls.SparrowProbe, self).__init__(simulation)
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
                    # sort the probe queue on the server by estimated service time.
                    target.controls = deque(sorted(target.controls, key=lambda control: target.completion_process.estimated_service_time(task)))
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
                    # def control_coroutine(control=self, scheduler=scheduler):
                        # while True:
                            # server = yield
                            # def start_task(control=control, server=server):
                                # control.target_states[server] = control.States.server_start
                                # server.control()
                            # event = scheduler.network.delay(
                                # start_task, logging_message=f'Notify server: {server}, to start task: {control.task}. Simulation time: {control.simulation.time}.'
                            # )
                            # server.start_task_event(next_probe.task, event) # Server is not idle
                            # probe.simulation.event_queue.put(
                                # event
                            # )
                    # try:
                        # control_coroutines = scheduler.control_coroutines
                    # except AttributeError:
                        # control_coroutines = {
                            # self: control_coroutine()
                        # }
                        # scheduler.control_coroutines = control_coroutines
                    # else:
                        # control_coroutines[self] = control_coroutines()
                    # finally:
                        # next(control_coroutines[self]) # Prime the coroutine   
                    self.logger.debug(f'Scheduler bound to PeacockProbe: {self.id}, for task: {self.task.id}. Simulation Time: {self.simulation.time}.')
                
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
        if not __package__:
            from computer.abstract_base_classes import ControlClass
        else:
            from .computer.abstract_base_classes import ControlClass
        class LatinSquareBatch(ControlClass):
            """Latin Square Batch Control Management for collections of tasks.
            """
            from enum import IntEnum
            if not __package__:
                from computer.abstract_base_classes import ControlClass
            else:
                from .computer.abstract_base_classes import ControlClass
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
            if not __package__:
                from computer.abstract_base_classes import ControlClass
            else:
                from .computer.abstract_base_classes import ControlClass
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
        if not __package__:
            from computer.abstract_base_classes import ControlClass
        else:
            from .computer.abstract_base_classes import ControlClass
        class RoundRobinTask(ControlClass):
            from enum import IntEnum
            if not __package__:
                from computer.abstract_base_classes import ControlClass
            else:
                from .computer.abstract_base_classes import ControlClass
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
            if not __package__:
                from computer.abstract_base_classes import ControlClass
            else:
                from .computer.abstract_base_classes import ControlClass
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
                
