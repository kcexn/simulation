import configparser

def latin_square(n):
    return [[(i+j)%n for i in range(n)] for j in range(n)]

def configuration(filename='./configuration.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config

#TODO: Implement Sparrow Latin Square Hybrid. Tolerate Worker Failures, Improve Response Times. Handle Heterogeneous Task Sizes.

class SchedulingPolicies:
    def round_robin(scheduler, job):
        from numpy import array_split
        tasks = job.tasks
        work = [work.tolist() for work in array_split(tasks,len(tasks))]
        scheduler.logger.info(f'Work batches to be scheduled are {[(task.id, task.start_time) for batch in work for task in batch]}. Simulation Time: {scheduler.simulation.time}')
        for batch in work:
            for _ in range(len(batch)):
                scheduler.schedule_batch(batch)

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

    def latin_square(scheduler, job):
        from math import ceil
        from numpy import array_split
        tasks = job.tasks
        latin_square_order = int(scheduler.simulation.CONFIGURATION['Computer.Scheduler']['LATIN_SQUARE_ORDER'])
        def square(n):
            return [[(i+j)%n for i in range(n)] for j in range(n)]
        latin_square = square(latin_square_order)
        work = [work.tolist() for work in array_split(tasks, ceil(len(tasks)/latin_square_order))]
        scheduler.logger.info(f'work batches to be scheduled are {[(task.id, task.start_time) for batch in work for task in batch]}. Simulation Time: {scheduler.simulation.time}')
        for batch in work:
            for sequence in latin_square:
                ordered_tasks = [batch[idx] for idx in sequence if idx < len(batch)]
                scheduler.schedule_batch(ordered_tasks)

    #Decorator to choose a scheduling policy:
    def scheduler(fn):
        """Decorator to Choose a scheduling Policy"""
        def func(*args):
            policy = args[0].POLICY
            match policy:
                case 'RoundRobin':
                    SchedulingPolicies.round_robin(*args)
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
            scheduler.cluster.network.delay(
                schedule_tasks, logging_message=f'Send tasks {[task for task in batch]} to be scheduled on server {server.id}. Simulation Time: {scheduler.simulation.time}.'
            )
        )
    
    def batch_schedule(fn):
        """Decorator to choose a batching policy"""
        def func(*args):
            policy = args[0].POLICY
            match policy:
                case 'Sparrow':
                    SparrowScheduler.Scheduler.Enqueuing.schedule_batch(*args)
                case 'LatinSquare':
                    LatinSquareScheduler.Scheduler.Enqueuing.schedule_batch(*args)
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
            random_servers = cluster.random_choice.choose_from(cluster.servers, num_samples)
            cluster.logger.debug(f'yielding servers: {[server.id for server in iter(random_servers)]}. Simulation Time: {cluster.simulation.time}')
            for server in iter(random_servers):
                yield server

    def server_selection(fn):
        def func(*args): 
            cluster = args[0]
            match cluster.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']:
                case 'Sparrow':
                    num_probes = int(cluster.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['NUM_SPARROW_PROBES'])
                    batch_size = int(cluster.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['BATCH_SIZE'])
                    return ServerSelectionPolicies.random(cluster, num_probes)
                case 'LatinSquare':
                    num_tasks = int(cluster.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['LATIN_SQUARE_ORDER'])
                    return ServerSelectionPolicies.random(cluster, num_tasks)
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
                case 'Sparrow':
                    SparrowScheduler.Server.Queue.block(*args)
                case 'LatinSquare':
                    LatinSquareScheduler.Server.Queue.block(*args)
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
                        server.tasks[task] = new_event

    def task_completion(fn):
        def func(*args):
            server = args[0]
            match server.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']:
                case 'Sparrow':
                    SparrowScheduler.Server.Executor.task_complete(*args)
                case 'LatinSquare':
                    LatinSquareScheduler.Server.Executor.task_complete(*args)
                case _:
                    ServerTaskExecutionPolicies.default_task_completion(*args)
        return func


class SchedulerTaskCompletionPolicies:
    def default_task_completion(scheduler,task):
        scheduler.logger.info(f'Task {task.id}, finished at time: {task.finish_time}.')
        for server in scheduler.cluster.servers:
            scheduler.simulation.event_queue.put(
                scheduler.cluster.network.delay(
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
                case 'LatinSquare':
                    LatinSquareScheduler.Scheduler.Executor.task_complete(*args, **kwargs)
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
                if not __package__:
                    from computer.control import SparrowBatch
                else:
                    from .computer.control import SparrowBatch
                tasks = job.tasks
                batch_size = int(scheduler.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['BATCH_SIZE'])
                batches = [tuple(tasks.tolist()) for tasks in array_split(tasks,ceil(len(tasks)/batch_size))]
                scheduler.logger.info(f'Work batches to be scheduled are {[(task.id, task.start_time) for batch in batches for task in batch]}. Simulation Time: {scheduler.simulation.time}')
                for batch in batches:
                    batch_control = SparrowBatch(scheduler.simulation, batch)
                    batch_control.bind(scheduler)
                    scheduler.schedule_batch(batch)

            def schedule_batch(scheduler,batch):
                """Enqueue batches of tasks scheduling"""
                if not __package__:
                    from computer.control import SparrowProbe
                else:
                    from .computer.control import SparrowProbe
                num_probes = int(scheduler.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['NUM_SPARROW_PROBES'])
                for idx,server in enumerate(scheduler.servers):
                    if idx >= num_probes:
                        break
                    for task in batch:
                        def schedule_task(task=task, server=server, scheduler=scheduler):
                            probe, = tuple(probe for probe in scheduler.controls if isinstance(probe,SparrowProbe) and probe.task is task)
                            probe.bind(server)
                            server.control()
                        scheduler.simulation.event_queue.put(
                            scheduler.cluster.network.delay(
                                schedule_task, logging_message=f'Send task: {task.id} to be scheduled on server {server.id}. Simulation time: {scheduler.simulation.time}.'
                            )
                        )

            def batch_sampling_enqueue_task_notify(probe,server):
                if not __package__:
                    from computer.abstract_base_classes import SchedulerClass, ServerClass
                else:
                    from .computer.abstract_base_classes import SchedulerClass, ServerClass
                scheduler, = tuple(binding for binding in probe.bindings if isinstance(binding, SchedulerClass))
                num_probes = int(probe.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['NUM_SPARROW_PROBES'])
                batch_control = probe.batch_control
                batch_size = int(probe.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['BATCH_SIZE'])
                probes = [probe for probe in batch_control.probes]
                ready_probe_servers = {
                    probe: [binding for binding in probe.bindings if isinstance(binding, ServerClass) and probe.target_states[binding]==probe.states.server_ready]
                    for probe in probes
                }
                ready_probes = set(probe for probe in probes if len(ready_probe_servers[probe]) == num_probes)
                ready_servers = set(server for probe in ready_probe_servers for server in ready_probe_servers[probe])
                if len(ready_probes) == batch_size:
                    server_queue_lengths = {server: 0 for server in ready_servers}
                    for probe in ready_probes:
                        min_server = min(probe.server_queue_lengths, key=lambda server: max(server_queue_lengths[server], probe.server_queue_lengths[server]))
                        scheduler.logger.debug(
                            f'Scheduler has selected server: {server.id}, to enqueue task: {probe.task.id}. Simulation time: {probe.simulation.time}.'
                        )
                        def enqueue_task(server=min_server, probe=probe):
                            probe.target_states[server] = probe.states.server_executing_task
                            server.control()
                        event = scheduler.cluster.network.delay(
                            enqueue_task, logging_message=f'Send message to server: {server.id}, enqueue task: {probe.task.id}. Simulation time: {server.simulation.time}.'
                        )
                        probe.simulation.event_queue.put(
                            event
                        )
                        queue_length = probe.server_queue_lengths[min_server] + 1
                        server_queue_lengths[min_server] = queue_length
                        other_servers = [server for server in ready_servers if not server is min_server]
                        for server in other_servers:
                            scheduler.logger.debug(
                                f'Scheduler has selected a different server to enqueue task: {probe.task.id}. Simulation time: {probe.simulation.time}.'
                            )
                            def unbind_probe(server=server, probe=probe):
                                probe.target_states[server] = probe.states.terminate
                                server.control()
                            scheduler.cluster.network.delay(
                                unbind_probe, logging_message=f'Send message to server: {server.id}, do not enqueue task: {probe.task.id}. Simulation time: {server.simulation.time}.'
                            )

            def scheduler_enqueue_task_notify(probe, server):
                if not __package__:
                    from computer.abstract_base_classes import SchedulerClass, ServerClass
                else:
                    from .computer.abstract_base_classes import SchedulerClass, ServerClass
                scheduler, = tuple(binding for binding in probe.bindings if isinstance(binding,SchedulerClass))
                if probe.task.is_finished:
                    # Probe task is finished respond to server that task is done.
                    scheduler.logger.debug(f'Task: {probe.task.id} has already finished, informing server: {server.id}. Simulation Time: {probe.simulation.time}')
                    def unbind_probe(probe=probe, server=server):
                        probe.target_states[server]=probe.states.terminated
                        server.control()
                    event = server.network.delay(
                        unbind_probe, logging_message=f'Send message to server: {server.id}, task: {probe.task.id} is finished. Simulation time: {server.simulation.time}'
                    )
                    server.tasks[probe.task]=event
                    probe.simulation.event_queue.put(
                        event
                    )
                elif not probe.enqueued:
                    probe.target_states[scheduler] = probe.states.server_executing_task
                    scheduler.logger.debug(
                        f'Scheduler has received response from sparrow probe: {probe.id}, for task: {probe.task.id}, from server: {server.id}. '
                        + f'Enqueuing task on server. Simulation time: {probe.simulation.time}.'
                    )
                    def enqueue_task(server=server):
                        server.control()
                    event = scheduler.cluster.network.delay(
                        enqueue_task, logging_message=f'Send Message to Server: {server.id} to enqueue task: {probe.task.id}. Simulation Time: {probe.simulation.time}'
                    )
                    server.tasks[probe.task] = event #Block subsequent probes. This will be overwritten by the server once the task is successfully enqueued.
                    probe.simulation.event_queue.put(
                        event
                    )
                    if probe.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['PREEMPTION'].lower() == 'true':
                        bindings = [binding for binding in probe.bindings if isinstance(binding, ServerClass) and binding is not server]
                        for binding in bindings:
                            def unbind_probe(probe=probe, server=binding):
                                probe.target_states[server] = probe.states.terminated
                                server.control()
                            event = scheduler.cluster.network.delay(
                                unbind_probe, logging_message=f'Send Preemption for probe: {probe.id} to server: {server.id} for task: {probe.task.id}. Simulation Time: {probe.simulation.time}.'
                            )
                            probe.simulation.event_queue.put(
                                event
                            )
                else:
                    scheduler.logger.debug(f'Task: {probe.task.id}, has been enqueued already. Rejecting the response. Simulation time: {probe.simulation.time}')
                    def unbind_probe(probe=probe, server=server):
                        probe.target_states[server] = probe.states.terminated
                        server.control()
                    event = scheduler.cluster.network.delay(
                        unbind_probe, logging_message=f'Send Message to Server: {server.id} do not enqueue task: {probe.task.id}. Simulation Time: {probe.simulation.time}'
                    )
                    server.tasks[probe.task]=event
                    probe.simulation.event_queue.put(
                        event
                    )

            def enqueue_task(fn):
                def func(*args):
                    probe = args[0]
                    policy = probe.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']
                    match policy:
                        case 'Sparrow':
                            late_binding = probe.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['LATE_BINDING']
                            preemption = probe.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['PREEMPTION']
                            if late_binding.lower() == 'true':
                                SparrowScheduler.Scheduler.Enqueuing.scheduler_enqueue_task_notify(*args)
                            else:
                                if preemption.lower() == 'true':
                                    raise ValueError(f'Sparrow sampling configuration does not support preemption.')
                                SparrowScheduler.Scheduler.Enqueuing.batch_sampling_enqueue_task_notify(*args)
                        case _:
                            fn(*args)
                return func

        class Executor:
            def task_complete(scheduler, task, server=None):
                if not __package__:
                    from computer.control import SparrowProbe
                else:
                    from .computer.control import SparrowProbe
                server.logger.debug(
                    f'Task: {task.id} completed on server: {server.id}. Simulation time: {scheduler.simulation.time}'
                )
                probe, = tuple(control for control in scheduler.controls if isinstance(control, SparrowProbe) and task is control.task)
                probe.target_states[scheduler] = probe.states.task_finished
                scheduler.control()

    class Server:
        class Queue:
            def block(scheduler, task, server):
                """Sparrow will block all task scheduling requests. 
                Instead task scheduling is handled by control signals in the SparrowProbes."""
                if not __package__:
                    from computer.control import SparrowProbe
                else:
                    from .computer.control import SparrowProbe
                try:
                    probe, = tuple(probe for probe in scheduler.controls if isinstance(probe, SparrowProbe) and probe.task is task)
                except ValueError as e:
                    # New probe.
                    probe = SparrowProbe(scheduler.simulation, task)
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
                if not __package__:
                    from computer.control import SparrowProbe
                else:
                    from .computer.control import SparrowProbe
                probe, = tuple(control for control in server.controls if isinstance(control, SparrowProbe) and task is control.task)
                probe.target_states[server] = probe.states.task_finished
                server.control()

        class Control:
            def late_binding_server_control(probe, target):
                if not __package__:
                    from computer.control import SparrowProbe
                else:
                    from .computer.control import SparrowProbe
                server = target
                match probe.target_states[server]:
                    case probe.states.server_probed:
                        probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.server_probed}, simulation time: {probe.simulation.time}')
                        if server.busy_until == server.simulation.time:
                            probes_on_server = [control for control in server.controls if isinstance(control, SparrowProbe)]
                            earliest_probe = probe
                            if len(probes_on_server) > 0:
                                early_probe = min(probes_on_server, key=lambda probe: probe.server_arrival_times[server])
                                if early_probe.server_arrival_times[server] < probe.server_arrival_times[server]:
                                    earliest_probe = probe
                            if earliest_probe is probe:
                                event = server.network.delay(
                                    probe.server_enqueue_task, server, logging_message=f'Send message to probe, ready to enqueue task: {probe.task.id} on server {server.id}. Simulation Time: {probe.simulation.time}'
                                )
                                server.tasks[probe.task] = event #Block subsequent probes. This will be overwritten by the server once the task is successfully enqueued.
                                probe.target_states[target] = probe.states.server_ready
                                probe.simulation.event_queue.put(
                                    event
                                )
                                probe.target_states[server] = probe.states.server_ready
                    case probe.states.server_ready:
                        probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.server_ready}, simulation time: {probe.simulation.time}')
                        if server.busy_until == probe.simulation.time:
                            event = server.enqueue_task(probe.task)
                            server.tasks[probe.task] = event
                            probe.simulation.event_queue.put(
                                event
                            )
                            probe.target_states[server] = probe.states.server_executing_task
                    case probe.states.server_executing_task:
                        probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.server_executing_task}, simulation time: {probe.simulation.time}')
                        pass
                    case probe.states.task_finished:
                        probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.task_finished}, simulation time: {probe.simulation.time}')
                        probe.unbind(server)
                    case probe.states.terminated:
                        probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.terminated}, simulation time: {probe.simulation.time}')
                        probe.unbind(server)

            def sampling_server_control(probe, target):
                server = target
                match probe.target_states[server]:
                    case probe.states.blocked:
                        probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state{ probe.states.blocked}, simulation time: {probe.simulation.time}.')
                    case probe.states.server_probed:
                        # Server probed needs to be idempotent.
                        probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.server_probed}, simulation time: {probe.simulation.time}')
                        def update_probe(probe=probe, server=server):
                            server.logger.debug(f'Update server: {server.id}, to state: {probe.states.server_ready}, for task {probe.task.id}. Simulation time: {probe.simulation.time}.')
                            probe.target_states[server] = probe.states.server_ready
                            probe.server_enqueue_task(server)
                        event = server.network.delay(
                            update_probe, logging_message=f'Notify probe, that server: {server.id} is ready for task: {probe.task.id}.'
                        )
                        probe.simulation.event_queue.put(
                            event
                        )
                        probe.target_states[server] = probe.states.blocked
                    case probe.states.server_ready:
                        # Server probe blocks in ready state until notified by probe.
                        probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.server_ready}, simulation time: {probe.simulation.time}.')
                    case probe.states.server_executing_task:
                        if server.busy_until == probe.simulation.time:
                            # If the server is currently idle.
                            probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.server_executing_task}, simulation time: {probe.simulation.time}')
                            event = server.enqueue_task(probe.task)
                            server.tasks[probe.task] = event
                            probe.simulation.event_queue.put(
                                event
                            )
                            probe.target_states[server] = probe.states.task_finished 
                    case probe.states.task_finished:
                        if server.busy_until == probe.simulation.time:
                            # If the server is currently idle.
                            probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.task_finished}, simulation time: {probe.simulation.time}')
                            probe.unbind(server)
                    case probe.states.terminated:
                        probe.logger.debug(f'Server: {target.id}, control loop for Sparrow Probe: {probe.id}, state: {probe.states.terminated}, simulation time: {probe.simulation.time}')
                        probe.unbind(server)        

            def server_control_select(fn):
                def func(*args):
                    probe = args[0]
                    late_binding = probe.simulation.CONFIGURATION['Computer.Scheduler.Sparrow']['LATE_BINDING']
                    if late_binding.lower() == 'false':
                        SparrowScheduler.Server.Control.sampling_server_control(*args)
                    else:
                        SparrowScheduler.Server.Control.late_binding_server_control(*args)
                return func
            
class LatinSquareScheduler:
    class Scheduler:
        class Enqueuing:
            def latin_square(scheduler, job):
                from numpy import array_split
                from math import ceil
                if not __package__:
                    from computer.control import LatinSquareBatch
                else:
                    from .computer.control import LatinSquareBatch
                tasks = job.tasks
                batch_size = int(scheduler.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['LATIN_SQUARE_ORDER'])
                batches = [tuple(tasks.tolist()) for tasks in array_split(tasks,ceil(len(tasks))/batch_size)]
                scheduler.logger.info(f'Work batches to be scheduled are {[(task.id, task.start_time) for batch in batches for task in batch]}. Simulation Time: {scheduler.simulation.time}')
                for batch in batches:
                    batch_control = LatinSquareBatch(scheduler.simulation, batch)
                    batch_control.bind(scheduler)
                    scheduler.schedule_batch(batch)

            def schedule_batch(scheduler,batch):
                """Enqueue batches of tasks scheduling"""
                if not __package__:
                    from computer.control import LatinSquareControl
                else:
                    from .computer.control import LatinSquareControl
                from collections import deque
                num_tasks = int(scheduler.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['LATIN_SQUARE_ORDER'])
                batch = deque(batch)
                for i in range(num_tasks):
                    if i >= num_tasks:
                        break
                    sequenced_batch = [task for task in batch]
                    server = next(scheduler.servers)
                    def schedule_batch(batch=sequenced_batch, server=server, scheduler=scheduler):
                        for task in batch:
                            control, = tuple(control for control in scheduler.controls if isinstance(control,LatinSquareControl) and control.task is task)
                            control.bind(server)
                        server.control()
                    scheduler.simulation.event_queue.put(
                        scheduler.cluster.network.delay(
                            schedule_batch, logging_message=f'Send batch: {[task for task in sequenced_batch]} to be scheduled on server {server.id}. Simulation time: {scheduler.simulation.time}.'
                        )
                    )
                    batch.rotate()

            def batch_sampling_enqueue_task_notify(probe,server):
                if not __package__:
                    from computer.abstract_base_classes import SchedulerClass, ServerClass
                else:
                    from .computer.abstract_base_classes import SchedulerClass, ServerClass
                scheduler, = tuple(binding for binding in probe.bindings if isinstance(binding, SchedulerClass))
                num_tasks = int(probe.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['LATIN_SQUARE_ORDER'])
                batch_control = probe.batch_control
                controls = [control for control in batch_control.controls]
                ready_control_servers = {
                    control: [binding for binding in probe.bindings if isinstance(binding, ServerClass) and control.target_states[binding]==control.states.server_ready]
                    for control in controls
                }
                ready_controls = set(control for control in controls if len(ready_control_servers[control]) == num_tasks)
                ready_servers = set(server for control in ready_control_servers for server in ready_control_servers[control])
                if len(ready_controls) == num_tasks:
                    server_queue_lengths = {server: 0 for server in ready_servers}
                    for control in ready_controls:
                        min_server = min(control.server_queue_lengths, key=lambda server: max(server_queue_lengths[server], control.server_queue_lengths[server]))
                        scheduler.logger.debug(
                            f'Scheduler has selected server: {server.id}, to enqueue task: {control.task.id}. Simulation time: {control.simulation.time}.'
                        )
                        def enqueue_task(server=min_server, control=control):
                            control.target_states[server] = control.states.server_executing_task
                            server.control()
                        event = scheduler.cluster.network.delay(
                            enqueue_task, logging_message=f'Send message to server: {server.id}, enqueue task: {control.task.id}. Simulation time: {server.simulation.time}.'
                        )
                        control.simulation.event_queue.put(
                            event
                        )
                        queue_length = control.server_queue_lengths[min_server] + 1
                        server_queue_lengths[min_server] = queue_length
                        other_servers = [server for server in ready_servers if not server is min_server]
                        for server in other_servers:
                            scheduler.logger.debug(
                                f'Scheduler has selected a different server to enqueue task: {control.task.id}. Simulation time: {control.simulation.time}.'
                            )
                            def unbind_control(server=server, control=control):
                                control.target_states[server] = control.states.terminate
                                server.control()
                            scheduler.cluster.network.delay(
                                unbind_control, logging_message=f'Send message to server: {server.id}, do not enqueue task: {control.task.id}. Simulation time: {server.simulation.time}.'
                            )

            def scheduler_enqueue_task_notify(control, server):
                if not __package__:
                    from computer.abstract_base_classes import SchedulerClass
                else:
                    from .computer.abstract_base_classes import SchedulerClass
                scheduler, = tuple(binding for binding in control.bindings if isinstance(binding,SchedulerClass))
                if not control.task.is_finished:
                    scheduler.logger.debug(
                        f'Scheduler has received response from LatinSquare control: {control.id}, for task: {control.task.id}, from server: {server.id}. '
                        + f'Enqueuing task on server. Simulation time: {control.simulation.time}.'
                    )
                    def enqueue_task(server=server, control=control):
                        server.control()
                        control.target_states[server] = control.states.server_ready
                        server.control()
                    event = scheduler.cluster.network.delay(
                        enqueue_task, logging_message=f'Send Message to Server: {server.id} to enqueue task: {control.task.id}. Simulation Time: {control.simulation.time}'
                    )
                    server.tasks[control.task] = event #Block subsequent controls. This will be overwritten by the server once the task is successfully enqueued.
                    control.simulation.event_queue.put(
                        event
                    )
                else:
                    # control task is finished respond to server that task is done.
                    scheduler.logger.debug(f'Task: {control.task.id} has already finished, informing server: {server.id}. Simulation Time: {control.simulation.time}')
                    def unbind_control(control=control, server=server):
                        control.target_states[server]=control.states.terminated
                        server.control()
                    event = server.network.delay(
                        unbind_control, logging_message=f'Send message to server: {server.id}, task: {control.task.id} is finished. Simulation time: {server.simulation.time}'
                    )
                    server.tasks[control.task]=event
                    control.simulation.event_queue.put(
                        event
                    )

            def enqueue_task(fn):
                    def func(*args):
                        control = args[0]
                        policy = control.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']
                        match policy:
                            case 'LatinSquare':
                                late_binding = control.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['LATE_BINDING']
                                preemption = control.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['PREEMPTION']
                                if late_binding.lower() == 'true':
                                    LatinSquareScheduler.Scheduler.Enqueuing.scheduler_enqueue_task_notify(*args)
                                else:
                                    if preemption.lower() == 'true':
                                        raise ValueError(f'LatinSquare sampling configuration does not support preemption.')
                                    LatinSquareScheduler.Scheduler.Enqueuing.batch_sampling_enqueue_task_notify(*args)
                            case _:
                                fn(*args)
                    return func

        class Executor:
            def task_complete(scheduler, task, server=None):
                if not __package__:
                    from computer.control import LatinSquareControl
                else:
                    from .computer.control import LatinSquareControl
                server.logger.debug(
                    f'Task: {task.id} completed on server: {server.id}. Simulation time: {scheduler.simulation.time}'
                )
                control, = tuple(control for control in scheduler.controls if isinstance(control, LatinSquareControl) and task is control.task)
                control.target_states[scheduler] = control.states.task_finished
                scheduler.control()

        class Control:
            def late_binding_scheduler_preemption(control, scheduler):
                if not __package__:
                    from computer.control import LatinSquareControl
                    from computer.abstract_base_classes import ServerClass
                else:
                    from .computer.control import LatinSquareControl
                    from .computer.abstract_base_classes import ServerClass
                if isinstance(control, LatinSquareControl) and control.task.is_finished:
                    bound_servers = set(binding for binding in control.bindings if isinstance(binding, ServerClass))
                    for server in bound_servers:
                        # if control.target_states[server] != control.states.server_executing_task:
                        def preempt_task(server=server, control=control):
                            control.target_states[server] = control.states.terminated
                            try:
                                event = server.tasks.pop(control.task)
                            except KeyError:
                                server.logger.debug(f'Task: {control.task.id}, already cleared from server: {server.id}. Simulation time: {server.simulation.time}.')
                            else:
                                server.logger.debug(f'Task: {control.task.id}, preempted from server: {server.id}. Simulation time: {server.simulation.time}.')
                                event.cancel()
                            finally:
                                server.control()
                        scheduler.simulation.event_queue.put(
                            scheduler.cluster.network.delay(
                                preempt_task, logging_message=f'Preempt task: {control.task.id}, on server: {server.id}. Simulation time: {scheduler.simulation.time}.'
                            )
                        )


            def scheduler_control_select(fn):
                def func(*args):
                    control = args[0]
                    late_binding = control.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['LATE_BINDING']
                    preemption = control.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['PREEMPTION']
                    if preemption.lower() == 'true' and late_binding.lower() == 'true':
                        LatinSquareScheduler.Scheduler.Control.late_binding_scheduler_preemption(*args)
                return func
    class Server:
        class Queue:
           def block(scheduler, task, server):
                """LatinSquare will block all task scheduling requests. 
                Instead task scheduling is handled by control signals in the LatinSquareControls."""
                if not __package__:
                    from computer.control import LatinSquareControl
                else:
                    from .computer.control import LatinSquareControl
                try:
                    control, = tuple(control for control in scheduler.controls if isinstance(control, LatinSquareControl) and control.task is task)
                except ValueError:
                    # New Control.
                    control = LatinSquareControl(scheduler.simulation, task)
                    control.bind(scheduler)
                else:
                    # Control already bound to scheduler.
                    pass
                finally:
                    # Control already bound to scheduler.
                    control.bind(server)
                if server.busy_until == scheduler.simulation.time:
                    server.control()
                    
        class Executor:
            def task_complete(server, task):
                """LatinSquare tasks are scheduled by RPC. So after
                tasks complete on the server, the server needs to enter the control loop.
                """
                if not __package__:
                    from computer.control import LatinSquareControl
                else:
                    from .computer.control import LatinSquareControl
                try:
                    control, = tuple(control for control in server.controls if isinstance(control, LatinSquareControl) and task is control.task)
                except ValueError:
                    server.logger.debug(f'Task: {task.id}, preempted on server: {server.id}. Simulation time: {server.simulation.time}.')
                else:
                    control.target_states[server] = control.states.task_finished
                finally:
                    server.control()

        class Control:
            def late_binding_server_control(control, target):
                if not __package__:
                    from computer.control import LatinSquareControl
                else:
                    from .computer.control import LatinSquareControl
                server = target
                match control.target_states[server]:
                    case control.states.blocked:
                        control.logger.debug(f'Server: {target.id}, control loop for LatinSquare Control: {control.id}, state: {control.states.blocked}, simulation time: {control.simulation.time}.')
                    case control.states.server_enqueued:
                        control.logger.debug(f'Server: {target.id}, control loop for LatinSquare Control: {control.id}, state: {control.states.server_enqueued}, simulation time: {control.simulation.time}')
                        if server.busy_until == server.simulation.time:
                            controls_on_server = [control for control in server.controls if isinstance(control, LatinSquareControl)]
                            earliest_control = control
                            if len(controls_on_server) > 0:
                                min_control = min(controls_on_server, key=lambda control: control.server_arrival_times[server])
                                if earliest_control.server_arrival_times[server] < min_control.server_arrival_times[server]:
                                    earliest_control = min_control
                            if earliest_control is control:
                                event = server.network.delay(
                                    control.server_enqueue_task, server, logging_message=f'Send message to control, ready to enqueue task: {control.task.id} on server {server.id}. Simulation Time: {control.simulation.time}'
                                )
                                server.tasks[control.task] = event #Block subsequent controls. This will be overwritten by the server once the task is successfully enqueued.
                                control.simulation.event_queue.put(
                                    event
                                )
                                control.target_states[server] = control.states.blocked #Block subsequent controls until scheduler response.
                    case control.states.server_ready:
                        control.logger.debug(f'Server: {target.id}, control loop for LatinSquare Control: {control.id}, state: {control.states.server_ready}, simulation time: {control.simulation.time}')
                        if server.busy_until == control.simulation.time:
                            event = server.enqueue_task(control.task)
                            server.tasks[control.task] = event
                            control.simulation.event_queue.put(
                                event
                            )
                            control.target_states[server] = control.states.server_executing_task
                    case control.states.server_executing_task:
                        control.logger.debug(f'Server: {target.id}, control loop for LatinSquare Control: {control.id}, state: {control.states.server_executing_task}, simulation time: {control.simulation.time}')
                    case control.states.task_finished:
                        control.logger.debug(f'Server: {target.id}, control loop for LatinSquare control: {control.id}, state: {control.states.task_finished}, simulation time: {control.simulation.time}')
                        control.unbind(server)
                    case control.states.terminated:
                        control.logger.debug(f'Server: {target.id}, control loop for LatinSquare control: {control.id}, state: {control.states.terminated}, simulation time: {control.simulation.time}')
                        control.unbind(server)

            def sampling_server_control(control, target):
                server = target
                match control.target_states[server]:
                    case control.states.blocked:
                        control.logger.debug(f'Server: {target.id}, control loop for LatinSquare control: {control.id}, state{ control.states.blocked}, simulation time: {control.simulation.time}.')
                    case control.states.server_enqueued:
                        # Server control needs to be idempotent.
                        control.logger.debug(f'Server: {target.id}, control loop for LatinSquare control: {control.id}, state: {control.states.server_enqueued}, simulation time: {control.simulation.time}')
                        def update_control(control=control, server=server):
                            server.logger.debug(f'Update server: {server.id}, to state: {control.states.server_ready}, for task {control.task.id}. Simulation time: {control.simulation.time}.')
                            control.target_states[server] = control.states.server_ready
                            control.server_enqueue_task(server)
                        event = server.network.delay(
                            update_control, logging_message=f'Notify control, that server: {server.id} is ready for task: {control.task.id}.'
                        )
                        control.simulation.event_queue.put(
                            event
                        )
                        control.target_states[server] = control.states.blocked
                    case control.states.server_ready:
                        # Server control blocks in ready state until notified by control.
                        control.logger.debug(f'Server: {target.id}, control loop for LatinSquare control: {control.id}, state: {control.states.server_ready}, simulation time: {control.simulation.time}.')
                    case control.states.server_executing_task:
                        if server.busy_until == control.simulation.time:
                            # If the server is currently idle.
                            control.logger.debug(f'Server: {target.id}, control loop for LatinSquare control: {control.id}, state: {control.states.server_executing_task}, simulation time: {control.simulation.time}')
                            event = server.enqueue_task(control.task)
                            server.tasks[control.task] = event
                            control.simulation.event_queue.put(
                                event
                            )
                            control.target_states[server] = control.states.task_finished 
                    case control.states.task_finished:
                        if server.busy_until == control.simulation.time:
                            # If the server is currently idle.
                            control.logger.debug(f'Server: {target.id}, control loop for LatinSquare control: {control.id}, state: {control.states.task_finished}, simulation time: {control.simulation.time}')
                            control.unbind(server)
                    case control.states.terminated:
                        control.logger.debug(f'Server: {target.id}, control loop for LatinSquare control: {control.id}, state: {control.states.terminated}, simulation time: {control.simulation.time}')
                        control.unbind(server)    

            def server_control_select(fn):
                def func(*args):
                    control = args[0]
                    late_binding = control.simulation.CONFIGURATION['Computer.Scheduler.LatinSquare']['LATE_BINDING']
                    if late_binding.lower() == 'false':
                        LatinSquareScheduler.Server.Control.sampling_server_control(*args)
                    else:
                        LatinSquareScheduler.Server.Control.late_binding_server_control(*args)
                return func