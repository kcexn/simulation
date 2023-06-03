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

    def sparrow(scheduler, job):
        from numpy import array_split
        tasks = job.tasks
        num_probes = int(scheduler.simulation.CONFIGURATION['Computer.Scheduler']['NUM_SPARROW_PROBES'])
        work = [work.tolist() for work in array_split(tasks,len(tasks))]
        scheduler.logger.info(f'Work batches to be scheduled are {[(task.id, task.start_time) for batch in work for task in batch]}. Simulation Time: {scheduler.simulation.time}')
        for batch in work:
            for _ in range(num_probes):
                scheduler.schedule_batch(batch)

    def sparrow_batch(scheduler,job):
        from numpy import array_split
        from math import ceil
        from computer.control import SparrowBatch
        tasks = job.tasks
        latin_square_order = int(scheduler.simulation.CONFIGURATION['Computer.Scheduler']['LATIN_SQUARE_ORDER'])
        def square(n):
            return [[(i+j)%n for i in range(n)] for j in range(n)]
        latin_square = square(latin_square_order)
        num_probes = int(scheduler.simulation.CONFIGURATION['Computer.Scheduler']['NUM_SPARROW_PROBES'])
        work = [tuple(work.tolist()) for work in array_split(tasks,ceil(len(tasks))/latin_square_order)]
        scheduler.logger.info(f'Work batches to be scheduled are {[(task.id, task.start_time) for batch in work for task in batch]}. Simulation Time: {scheduler.simulation.time}')
        for batch in work:
            batch_control = SparrowBatch(scheduler.simulation, batch)
            batch_control.bind(scheduler)
            for sequence in latin_square:
                sequenced_batch = tuple(batch[idx] for idx in sequence)
                batch_control.batches.append(sequenced_batch)
                for _ in range(num_probes):
                    scheduler.schedule_batch(sequenced_batch)

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
                case 'LatinSquare':
                    SchedulingPolicies.latin_square(*args)
                    return fn(*args)
                case 'Sparrow':
                    SchedulingPolicies.sparrow(*args)
                    return fn(*args)
                case 'SparrowBatch':
                    SchedulingPolicies.sparrow_batch(*args)
                    return fn(*args)
        return func
    
    # Task Batching Policies
    def schedule_batch(self,batch):
        """Enqueue batches of tasks scheduling"""
        server = next(self.servers)
        def schedule_tasks(batch=batch, schedule_task=self.schedule_task, server=server):
            for task in batch:
                schedule_task(task,server)
        self.simulation.event_queue.put(
            self.cluster.network.delay(
                schedule_tasks, logging_message=f'Send tasks {[task for task in batch]} to be scheduled on server {server.id}. Simulation Time: {self.simulation.time}.'
            )
        )

    def sparrow_schedule_sequenced_batch(scheduler, batch):
        from computer.control import SparrowBatch
        server = next(scheduler.servers)
        batch_control, = tuple(control for control in scheduler.controls if isinstance(control, SparrowBatch) and batch in control.batches)
        batch_control.bind(server, batch=batch)    
        scheduler.logger.info(f'Schedule Tasks: {[task.id for task in batch]} on Server: {server.id}, Simulation Time: {scheduler.simulation.time}')
        def schedule_tasks(batch=batch, schedule_task=scheduler.schedule_task, server=server):
            server.logger.debug(f'Probes for tasks: {[task.id for task in batch]}, arrived at server: {server.id}. Simulation time: {server.simulation.time}.')
            for task in batch:
                schedule_task(task,server)
        scheduler.simulation.event_queue.put(
            scheduler.cluster.network.delay(
                schedule_tasks, logging_message=f'Send tasks {[task.id for task in batch]} to be scheduled on server {server.id}. Simulation Time: {scheduler.simulation.time}.'
            )
        )
    
    def batch_schedule(fn):
        """Decorator to choose a batching policy"""
        def func(*args):
            policy = args[0].POLICY
            match policy:
                case 'SparrowBatch':
                    SchedulingPolicies.sparrow_schedule_sequenced_batch(*args)
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
            num_probes = int(cluster.simulation.CONFIGURATION['Computer.Scheduler']['NUM_SPARROW_PROBES'])
            latin_square_order = int(cluster.simulation.CONFIGURATION['Computer.Scheduler']['LATIN_SQUARE_ORDER'])
            match cluster.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']:
                case 'Sparrow':
                    return ServerSelectionPolicies.random(cluster, num_probes)
                case 'SparrowBatch':
                    return ServerSelectionPolicies.random(cluster, num_probes*latin_square_order)
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

    def sparrow_blocking(scheduler, task, server):
        """Sparrow will block all task scheduling requests. 
        Instead task scheduling is handled by control signals in the SparrowProbes."""
        from computer.control import SparrowProbe
        try:
            probe, = tuple(probe for probe in scheduler.controls if isinstance(probe, SparrowProbe) and probe.task is task)
        except ValueError as e:
            # New probe.
            probe = SparrowProbe(scheduler.simulation, task)
            probe.bind(scheduler)
        except Exception as e:
            scheduler.logger.debug(f'Unexpected Error: {e}')
        else:
            # Probe already bound to scheduler.
            pass
        finally:
            # Probe already bound to scheduler.
            probe.bind(server)
        if server.busy_until == scheduler.simulation.time:
            server.control()

    def blocking(fn):
        """Decorator to Choose a Blocking Policy."""
        def func(*args):
            scheduler = args[0]
            match scheduler.POLICY:
                case 'Sparrow':
                    BlockingPolicies.sparrow_blocking(*args)
                case 'SparrowBatch':
                    BlockingPolicies.sparrow_blocking(*args)
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

    def sparrow_task_completion(server):
        """Sparrow tasks are scheduled by RPC. So after
        tasks complete on the server, the server needs to enter the control loop.
        """
        server.control()

    def task_completion(fn):
        def func(*args):
            server = args[0]
            match server.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']:
                case 'Sparrow':
                    ServerTaskExecutionPolicies.sparrow_task_completion(*args)
                case 'SparrowBatch':
                    ServerTaskExecutionPolicies.sparrow_task_completion(*args)
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

    def sparrow_task_completion(scheduler, task, server=None):
        from computer.control import SparrowProbe
        server.logger.debug(
            f'Task: {task.id} completed on server: {server.id}. Simulation time: {scheduler.simulation.time}'
        )
        probe, = tuple(control for control in scheduler.controls if isinstance(control, SparrowProbe) and task is control.task)
        probe.unbind(server)
        server.complete_task()

    def task_complete(fn):
        def func(*args, **kwargs):
            fn(*args)
            scheduler = args[0]
            policy = scheduler.POLICY
            match policy:
                case 'Sparrow':
                    SchedulerTaskCompletionPolicies.sparrow_task_completion(*args, **kwargs)
                case 'SparrowBatch':
                    SchedulerTaskCompletionPolicies.sparrow_task_completion(*args, **kwargs)
                case _:
                    SchedulerTaskCompletionPolicies.default_task_completion(*args)
        return func

class SparrowConfiguration:
    # Sparrow Probe Task Enqueuing Policies.
    def server_enqueue_task(probe, server):
        from computer.abstract_base_classes import SchedulerClass, ServerClass
        scheduler, = tuple(binding for binding in probe.bindings if isinstance(binding,SchedulerClass))
        if probe.task.is_finished:
            # Probe task is finished respond to server that task is done.
            scheduler.logger.debug(f'Task: {probe.task.id} has already finished, informing server: {server.id}. Simulation Time: {probe.simulation.time}')
            def unbind_probe(probe=probe, server=server):
                probe.unbind(server)
                server.control()
            event = server.network.delay(
                unbind_probe, logging_message=f'Send message to server: {server.id}, task: {probe.task.id} is finished. Simulation time: {server.simulation.time}'
            )
            server.tasks[probe.task]=event
            probe.simulation.event_queue.put(
                event
            )
        elif not probe.enqueued:
            probe.enqueued = True
            scheduler.logger.debug(
                f'Scheduler has received response from sparrow probe: {probe.id}, for task: {probe.task.id}, from server: {server.id}. '
                + f'Enqueuing task on server. Simulation time: {probe.simulation.time}.'
            )
            event = scheduler.cluster.network.delay(
                server.enqueue_task, probe.task, logging_message=f'Send Message to Server: {server.id} to enqueue task: {probe.task.id}. Simulation Time: {probe.simulation.time}'
            )
            server.tasks[probe.task] = event #Block subsequent probes. This will be overwritten by the server once the task is successfully enqueued.
            probe.simulation.event_queue.put(
                event
            )
            if probe.simulation.CONFIGURATION['Computer.Scheduler']['PREEMPTION'].lower() == 'true':
                bindings = [binding for binding in probe.bindings if isinstance(binding, ServerClass) and binding is not server]
                for binding in bindings:
                    def unbind_probe(probe=probe, server=binding):
                        probe.unbind(server)
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
                probe.unbind(server)
                server.control()
            event = scheduler.cluster.network.delay(
                unbind_probe, logging_message=f'Send Message to Server: {server.id} do not enqueue task: {probe.task.id}. Simulation Time: {probe.simulation.time}'
            )
            server.tasks[probe.task]=event
            probe.simulation.event_queue.put(
                event
            )

    def batch_enqueue_task(probe, server):
        from computer.abstract_base_classes import SchedulerClass
        scheduler, = tuple(binding for binding in probe.bindings if isinstance(binding,SchedulerClass))
        if probe.task.is_finished:
            # Probe task is finished respond with next task in batch, or unbind probe from server.
            batch_control = probe.batch_control
            server_tasks = batch_control.server_tasks[server]
            unfinished_tasks = [task for task in server_tasks if not task.is_finished] #tasks in batch all arrive simultaneously.
            if len(unfinished_tasks) > 0:
                # Scheduled the next unfinished task on server.
                next_unfinished_task = unfinished_tasks[0]
                scheduler.logger.debug(
                    f'Scheduler has received response from sparrow probe: {probe.id}, for task: {probe.task.id}, from server: {server.id}.'
                    + f'Since task: {probe.task.id} is already finished. Task: {next_unfinished_task.id} will be enqueued on server instead. Simulation time: {probe.simulation.time}.'
                )
                event = scheduler.cluster.network.delay(
                    server.enqueue_task, next_unfinished_task, logging_message=f'Send Message to Server: {server.id} to enqueue task: {next_unfinished_task}. Simulation Time: {probe.simulation.time}'
                )
                server.tasks[probe.task] = event #Block subsequent probes. This will be overwritten by the server once the task is successfully enqueued.
                probe.simulation.event_queue.put(
                    event
                )
            else:
                # all tasks in batch are finished, notify the server to unbind from batch.
                def unbind_batch(batch_control = batch_control, server=server):
                    batch_control.unbind(server)
                    server.control()            
                event = scheduler.cluster.network.delay(
                    unbind_batch, logging_message=f'Send Message to Server: {server.id}, batch: {batch_control.id} is finished. Unbind all probes. Simulation Time: {scheduler.simulation.time}.'
                )
                server.tasks[probe.task] = event # Block server on RPC response.
                probe.simulation.event_queue.put(
                    event
                )
        else:
            scheduler.logger.debug(
                f'Scheduler has received response from sparrow probe: {probe.id}, for task: {probe.task.id}, from server: {server.id}. '
                + f'Enqueuing task on server. Simulation time: {probe.simulation.time}.'
            )
            event = scheduler.cluster.network.delay(
                server.enqueue_task, probe.task, logging_message=f'Send Message to Server: {server.id} to enqueue task: {probe.task.id}. Simulation Time: {probe.simulation.time}'
            )
            server.tasks[probe.task] = event #Block subsequent probes. This will be overwritten by the server once the task is successfully enqueued.
            probe.simulation.event_queue.put(
                event
            )      

    def enqueue_task(fn):
        def func(*args):
            probe = args[0]
            policy = probe.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']
            match policy:
                case 'Sparrow':
                    SparrowConfiguration.server_enqueue_task(*args)
                case 'SparrowBatch':
                    SparrowConfiguration.batch_enqueue_task(*args)
                case _:
                    fn(*args)
        return func