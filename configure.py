import configparser

def latin_square(n):
    return [[(i+j)%n for i in range(n)] for j in range(n)]

def configuration(filename='./configuration.ini'):
    config = configparser.ConfigParser()
    config.read(filename)
    return config

#TODO: Implement Sparrow Latin Square Hybrid. Tolerate Worker Failures, Improve Response Times.

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
        scheduler.logger.debug(f'Latin Square order is {scheduler.LATIN_SQUARE.shape[0]}')
        batch_size = scheduler.LATIN_SQUARE.shape[0]
        work = [work.tolist() for work in array_split(tasks, ceil(len(tasks)/batch_size))]
        scheduler.logger.info(f'work batches to be scheduled are {[(task.id, task.start_time) for batch in work for task in batch]}. Simulation Time: {scheduler.simulation.time}')
        for batch in work:
            for i in range(len(batch)):
                scheduled_order = [batch[scheduler.LATIN_SQUARE[i][j]] for j in range(len(batch))]
                scheduler.schedule_batch(scheduled_order)

    def sparrow(scheduler, job):
        from numpy import array_split
        tasks = job.tasks
        scheduler.logger.debug(f'Sparrow Scheduler')
        num_probes = int(scheduler.simulation.CONFIGURATION['Computer.Scheduler']['NUM_SPARROW_PROBES'])
        work = [work.tolist() for work in array_split(tasks,len(tasks))]
        scheduler.logger.info(f'Work batches to be scheduled are {[(task.id, task.start_time) for batch in work for task in batch]}. Simulation Time: {scheduler.simulation.time}')
        for batch in work:
            for _ in range(num_probes):
                scheduler.schedule_batch(batch)
    
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
        return func
    

class ServerSelectionPolicies:
    def cycle(cluster):
        """Cycle Through servers in cluster in an infinite loop, default policy."""
        import itertools
        return itertools.cycle(cluster.servers)
    
    def random(cluster):
        """Select a subset of servers randomly, and cycle through them."""
        while True:
            random_servers = cluster.random_choice.choose_from(cluster.servers, int(cluster.simulation.CONFIGURATION['Computer.Scheduler']['NUM_SPARROW_PROBES']))
            cluster.logger.debug(f'yielding servers: {[server.id for server in iter(random_servers)]}')
            for server in iter(random_servers):
                yield server

    def server_selection(fn):
        def func(*args):
            cluster = args[0]
            match cluster.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']:
                case 'Sparrow':
                    return ServerSelectionPolicies.random(cluster)
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
        scheduler.logger.debug(f'Server: {server.id} selected as a candidate to queue task: {task.id}')
        try:
            probe, = tuple(probe for probe in scheduler.controls if isinstance(probe, SparrowProbe) and probe.task is task)
        except ValueError as e:
            # New probe.
            scheduler.logger.debug(f'Sparrow Probe Unpacking Error: {e}. Simulation Time: {scheduler.simulation.time}')
            probe = SparrowProbe(scheduler.simulation, task)
            probe.bind(scheduler)
            scheduler.logger.debug(f'Sparrow Probe bound to Scheduler. Simulation Time: {scheduler.simulation.time}')
        except Exception as e:
            scheduler.logger.debug(f'Unexpected Error: {e}')
        else:
            # Probe already bound to scheduler.
            pass
        finally:
            # Probe already bound to scheduler.
            probe.bind(server)
            scheduler.logger.debug(f'Sparrow probe bound to server {server.id}. Simulation Time: {scheduler.simulation.time}')
        if server.busy_until <= scheduler.simulation.time:
            server.logger.debug(f'Server is idle, sparrow probe can be responded to instantly.')
            server.control()

    def blocking(fn):
        """Decorator to Choose a Blocking Policy."""
        def func(*args):
            scheduler = args[0]
            match scheduler.POLICY:
                case 'Sparrow':
                    BlockingPolicies.sparrow_blocking(*args)
                case _:
                    BlockingPolicies.infinite_queue(*args)
                    return fn(*args)
        return func
    
class ServerTaskExecutionPolicies:
    def default_task_reschedule(server, task, event):
        """Preempt the job, and move all subsequent tasks up in the queue.
        Task completion is idempotent so there is no need to remove or otherwise update all of the old events.
         they will just eventually clear from the event queue.
        """
        from copy import copy
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

    def sparrow_task_reschedule(server):
        """Sparrow tasks are scheduled by RPC. So after
        tasks complete on the server, the server needs to enter the control loop.
        """
        server.control()

    def task_reschedule(fn):
        def func(*args):
            event = fn(*args)
            server = args[0]
            match server.simulation.CONFIGURATION['Computer.Scheduler']['POLICY']:
                case 'Sparrow':
                    server.logger.debug(f'Server: {server.id} is entering its control loop.')
                    ServerTaskExecutionPolicies.sparrow_task_reschedule(server)
                    server.logger.debug(f'Server: {server.id} is exiting its control loop.')
                case _:
                    if not event is None:
                        ServerTaskExecutionPolicies.default_task_reschedule(*args, event)
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

    def sparrow_task_completion(scheduler, task):
        scheduler.control()

    def task_complete(fn):
        def func(*args):
            fn(*args)
            scheduler = args[0]
            policy = scheduler.POLICY
            match policy:
                case 'Sparrow':
                    SchedulerTaskCompletionPolicies.sparrow_task_completion(*args)
                case _:
                    SchedulerTaskCompletionPolicies.default_task_completion(*args)
        return func