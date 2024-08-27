import configparser
import logging

if not __package__:
    from schedulers import *
else:
    from .schedulers import *
    
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
                case 'Peacock':
                    PeacockScheduler.Scheduler.Enqueuing.peacock(*args)
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
                case 'Peacock':
                    try:
                        cluster_control_config = cluster.CONTROL_CONFIG
                    except AttributeError:
                        cluster_control_config = {
                            'server_selection': cluster.simulation.CONFIGURATION['Computer.Scheduler.Peacock']['SERVER_SELECTION']
                        }
                        cluster.CONTROL_CONFIG = cluster_control_config
                    else:
                        pass
                    finally:
                        match cluster_control_config['server_selection'].lower():
                            case 'random':
                                return ServerSelectionPolicies.random(cluster,1)
                            case _:
                                return ServerSelectionPolicies.cycle(cluster)
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
