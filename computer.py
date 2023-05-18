import logging
from numpy import array_split, array

from processes import CompletionProcess, ArrivalProcess


class Server(object):
    """Class to keep track of server status"""
    def __init__(self, simulation):
        self.simulation = simulation
        self.tasks = {}
        self.completion_process = CompletionProcess(simulation)

    def get_busy_until(self):
        events = [self.tasks[task] for task in list(self.tasks)]
        if len(events) == 0:
            return self.simulation.get_simulation_time()
        else:
            max_event = max(events)
            return max_event[0] 

    def get_current_task(self):
        if len(self.task_queue) > 0:
            return self.task_queue[0]
        else:
            return None
    
    def enqueue_task(self, task):
        time,event = self.completion_process.get_task_completion(task)
        interrenewal_time = time - self.simulation.get_simulation_time()
        logging.debug(f'interrenewal time for task {task.get_id()} is {interrenewal_time}, executing on server {self.get_id()}')
        completion_time = time + (self.get_busy_until() - self.simulation.get_simulation_time())
        self.tasks[task] = (completion_time, event)
        return (completion_time, event)


    def complete_task(self, task):
        """Task completion is idempotent"""
        try:
            time, event = self.tasks.pop(task)
        except KeyError:
            pass
        else:
            if time > self.simulation.get_simulation_time():
                event.cancel()
                reschedule_tasks = [task for task in list(self.tasks) if self.tasks[task][0]>=time and not task.get_job().is_finished()]
                logging.debug(f'rescheduling {[task.get_id() for task in reschedule_tasks]}')
                if len(reschedule_tasks)>0:
                    event_times = sorted([self.tasks.pop(task) for task in reschedule_tasks])
                    for _,event in event_times:
                        event.cancel()
                    for task in reschedule_tasks:
                        self.simulation.event_queue.put(
                            self.enqueue_task(task)
                        )

    def get_id(self):
        return id(self)


class Cluster(object):
    """A Collection of Servers"""
    NUM_SERVERS = 3
    def __init__(self,simulation):
        self.servers = [Server(simulation) for _ in range(Cluster.NUM_SERVERS)]
        logging.debug(f'servers have ids: {[server.get_id() for server in self.servers]}')

    def get_servers(self):
        return self.servers
    
    def get_num_servers(self):
        return Cluster.NUM_SERVERS


class Scheduler(object):
    POLICY = 'RoundRobin' # Currently Support RoundRobin, FullRepetition, LatinSquare
    LATIN_SQUARE = array([
        [0,1,2],
        [1,2,0],
        [2,0,1]
    ])
    def __init__(self, simulation):
        self.simulation = simulation
        self.cluster = Cluster(simulation)
        self.arrival_process = ArrivalProcess(simulation)
        self.completion_process = CompletionProcess(simulation)
        self.counter = 0

    def generate_job_arrivals(self):
        for _ in range(self.simulation.NUM_JOBS):
            self.simulation.event_queue.put(self.arrival_process.get_job())

    def schedule_job_completion(self,job):
        self.simulation.event_queue.put(self.completion_process.get_job_completion(job))

    def schedule_task(self, task, server):
        """Enqueue the task and return a task completion time"""
        try:
            task.get_job()
        except AttributeError:
            self.simulation.work.append(task)
        self.simulation.event_queue.put(
            server.enqueue_task(task)
        )

    def schedule_batch(self,batch):
        """Enqueue batches of tasks round robin scheduling"""
        server = self.cluster.get_servers()[self.counter]
        logging.debug(f'server {server.get_id()} is busy until time: {server.get_busy_until()}')
        self.counter = (self.counter+1)%self.cluster.get_num_servers()
        for task in batch:
            self.schedule_task(task, server)

    def complete_task(self, task):
        """Complete a task and dequeue from server"""
        try:
            task.set_finish_time(self.simulation.get_simulation_time())
            job = task.get_job()
        except AttributeError:
            pass
        except ValueError:
            pass
        else:
            if not job.is_finished() and False not in (task.is_finished() for task in job.get_tasks()):
                self.schedule_job_completion(job)
        for server in self.cluster.get_servers():
            server.complete_task(task)


    def schedule_job(self, job):
        """Schedule the tasks in the job"""
        self.simulation.work.append(job)
        tasks = job.get_tasks()

        if Scheduler.POLICY == 'RoundRobin':
            batch_size = 1
        elif Scheduler.POLICY == 'FullRepetition':
            batch_size = len(tasks)
        elif Scheduler.POLICY == 'LatinSquare':
            logging.debug(f'Latin Square order is {Scheduler.LATIN_SQUARE.shape[0]}')
            batch_size = Scheduler.LATIN_SQUARE.shape[0]

        work = [el.tolist() for el in array_split(tasks, len(tasks)/batch_size)]
        logging.debug(f'work batches to be scheduled are {work}')
        for batch in work:
            for i in range(batch_size):
                if Scheduler.POLICY == 'LatinSquare':
                    scheduled_order = [batch[Scheduler.LATIN_SQUARE[i][j]] for j in range(batch_size)]
                else:
                    scheduled_order = batch
                self.schedule_batch(scheduled_order)
                

    def complete_job(self, job):
        time = self.simulation.get_simulation_time()
        try:
            job.set_finish_time(self.simulation.get_simulation_time())
            logging.debug(f'setting job finishing time to {time} for job: {job.get_id()}')
        except ValueError:
            pass
