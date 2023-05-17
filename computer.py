import logging

from processes import CompletionProcess, ArrivalProcess


class Server(object):
    """Class to keep track of server status"""
    def __init__(self, simulation):
        self.simulation = simulation
        self.busy_until = simulation.get_simulation_time()
        self.task_queue = []
        self.completion_process = CompletionProcess(simulation)

    def get_busy_until(self):
        if self.busy_until <= self.simulation.get_simulation_time():
            return self.simulation.get_simulation_time()
        else:
            return self.busy_until         
    
    def set_busy_until(self,time):
        self.busy_until = time

    def get_current_task(self):
        if len(self.task_queue) > 0:
            return self.task_queue[0]
        else:
            return None
    
    def enqueue_task(self, task):
        self.task_queue.append(task)
        time,event = self.completion_process.get_task_completion(task)
        interrenewal_time = time - self.simulation.get_simulation_time()
        logging.debug(f'interrenewal time for task {task.get_id()} is {interrenewal_time}, executing on server {self.get_id()}')
        completion_time = time + (self.get_busy_until() - self.simulation.get_simulation_time())
        self.set_busy_until(completion_time)
        return (completion_time, event)


    def complete_task(self, task):
        """Task completion is idempotent"""
        try:
            self.task_queue.remove(task)
        except ValueError:
            pass
        else:
            try:
                job = task.get_job()
            except AttributeError:
                pass
            else:
                if False not in (task.is_finished() for task in job.get_tasks()):
                    self.simulation.scheduler.schedule_job_completion(job)

    def get_id(self):
        return id(self)


class Cluster(object):
    """A Collection of Servers"""
    NUM_SERVERS = 2
    def __init__(self,simulation):
        self.servers = [Server(simulation) for _ in range(Cluster.NUM_SERVERS)]
        logging.debug(f'servers have ids: {[server.get_id() for server in self.servers]}')

    def get_servers(self):
        return self.servers
    
    def get_num_servers(self):
        return Cluster.NUM_SERVERS


class Scheduler(object):
    POLICY = 'RoundRobin'
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

    def schedule_task(self, task):
        """Enqueue the task and return a task completion time"""
        server = self.cluster.get_servers()[self.counter]
        logging.debug(f'server {server.get_id()} is busy until time: {server.get_busy_until()}')
        self.counter = (self.counter+1) % self.cluster.get_num_servers()
        try:
            task.get_job()
        except AttributeError:
            self.simulation.work.append(task)
        self.simulation.event_queue.put(
            server.enqueue_task(task)
        )

    def complete_task(self, task):
        """Complete a task and dequeue from server"""
        task.set_finish_time(self.simulation.get_simulation_time())
        for server in self.cluster.get_servers():
            server.complete_task(task)


    def schedule_job(self, job):
        """Schedule the tasks in the job"""
        self.simulation.work.append(job)
        enqueued_servers = []
        for task in job.tasks:
            self.schedule_task(task)
            for server in self.cluster.get_servers():
                if task in server.task_queue:
                    enqueued_servers.append(server)

    def complete_job(self, job):
        job.set_finish_time(self.simulation.get_simulation_time())