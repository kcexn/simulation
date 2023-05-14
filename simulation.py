import logging
from queue import PriorityQueue

import processes

class Server(object):
    """Class to keep track of server status"""
    def __init__(self, simulation):
        self.simulation = simulation
        self.busy_until = simulation.get_simulation_time()
        self.task_queue = []
        self.completion_process = processes.CompletionProcess(simulation)

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

    def complete_task(self, task):
        """Task completion is idempotent"""
        try:
            self.task_queue.remove(task)
        except ValueError:
            pass

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
        self.counter = 0

    def schedule_task(self, task):
        """Enqueue the task and return a task completion time"""
        server = self.cluster.get_servers()[self.counter]
        logging.debug(f'server {server.get_id()} is busy until time: {server.get_busy_until()}')
        self.counter = (self.counter+1) % self.cluster.get_num_servers()
        server.enqueue_task(task)
        server.simulation.work.append(task)
        task_completion_time,event = server.completion_process.get_task_completion(task)
        if server.get_current_task() is not None:
            interrenewal_time = server.completion_process.get_interrenewal_time()
            logging.debug(f'server interrenewal time for task {task.get_id()} is {interrenewal_time}')
            task_completion_time = server.get_busy_until() + interrenewal_time
        server.set_busy_until(task_completion_time)
        self.simulation.event_queue.put(
            (task_completion_time, event)
        )
        self.simulation.work.append(task)

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
        task_completion_times = [server.get_busy_until() for server in enqueued_servers]
        job_completion_time = max(task_completion_times)
        self.simulation.event_queue.put(
            (job_completion_time, self.simulation.completion_process.get_job_completion(job))
        )

    def complete_job(self, job):
        job.set_finish_time(self.simulation.get_simulation_time())

class Simulation(object):
    INITIAL_TIME=0
    NUM_JOBS=1
    SIMULATION_TIME=100000
    def __init__(self):
        self.work = []
        self.time = Simulation.INITIAL_TIME
        self.arrival_process = processes.ArrivalProcess(self)
        self.completion_process = processes.CompletionProcess(self)
        self.event_queue = PriorityQueue()
        self.scheduler = Scheduler(self)

    def get_simulation_time(self):
        return self.time
    
    def set_simulation_time(self, time):
        self.time = time

    def generate_job_arrivals(self):
        for _ in range(Simulation.NUM_JOBS):
            self.event_queue.put(self.arrival_process.get_job())

    def run(self):
        logging.debug('running...')
        self.generate_job_arrivals()
        while not self.event_queue.empty():
            time,event = self.event_queue.get()
            if time > Simulation.SIMULATION_TIME:
                break
            self.set_simulation_time(time)
            event.resolve()
        logging.debug(f'total simulation time: {self.get_simulation_time()}')
            
            

if __name__ == "__main__":
    logging.basicConfig(level='DEBUG')
    simulation = Simulation()
    simulation.run()
