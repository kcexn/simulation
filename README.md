# Simulation

## Usage:
After configuring the simulation parameters, the simulation can be run simply 
as a python script:

`
    python simulation.py
`
## Configuration:
### Configure Jobs and Tasks
- A task is a unit of work that is assigned to a server by the scheduler.
- A job is a collection of tasks.

The number of tasks that belong to a job is defined by `NUM_TASKS` 
in `work.Job`.
### Configure the Computing Cluster
- A server is responsible for the servicing of tasks.
- Each server has a FIFO queue, and services tasks in the order that 
the scheduler gives them to the server.
- A cluster is a collection of servers.

The number of servers in the cluster is defined by `NUM_SERVERS` in 
`computer.Cluster`.

There are three currently implemented scheduling policies:
- `RoundRobin`: Assign tasks round robin to all servers.
- `FullRepetition`: Assign all tasks in each job to every server in the order 
that they arrive in. Preempt any remaining tasks in the server queues after 
the associated job is completed, and move all subsequent tasks up in the server queues.
- `LatinSquare`: Assign tasks in each job according to the sequence defined by 
the latin square. Examples for latin squares of order 2, 3, and 6 are given. 
Higher orders can be constructed as the addition table of the finite group 
with `n` elements, where `n` is the order of the latin square.

The scheduling policy is defined by `POLICY` in `computer.Scheduler`.

### Configure the Arrival and Completion Processes:
- The arrival process controls the rate at which jobs arrive.
- The completion process controls the rate at which servers complete tasks.

The arrival process yields interarrival times as a gamma random variable 
with a shape-scale parameterization in 
`processes.ArrivalProcess.interrenewal_times`

The completion process yields completion times as an exponential random variable 
with a scale parameterization in `processes.Process.interrenewal_times`.

### Configure the Simulation:
The simulation is controlled by two parameters:
- `NUM_JOBS`
- `SIMULATION_TIME`

`NUM_JOBS` defines the number of job arrivals to simulate.
`SIMULATION_TIME` defines the maximum simulation time to run for. The simulation 
will terminate if either:
- The event queue is empty
- the simulated time exceeds `SIMULATION_TIME`. 



