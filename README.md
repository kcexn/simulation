# Simulation

## Usage:
After configuring the simulation parameters, the simulation can be run simply 
as a python script:

`python simulation.py`
## Configuration:
Simulation Parameters are kept in a file called configuration.ini in the working directory and contains the parameters:
[Simulation]
NUM_JOBS = 500
SIMULATION_TIME = 2500.0

[Computer]

[Computer.Cluster]
NUM_SERVERS = 10000

[Computer.Scheduler]
POLICY = LatinSquare

[Computer.Scheduler.LatinSquare]
LATIN_SQUARE_ORDER = 2
PREEMPTION = True
SERVER_SELECTION = RANDOM
NUM_PROBES_PER_BATCH = 1

[Computer.Scheduler.Sparrow]
NUM_SPARROW_PROBES = 1
PREEMPTION = True
LATE_BINDING = True
BATCH_SIZE = 2
SERVER_SELECTION = RANDOM

[Computer.Network]
MEAN_DELAY = 0.0000001
DELAY_STD = 0.0000000001

[Processes]

[Processes.Arrival]
INITIAL_TIME = 0.0
SCALE = 0.000111111111

[Processes.Arrival.Job]
POLICY = Erlang

[Processes.Completion]

[Processes.Completion.Task]
CORRELATED_TASKS = True
HOMOGENEOUS_TASKS = False

[Work]

[Work.Job]
NUM_TASKS = 100

### Configure Jobs and Tasks
- A task is a unit of work that is assigned to a server by the scheduler.
- A job is a collection of tasks.

The number of tasks that belong to a job is defined by `NUM_TASKS` 
in `work.Job`.

`NUM_TASKS` also controls the rate at which jobs will arrive (the shape parameter of the Erlang distribution).
### Configure the Computing Cluster
- A server is responsible for the servicing of tasks.
- Each server has a FIFO queue, and services tasks in the order that 
the scheduler gives them to the server.
- A cluster is a collection of servers.

The number of servers in the cluster is defined by `NUM_SERVERS` in 
`Computer.Cluster`.

There are three currently implemented scheduling policies:
- `RoundRobin`: Assign tasks round robin to all servers.
- `FullRepetition`: Assign all tasks in each job to every server in the order 
that they arrive in. Preempt any remaining tasks in the server queues after 
the associated job is completed, and move all subsequent tasks up in the server queues.
- `LatinSquare`: Assign tasks in each job according to the sequence defined by 
the latin square. Examples for latin squares of order 2, 3, and 6 are given. 
Higher orders can be constructed as the addition table of the finite group 
with `n` elements, where `n` is the order of the latin square.

The scheduling policy is defined by `POLICY` in `Computer.Scheduler`.

### Configure the Arrival and Completion Processes:
- The arrival process controls the rate at which jobs arrive.
- The completion process controls the rate at which servers complete tasks.

The arrival process yields interarrival times as an Erlang random variable 
with a `SCALE` parameter in `Process.Arrivals`.

The completion process yields completion times as a standard exponential.

### Configure the Simulation:
The simulation is controlled by two parameters:
- `NUM_JOBS`
- `SIMULATION_TIME`

`NUM_JOBS` defines the number of job arrivals to simulate.
`SIMULATION_TIME` defines the maximum simulation time to run for. The simulation 
will terminate if either:
- The event queue is empty
- the simulated time exceeds `SIMULATION_TIME`. 