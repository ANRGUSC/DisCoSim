# DistCompSim
A simulator for evaluating a distributed computing scheduling

# Introduction
This simulator helps evaluating different possible mappings of the tasks of a task graph to available compute nodes. It evaluates any given mapping and provides the average latency for it along with the average task execution times for all the tasks.

# How to Simulate a Schedule?
Consider following example, where we have tasks {A, B, C, D} and compute nodes {0, 1, 2, 3, 4, 5}. The task graph dependency structure, the communication requirements of the tasks, their task compute profiles , the 

```python
# Mapping of tasks to compute nodes
mapping = {
    "A": [0, 1],
    "B": [2],
    "C": [3, 5],
    "D": [4]
}

# Data communication requirements between the tasks of the graph
sys_comm_req = {
    "A": {"B": 0.05, "C": 0.04},
    "B": {"D": 0.03},
    "C": {"D": 0.02},
    "D": dict()
}

# Time needed by a compute node ("key") to compute different tasks 
# Given by task profiler
sys_exec_profile = {
    0: {"A": 0.02, "B": 0.03, "C": 0.01, "D": 0.04},
    1: {"A": 0.02, "B": 0.01, "C": 0.02, "D": 0.02},
    2: {"A": 0.01, "B": 0.03, "C": 0.01, "D": 0.04},
    3: {"A": 0.02, "B": 0.02, "C": 0.03, "D": 0.02},
    4: {"A": 0.03, "B": 0.03, "C": 0.03, "D": 0.04},
    5: {"A": 0.04, "B": 0.02, "C": 0.04, "D": 0.01},
}

# Matrix of link rates r_ij from node "i" to node "j"
# Given by link profiler
sys_comm_profile = [
    [1, 2, 3, 4, 5, 6],
    [4, 5, 2, 1, 3, 1],
    [2, 4, 6, 1, 3, 5],
    [3, 5, 1, 2, 4, 6],
    [3, 4, 2, 1, 6, 5],
    [6, 5, 2, 1, 3, 4]
]

# Number of job arrivals per second
arrival_rate = 10

dis = Dispatcher(sys_exec_profile, sys_comm_profile, sys_comm_req, mapping, arrival_rate)

# Number of jobs to simulate
job_count = 10

# Simulation execution
avg_latency, avg_exec_times = dis.start_simulation(job_count)

print("Average latency of the schedule:", avg_latency)
print("Average execution time of the tasks:", avg_exec_times)
 ```

# References
[1] Diyi​ ​ Hu,​ ​ Pranav​ ​ Sakulkar,​ ​ Bhaskar​ ​ Krishnamachari, [“Greedy​ ​ Pipeline​ ​ Scheduling​ ​ for​ ​ Online​ ​ Dispersed​ ​ Computing“](http://anrg.usc.edu/www/papers/GreedyPipelineSchedulingForOnlineDispersedComputing_ANRGTechReport.pdf "ANRG Technical Report"), USC ANRG Technical Report, ANRG-2017-06. 

# Acknowledgement
This material is based upon work supported by Defense Advanced Research Projects Agency (DARPA) under Contract No. HR001117C0053. Any views, opinions, and/or findings expressed are those of the author(s) and should not be interpreted as representing the official views or policies of the Department of Defense or the U.S. Government.