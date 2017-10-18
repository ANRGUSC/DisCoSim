from __future__ import division
import threading

import numpy as np
from instream import InStream
from node import Node
import time
import util

class Dispatcher(object):
    def __init__(self, sys_exec_profile, sys_comm_profile, sys_comm_req, mapping, arrival_rate):
        """
        sys_exec_profile: dictionary {node number: {task: exec time}}.
        sys_comm_profile: 2D matrix, element [i][j] specifies the comm cost between node i and j.
        sys_comm_req: dictionary {task: dict({next task: comm_req})}
        mapping: dictionary {task: [list of node numbers]}
        arrival_rate: input jobs arrival rate
        """
        self.mapping = mapping
        self.sys_exec_profile = sys_exec_profile
        self.sys_comm_profile = sys_comm_profile
        self.sys_comm_req = sys_comm_req

        self.node_list = [] # NOTE: Nodes are not ordered with respect to their node_num
        self.init_nodes = [] # task nodes (numbers) with inputs
        self.end_nodes = [] # Nodes with final task

        # Construct and initialize nodes and instream destinations
        self.generate_nodes()

        util.echo("Node list indexes {}".format([node.node_num for node in self.node_list]))
        util.echo("Initial task nodes: {}".format(self.init_nodes))
        util.echo("End task nodes: {}".format([node.node_num for node in self.end_nodes]))

        self.instream = InStream(arrival_rate) # Input job generator
        self.set_input_nodes()

    def set_input_nodes(self):
        # Set the destinations for input stream => Init task nodes
        for node_idx_list in self.init_nodes:
            node_list = []
            for node_num in node_idx_list:
                node_list.append(self.node_list[node_num])
            self.instream.add_dests(node_list)

    def generate_nodes(self):
        """
        Generate parents dictionary of lists
        parents: {task: [list of parent tasks]}
        """
        self.parents = {}
        #import pdb;pdb.set_trace()
        for task in self.mapping:
            self.parents[task] = []
            for pre_task in self.sys_comm_req:
                if self.sys_comm_req[pre_task] and task in self.sys_comm_req[pre_task]:
                    # print("{} is a pre-task of task {}".format(pre_task, task))
                    self.parents[task].append(pre_task)

            # Identify the tasks where input queue is required
            if not self.parents[task]:
                self.parents[task].append("Input")
                self.init_nodes.append(self.mapping[task])

        ### e.g. parents = {A: [], B: [A], C:[A], D:[B,C]}
        # print("Parents dict:", self.parents)

        # Generate nodes and append in node list
        for node_num in self.sys_exec_profile.keys():
            for task in self.mapping:
                if node_num in self.mapping[task]:
                    # Nodes requires: (node_num, task, comm_req, exec_times, sys_comm_profile, parent_list)                
                    self.node_list.append(Node(node_num, task, self.sys_comm_req[task], # if task in self.sys_comm_req else []
                        self.sys_exec_profile[node_num], self.sys_comm_profile, self.parents[task]))
                    break

        # Set destinations when all nodes are generated
        for node in self.node_list:
            for next_task in self.sys_comm_req[node.task]:
                task_nodes = []
                for node_num in self.mapping[next_task]:
                    assigned_node_num_l = np.array([n.node_num for n in self.node_list])
                    node_idx = int(np.where(assigned_node_num_l==node_num)[0])
                    task_nodes.append(self.node_list[node_idx])#node_num])
                node.add_dests(task_nodes)

            if not node.dest:
                # Destination list is empty => end node
                self.end_nodes.append(node)

        return

    def start_simulation(self, total_jobs):
        self.total_jobs = total_jobs
        util.echo("Starting simulation with {} jobs".format(self.total_jobs))

        for node in self.node_list:
            util.echo("Starting node {}".format(node.node_num))
            # Start each node on a separate thread
            t = threading.Thread(target=node.run)
            t.daemon = True
            t.start()

        # Start generating inputs
        t = threading.Thread(target=self.instream.generate_inputs, args=(self.total_jobs*2,))   ### this is to avoid some corner effects, where the end task runs short of 1 parent task
        t.daemon = True
        t.start()

        while True:
            if self.check_end_status(): # Check if all jobs executed
                util.echo("=== Ending the simulation ===")
                return self.end_simulation()

    def end_simulation(self):
        # Turn off all the nodes
        for node in self.node_list:
            # This causes the function thread to die
            node.set_status(False)

        # Calculate the average times for each task
        self.avg_times = dict()
        for task in self.mapping:
            total_exec_time = 0
            total_jobs = 0
            for node_num in self.mapping[task]:
                assigned_node_num_l = np.array([n.node_num for n in self.node_list])
                node_idx = int(np.where(assigned_node_num_l==node_num)[0])
                total_exec_time = max(total_exec_time, self.node_list[node_idx].total_exec_time) #node_num].total_exec_time
                total_jobs += self.node_list[node_idx].num_finished_tasks#node_num].num_finished_tasks
            self.avg_times[task] = total_exec_time / total_jobs

        # Average finish time over all tasks
        end_times = []
        for node in self.end_nodes:
            end_times += node.finish_times

        sum_time = 0
        for start, end in zip(self.instream.start_times, end_times):
            sum_time += (end - start).total_seconds()
        self.avg_finish_time = sum_time / total_jobs

        return (self.avg_finish_time, self.avg_times)

    def check_end_status(self):
        # Check to see if the total number of executed jobs in the final nodes sums to the total jobs
        completed_jobs = 0
        for node in self.end_nodes:
            completed_jobs += node.num_finished_tasks
        ### some problems with duplicated end nodes
        return completed_jobs == self.total_jobs


if __name__ == '__main__':

    mapping = {
        "A": [0, 1],
        "B": [2],
        "C": [3, 5],
        "D": [4]
    }

    sys_comm_req = {
        "A": {"B": 0.05, "C": 0.04},
        "B": {"D": 0.03},
        "C": {"D": 0.02},
        "D": dict()
    }

    # Given by task profiler
    sys_exec_profile = {
        0: {"A": 0.02, "B": 0.03, "C": 0.01, "D": 0.04},
        1: {"A": 0.02, "B": 0.01, "C": 0.02, "D": 0.02},
        2: {"A": 0.01, "B": 0.03, "C": 0.01, "D": 0.04},
        3: {"A": 0.02, "B": 0.02, "C": 0.03, "D": 0.02},
        4: {"A": 0.03, "B": 0.03, "C": 0.03, "D": 0.04},
        5: {"A": 0.04, "B": 0.02, "C": 0.04, "D": 0.01},
    }

    # Given by link profiler
    sys_comm_profile = [
        [1, 2, 3, 4, 5, 6],
        [4, 5, 2, 1, 3, 1],
        [2, 4, 6, 1, 3, 5],
        [3, 5, 1, 2, 4, 6],
        [3, 4, 2, 1, 6, 5],
        [6, 5, 2, 1, 3, 4]
    ]

    arrival_rate = 10
    dis = Dispatcher(sys_exec_profile, sys_comm_profile, sys_comm_req, mapping, arrival_rate)

    job_count = 10
    avg_latency, avg_exec_times = dis.start_simulation(job_count)
    print("Average latency of the schedule:", avg_latency)
    print("Average execution time of the tasks:", avg_exec_times)
    time.sleep(1)