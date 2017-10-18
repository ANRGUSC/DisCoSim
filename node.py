from __future__ import division

import time
import threading
from datetime import datetime
import numpy as np
import util

class Node(object):
    """docstring for Node"""
    def __init__(self, node_num, task, comm_req, sys_exec_profile, sys_comm_profile, parent_list):
        """
        node_num: index of the node.
        task: task name (ex. "A", "B" etc.)
        comm_req: dictionary {next task: data tx reqd}
        exec_times: dictionary {task: execution time}
        sys_comm_profile: 2D matrix, element [i][j] specifies the comm cost between node i and j.
        parent_list: [list of parent tasks]
        """
        self.node_num = node_num
        self.task = task
        self.dest = []
        self.parent_list = parent_list

        self.queues = dict()
        # queues: {"A": (4, Lock()), "B": (0, Lock())}
        for task in parent_list:
            self.queues[task] = [0, threading.Lock()]

        util.echo("Generating node {} with task {}".format(self.node_num, self.task))
        self.exec_times = sys_exec_profile[self.task]
        
        self.sys_comm_profile = sys_comm_profile[self.node_num]

        self.comm_req = comm_req

        # Store the finish times for the jobs #TODO
        self.finish_times = []
        self.num_finished_tasks = 0
        self.total_exec_time = 0

        self.set_status(True)

    def set_status(self, on_status):
        # Node accepting jobs or not
        self.on_status = on_status

    def add_dests(self, dest):
        """
        dest: multiple dest nodes executing the SAME task
        e.g., if you hv dest nodes of [n1,n2] executing task1, [n3] executing task2,
        then you should invoke this function as:
            self.add_dests([n1,n1])
            self.add_dests([n3])
        """
        self.dest.append(dest)

    def __str__(self):
        _queueStr_ = lambda q: '\t'.join(['{}:{}'.format(k,v) for k,v in q.items()])
        s = ('{:>20s}: {}\n'
             '{:>20s}: {}\n'
             '{:>20s}: {}\n'
             '{:>20s}: {}\n'
             '{:>20s}: {}\n'
             '{:>20s}: {}\n'
             '{:>20s}: {}\n'
             ).format(  'task',self.task,
                        'dest',', '.join([d.task for dl in self.dest for d in dl]),
                        'parent list',', '.join([p.task for p in self.parent_list]),
                        'queue',_queueStr_(self.queues),
                        'time',self.time,
                        'node num',self.node_num,
                        '# finished task',self.num_finished_tasks)
        return s

    def run(self):
        while self.on_status: # on: True, off: False
            if self.execute():
                t = threading.Thread(target=self.send_outputs, args=(self.num_finished_tasks,))
                t.daemon = True
                t.start()
        ### Solve the atomic print issue.
        util.echo("** Turning off node {} **".format(self.node_num)) # Why does this not get printed????

    def execute(self):
        for key in self.queues:
            if self.queues[key][0] == 0:
                return False

        start_time = datetime.now()
        self.decrease()
        # print("Execution starting")
        time.sleep(self.exec_times)
        self.num_finished_tasks += 1 
        # util.echo("node {} finishes {} tasks.".format(self.node_num, self.num_finished_tasks))
        end_time = datetime.now()

        self.total_exec_time += (end_time - start_time).total_seconds()
        self.finish_times.append(end_time)

        return True
        
    def send_outputs(self, num_finished_tasks):
        if not self.dest:
            return

        successors = np.array([])
        comm_times = np.array([])
        for destination in self.dest:
            partition = len(destination)
            dest_idx = (num_finished_tasks - 1) % partition
            succ = destination[dest_idx]
            successors = np.concatenate((successors, [succ]))

            comm_times = np.concatenate((comm_times, [self.comm_req[succ.task] / self.sys_comm_profile[succ.node_num]]))

        ### NOTE: the node sequence should start from 0.

        # sort the communication times and return indexes
        sorted_idx = np.argsort(comm_times)
        successors_sorted = successors[sorted_idx]
        comm_times_sorted = comm_times[sorted_idx]
        # get the delta time of communication times
        comm_time_delta = np.diff(comm_times_sorted)

        comm_time_delta = np.concatenate(([comm_times_sorted[0]], comm_time_delta))
        
        for i in range(len(comm_time_delta)):
            time.sleep(comm_time_delta[i])
            successors_sorted[i].increase(self.task)

    def increase(self, task):
        with self.queues[task][1]:
            # increment the queue size for task
            self.queues[task][0] += 1
        #print('succ of {} is {}, queue size {}'.format(self.task,succ.task,succ.queues))

    def decrease(self):
        for task in self.parent_list:
            with self.queues[task][1]:
                self.queues[task][0] -= 1