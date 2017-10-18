import threading
import time
from datetime import datetime
import util

class InStream(object):
    def __init__(self, arrival_rate):
        """
        arrival_rate: number of jobs per second
        node_list: list of nodes
        """
        self.arrival_diff = 1.0 / arrival_rate
        self.dest = []
        self.start_times = []

    def add_dests(self, dest):
        """
        dest: multiple dest nodes executing the SAME task
        e.g., if you hv dest nodes of [n1,n2] executing task1, [n3] executing task2,
        then you should invoke this function as:
            self.add_dests([n1,n1])
            self.add_dests([n3])
        """
        self.dest.append(dest)

    def generate_inputs(self, total_jobs):
        """
        total_jobs: total number of jobs to simulate
        """
        self.total_jobs = total_jobs
        self.curr_jobs = 0

        if not self.dest:
            util.echo("No destinations for the input stream")
            return

        while self.curr_jobs < self.total_jobs:
            # util.echo(">>> Generating job {}".format(self.curr_jobs))
            for destination in self.dest:
                partition = len(destination)
                dest_idx = (self.curr_jobs) % partition
                node = destination[dest_idx]
                node.increase("Input")

            self.curr_jobs += 1
            self.start_times.append(datetime.now())
            time.sleep(self.arrival_diff)

        return