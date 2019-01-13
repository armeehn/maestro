import json
import psutil
import os
import time

class Process():
    """
        docstring for Process.

        this class is the base class that will be used
        its aim is to give structure to the processes

        I've learned too much OCaml, alas it seems to be simpler in that
        language but we'll do it in python.

        pid: process id (given when it is running)
        filename: file to be run
        log_dir: log directory (if applicable)
        status: 'completed' | 'killed' | 'failed' | 'queued' | 'running'

    """
    def __init__(self, pid, filename, log_dir, status):
        #super(Process, self).__init__()
        self.pid = pid
        self.filename = filename
        self.log_dir = log_dir
        self.status = status

    def __repr__(self):
        info_dict = {
            'pid': self.pid,
            'filename': self.filename,
            'log_dir': self.log_dir,
            'status': self.status
        }
        return info_dict.__repr__()

    def kill(self):
        if self.status == 'running' and self.pid is not None:
            try:
                psutil.Process(self.pid).kill()
                self.status = 'killed'

            except psutil.NoSuchProcess:
                print('whoa there, something is off... '
                            'maybe it\'s actually completed?')
                time.sleep(20) # inner delay
                if self.status == 'killed':
                    print('Process was already killed, aborting...')
                elif self.status != 'completed':
                    print('k, something is off...')
        elif self.status == 'queued':
            self.status = 'killed'
        else:
            # Trying to kill already killed or completed process
            pass

class Batch():
    """
        I don't know how to write docstrings yet so we'll leave
        this as is for now. But, what I can do is give a general description of what this file will do:

        this is a class that will be a list of either running processes or files to be run that are queued
    """

    def __init__(self, label, id, processes, options = None):
        #super(, self).__init__()
        self.label = label
        self.id = id
        self.processes = processes
        self.options = options # don't know what to do with this yet

    def __repr__(self):
        info_dict = {
            'id': self.id,
            'label': self.label,
            'processes': self.processes,
            'options': self.options
        }
        return info_dict.__repr__()

    def kill(self):
        """
            kills a batch's processes without hesitation
            * blows smoke from gun *
        """
        for p in self.processes:
            p.kill()

    def get_all_id(self):
        return [p.pid for p in self.processes]

    def mark(self, fname, pid, status):
        for p in self.processes:
            if p.filename == fname:
                p.status = status
                p.pid = pid

class State():
    """
        docstring for State.

        The State class will be for containing the state of the experiment
        manager at any point in time, we will then pickle it upon save and have
        a daemon update it every so often (perhaps an argument when running the
        manager? something to think about...)

    """
    #### initialization ####

    def __init__(self, batches = {}):
        #super(State, self).__init__()
        self.batches = self.to_dict(batches)
        print(self.batches)

    #### methods ####

    #### representation stuff ####

    def to_dict(self, batches):
        IDs = [batch.id for batch in batches]
        return dict(zip(IDs, batches))

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
            sort_keys=False, indent=4)
