import multiprocessing as mp
import os
import shutil
from collections import deque
import subprocess
import datetime
import time
import logging
from queue import Empty

class Dispatcher():
    """Dispatcher.

        The dispatcher's goal is to check the queue directory and load as many
        files as possible into its queue. It checks the queue directory every
        [wait] seconds and then checks the GPU for any free GPUs. It does this
        using the multiprocessing python module.

    """
    def __init__(self, run, queue, wait, spread, block, q):
        """
            run    :   run/execution directory
            dir    :   main directory where all the folders will be, based on
                       queue location
            wait   :   wait time in between checking for new scripts
            spread :   spread range to spread GPU's over (is an integer)
            block  :   list of GPUs to block (so we can use them for debugging)
        """

        self.run = run
        self.dir = os.path.dirname(queue)
        self.queue = queue
        self.wait = wait
        self.spread = spread
        self.block = block
        self.q = q

        dirs = ['completed', 'failed', 'queue']

        # build the directories if they don't exist already
        for d in dirs:
            d = os.path.join(self.dir, d)
            os.makedirs(d, exist_ok = True)

        self.p = mp.Process(target=self.dispatch, args=(self.q, ))

    def start(self):
        """
            Start the dispatcher.
        """
        self.p.start()
        return self.p.pid

    def stop(self, pid):
        """
            Stop the dispatcher. We shouldn't use terminate here.
        """
        try:
            psutil.Process(self.pid).kill()
            return True
        except NoSuchProcess:
            return False

    def get_files(self):
        """
            Get the list of files in the queue directory that are shell files
        """
        l = []
        with os.scandir(self.queue) as it:
            for f in it:
                if f.is_dir():
                    folder = os.path.join(self.queue, f.name)
                    for file in os.listdir(folder):
                        file = os.path.join(folder, file)
                        l.append(file) if file.endswith('.sh') else None
        return l

    def gpu(self):
        """
            Check for available GPUs using NVIDIA's PMON tool.
            Hopefully you have NVIDIA GPUs, otherwise this won't work.
        """
        # Nvidia-smi's process monitoring system with count 1
        sp = subprocess.Popen(['nvidia-smi', 'pmon', '-c', '1'],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        out_str = sp.communicate()
        out_list = list(filter(None, out_str[0].decode("utf-8").split('\n')))

        gpu_info = []
        # We skip the first two lines because it has the column names
        for line in out_list:
            exploded_info = [x for x in line.split(' ') if x != '']
            gpu_info.append(exploded_info)

        available_GPUs = []
        for gpu in gpu_info:
            gpu_id = gpu[0]
            gpu_pid = gpu[1]

            # this is what nvidia-smi pmon -c 1 uses to say that there's no process
            # thankfully, if nvidia-smi pmon -c 1 screws up, which it might
            # this will skip over the column identifiers
            if gpu_pid == '-':
                available_GPUs.append(gpu_id)

        if self.block:
            for b in self.block:
                if b in available_GPUs:
                    available_GPUs.remove(b)

        return available_GPUs

    '''
    def gpu(self):
        return ['0','1','2','3']
    '''

    def check_jobs(self, running):
        """ Void function that checks the global running jobs list
        and checks if any are completed and to remove them from the list.

        The function also does the necessary logging, and moving of files to the
        completed/failed job subdirectories as done before.
        """
        if running:
            for job_name in list(running.keys()):
                process = running[job_name]
                if process.poll() is not None:

                    logging.info('Found completed process... Checking '
                    'return code.')

                    if process.poll() == 0:
                        logging.info("%s returned %d", job_name,
                        process.poll())
                        self.mark(  q = self.q,
                                    job = job_name,
                                    pid = process.pid,
                                    status = 'completed')

                    else:
                        logging.warning("%s returned %d", job_name,
                        process.poll())
                        self.mark(  q = self.q,
                                    job = job_name,
                                    pid = process.pid,
                                    status = 'failed')

                    # Remove job from running jobs
                    del running[job_name]

    def mark(self, q, pid, job, status):
        # we get the filename from the job's path:
        fname = os.path.basename(job)
        # then we get the batch id from the job's path:
        # will be of the form /a/b/c/file.sh
        # so we do len(job) - len(fname) - 1 to get "batch-id"
        # then we do basename on that and remove "batch-" which is [6:]
        bid = os.path.basename(job[:len(job) - len(fname) - 1])[6:]
        # then we mark the process' status
        empty = True
        while empty:
            time.sleep(1)
            try:
                s = q.get(timeout = 10)
                s.batches[int(bid)].mark(fname, pid, status)
                q.put(s)

                # so we don't iterate again
                empty = False
            except Empty:
                empty = True


    def dispatch(self, q):
        # this is where most of the code is going to be
        # process is as follows:
        # check queue directory every [self.wait] time
        # check if there are any free GPUs
        # if so, move file and run that bad boy in [self.run]
        #
        # check for deleted (killed) files, do some logging, etc

        """
            The main chunk of code. Not for the end user to play with.
            Does necessary logging in `logfile.txt`, in [dir] directory.
        """

        logging.basicConfig(filename=os.path.join(self.dir, 'logfile.txt'),
                            format='%(asctime)s %(levelname)s: %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S',
                            level=logging.DEBUG)

        logging.info('------------ Job Dispatcher Started ------------')

        job_queue = deque()
        time_now = datetime.datetime.now()

        running_jobs = {}

        while True:

            time.sleep(self.wait)

            in_queue = self.get_files()

            if not in_queue:
                logging.info("Job queue empty. Waiting...")

                while not in_queue:
                    time.sleep(self.wait)
                    in_queue = self.get_files()

                    # Check for completed jobs in the mean time
                    self.check_jobs(running_jobs)

            available_GPUs = self.gpu()

            # Check if there are any available GPUs
            if not available_GPUs:
                logging.info("No Available GPUs. Waiting...")

                while not available_GPUs:
                    time.sleep(self.wait)
                    available_GPUs = self.gpu()

                    # Check for completed jobs in the mean time
                    self.check_jobs(running_jobs)

            # Do some checking on how many GPUs to release the process to
            next_GPU = ",".join(available_GPUs[:self.spread])

            logging.info("Next available GPU is #%s", next_GPU)

            # Check for new script files added to queue
            new_jobs = sorted(list(set(self.get_files()) -
                                  set(job_queue)))
            if new_jobs:
                job_queue.extend(new_jobs)
                logging.info("%d new jobs added to queue %s", len(new_jobs),
                             new_jobs.__repr__())

            # Check if any script files removed from queue
            jobs_removed = set(job_queue) - set(in_queue)
            if jobs_removed:
                logging.info("Detected %d jobs removed from queue",
                             len(jobs_removed))
                for job in jobs_removed:
                    job_queue.remove(job)

            current_job = job_queue.popleft()
            shutil.move(current_job, os.path.join(self.run, os.path.basename(current_job)))

            # Execute next script in queue
            logging.info("Running job %s on GPU #%s...", current_job, next_GPU)

            # Make the next available GPU visible to the server
            env_vars = {'CUDA_VISIBLE_DEVICES': next_GPU}

            os.environ.update(env_vars)

            try:
                p = subprocess.Popen([os.path.join(self.run, os.path.basename(current_job))])
                running_jobs[current_job] = p
                self.mark(  q = self.q,
                            job = current_job,
                            pid = p.pid,
                            status = 'running')
            except PermissionError as err:
                logging.warning("PermissionError: {0}".format(err))
                # Will get moved in upper code on next iteration
                self.mark(  q = self.q,
                            job = current_job,
                            pid = None,
                            status = 'failed')

                failed = os.path.join(self.dir, 'failed')
                fname = os.path.basename(current_job)
                shutil.move(os.path.join(self.run, fname),
                                        os.path.join(failed,fname))
