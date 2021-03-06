# maestro 1.0

#### important stuff ####

from __future__ import print_function, unicode_literals
try:
    from base import base, fscompleter, dispatcher
except ImportError:
    print('It seems that you don\'t have the base files'
                                ' installed, look into that first...')

#### stuff for the interface ####

import questionary
import prompt_toolkit

#### general imports ####

from pprint import pprint
import json
import subprocess
import argparse
import os
import pickle as pkl
import glob
import shutil
import psutil
import multiprocessing as mp
import time
from queue import Empty

#### cool welcome message producer ####

from pyfiglet import Figlet

#### general usage things ####

main_menu = ['view','load','start/stop','clear','kill','exit']

commands = [    'Overview of the job history',
                'Load files into the dispatcher',
                'Start/Stop the dispatcher',
                'Kill batch or specific process',
                'Clear current screen',
                'Exit the experiment manager'
            ]

dash = '- ' * 40 # for viewing batches


#### parse arguments with argparse ####

parser = argparse.ArgumentParser('Python script to automate the execution of '
                                 'shell scripts')

parser.add_argument('--no-welcome', action='store_true',
                    default=False,
                    help='Display welcome message on load. Default: False.')

parser.add_argument('--delay', type=int, default=60, metavar='DELAY',
                    help='Delay (time) in checking the state object. Default: 60 seconds.')

args = parser.parse_args()

#### welcome message ####
if not args.no_welcome:
    figlet = Figlet()
    print(figlet.renderText('maestro'))
    print('Written by Alex Iannantuono')

#### housekeeping ####

print('Welcome to maestro: the experiment manager.')
print('Loading settings (if available)...')

try:
    f = open('maestro_sys/settings.pkl', 'rb')
    settings = pkl.load(f)
except FileNotFoundError:
    settings = {}

    print('Couldn\'t find any settings file so we\'re going to ask a couple questions...')
    print('For the following questions, you can use "~" to expand your home directory...\n')

    queue_dir = prompt_toolkit.prompt('Enter the path for your queue directory: ',
                            completer=fscompleter.PathCompleter(),
                                complete_while_typing=True)
    run_dir = prompt_toolkit.prompt('Enter the path for your run directory: ',
                            completer=fscompleter.PathCompleter(),
                                complete_while_typing=True)
    run_dir = os.path.expanduser(run_dir)
    queue_dir = os.path.expanduser(queue_dir)
    settings['queue_dir'] = os.path.join(queue_dir, 'queue')
    settings['run_dir'] = run_dir

    try:
        os.mkdir('maestro_sys')
    except FileExistsError:
        pass

    # add additional settings if need be
    with open('maestro_sys/settings.pkl', 'wb') as settings_file:
        pkl.dump(settings, settings_file)

# check if there's a state file that exists, otherwise initialize to nothing
print('Setting up maestro state...')
try:
    f = open('maestro_sys/manager_state.pkl', 'rb')
    state = pkl.load(f)
except FileNotFoundError:
    try:
        os.mkdir('maestro_sys')
    except FileExistsError:
        state = base.State()


finally:
    q = mp.Queue()
    q.put(state)


#### define the daemon ####
def save(q):
    while True:
        time.sleep(5)
        empty = True
        while empty:
            try:
                s = q.get(timeout = 10)
                q.put(s)
                empty = False
                with open('maestro_sys/manager_state.pkl', 'wb') as state_file:
                    pkl.dump(s, state_file)
            except Empty:
                empty = True


#### start the daemon ####

saver = mp.Process(target=save, args=(q,))
saver.start()

#### main loop ####
while True:
    try:
        print('Options:', *main_menu)
        answer = questionary.select('What do you want to do?',
                                            choices=commands).ask()

        if answer == 'Exit the experiment manager':
            try:
                pid = settings['dispatcher']
                alive = psutil.pid_exists(pid)
                if alive:
                    print('Kill the dispatcher first using built-in stopper.')
                else:
                    raise SystemExit
            except KeyError:
                raise SystemExit

        elif answer == 'Clear current screen':
            os.system('clear')

        elif answer == 'Overview of the job history':
            state = q.get()
            if state.batches == {}:
                print('Job history is empty.')

            else:
                for bid in state.batches.keys():
                    batch = state.batches[bid]
                    header = ('BATCH ID: %g \t\t LABEL: %s' % (bid, batch.label))
                    print(header)
                    print(dash)
                    print('PROCESSES:')
                    print('{:<10s}{:<36s}{:<20s}{:<10s}'.format('pid', 'filename', 'log', 'status'))
                    print(dash)
                    for proc in batch.processes:
                        dic = proc.__dict__
                        dic['filename'] = os.path.basename(dic['filename'])
                        replace = lambda x: '~' if x is None else str(x)
                        print('{:<10s}{:<36s}{:<20s}{:<10s}'.format(*[replace(x) for x in dic.values()]))
                    print('')

                # ask if you want to delete records...
                print('Do you want to delete some history of records?')
                answer = questionary.confirm('Are you sure?').ask()
                if answer:
                    to_delete = input('Enter the batch IDs to delete,'
                                                ' separated by spaces: ').split(' ')
                    for d in to_delete:
                        if not d.isdigit() and d not in state.batches.keys():
                            print('Attempted to delete non-existent batch, skipping.')
                        else:
                            del state.batches[int(d)]

                            try:
                                shutil.rmtree(os.path.join(settings['queue_dir'],
                                                                    'batch-' + d))
                            except FileNotFoundError:
                                pass

            q.put(state)

        elif answer == 'Load files into the dispatcher':
            state = q.get()
            files = prompt_toolkit.prompt('Enter files you wish to load (* wildcard allowed): ',
                completer=fscompleter.PathCompleter(),
                        complete_while_typing=True)

            files = os.path.expanduser(files)
            globbed = glob.glob(files)

            if len(globbed) > 0:
                print('You\'ve loaded %g file(s) '
                            'into the manager.' % len(globbed))

                answer = questionary.confirm('Are you sure?').ask()
                if answer: #if yes
                    label = input('Please type in a label for this batch: ')
                    try:
                        k = list(state.batches.keys())
                        id = state.batches[k[-1]].id + 1
                    except IndexError:
                        id = 0
                    processes = []
                    for g in globbed:
                        proc = base.Process(
                            pid = None,
                            filename = g,
                            log_dir = None,
                            status = 'queued'
                        )
                        processes.append(proc)
                    new_batch = base.Batch(
                        id = id,
                        label = label,
                        processes = processes,
                        options = None
                    )
                    state.batches[new_batch.id] = new_batch
                    # move files to batch queue folder...
                    batch_folder = os.path.join(settings['queue_dir'],
                                'batch-' + str(new_batch.id))
                    os.makedirs(batch_folder)

                    for file in globbed:
                        shutil.copy(file, os.path.join(batch_folder, os.path.basename(file)))

            q.put(state)
        elif answer == 'Start/Stop the dispatcher':
            # start the dispatcher
            print('Some questions before we start the dispatcher...')
            menu = [    'Start Dispatcher',
                        'Stop Dispatcher',
                        'Back to Main Menu'
                        ]
            answer = questionary.select('What do you want to do?', choices = menu).ask()

            if answer == 'Back to Main Menu':
                continue

            elif answer == 'Start Dispatcher':
                # check if there is already a dispatcher running
                try:
                    pid = settings['dispatcher']
                    alive = psutil.pid_exists(pid)
                    if alive:
                        print('There\'s already a dispatcher running.')
                        start = False

                    else:
                        start = True

                except KeyError:
                    print('No dispatcher detected, setting up now.')
                    start = True

                if start:
                    # initiate start sequence
                    block = []
                    spread = 1
                    wait = 60

                    inp = input('Which GPUs would you like to block? ')
                    inp = None if inp in ['None', 'none', 'NONE'] else inp
                    if inp is not None:
                        while(not isinstance(eval(inp), list)):
                            inp = input('Please enter a list: ')
                        block = eval(inp)

                    inp = input('How many GPUs would you like to spread this over? ')
                    # if it isnt a digit second branch won't get triggered
                    while(not inp.isdigit() or int(inp) < 1):
                        inp = input('Please enter an integer greater'
                                                            ' or equal to 1: ')
                    spread = int(inp)

                    inp = input('Enter a wait time between checking'
                                                    ' for new scripts? ')

                    while(not inp.isdigit() or int(inp) < 30):
                        inp = input('Please enter an integer greater'
                                                            ' or equal to 30: ')
                    wait = int(inp)

                    d = dispatcher.Dispatcher(
                            run = settings['run_dir'],
                            queue = settings['queue_dir'],
                            wait = wait,
                            spread = spread,
                            block = block,
                            q = q)

                    pid = d.start()
                    settings['dispatcher'] = pid
                    print('Dispatcher has been started.')

            elif answer == 'Stop Dispatcher':
                try:
                    pid = settings['dispatcher']
                    # settings['dispatcher'] contains the PID
                    try:
                        psutil.Process(pid).kill()
                        del settings['dispatcher']
                    except psutil.NoSuchProcess:
                        print('No dispatcher detected. Try starting one.')
                except KeyError:
                    print('No dispatcher detected. Try starting one.')
            else:
                print('What else is there to do?')

        elif answer == 'Kill batch or specific process':
            state = q.get()

            menu = [    'Kill certain process',
                        'Kill all processes in a batch',
                        'Go back to main menu'
                ]

            answer = questionary.select('What do you want to do?', choices = menu).ask()

            if answer == 'Kill certain process':
                answer = input('Input the ID of the process'
                                                    ' you wish to cancel: ')
                kv_id = {}
                for k in state.batches.keys():
                    kv_id[k] = state.batches[k].get_all_id()

                # a neat way to flatten a list of lists
                all = [item for sublist in kv_id.values() for item in sublist]
                while(not answer.isdigit() and answer not in all):
                    answer = input('Please try again: ')

                answer = int(answer)
                # kill the process (must perform a search)
                for b in state.batches.values():
                    if answer in b.get_all_id():
                        for p in b.processes:
                            if answer == p.pid:
                                print('Killing Process %g' % p.pid)
                                p.kill()
                                break
                        break

            elif answer == 'Kill all processes in a batch':
                answer = input('Input the ID of the batch you wish to cancel: ')
                while(not answer.isdigit() or int(answer) < 0 or \
                    int(answer) not in state.batches.keys()):
                        answer = input('Please try again: ')

                print('Killing batch ID %g' % int(answer))
                dir = os.path.join(settings['queue_dir'], 'batch-' + answer)
                state.batches[int(answer)].kill()
                try:
                    shutil.rmtree(dir)
                except FileNotFoundError:
                    print('Trying to kill batch that already was killed.')


            elif answer == 'Go back to main menu':
                continue

            else:
                print('Something has gone terribly wrong in the logic...')

            q.put(state)

        else:
            print('Something has gone terribly wrong in the logic...')

    except (KeyboardInterrupt, SystemExit):
        print('')
        print('Exiting and saving current state...')
        # save state on exit
        with open('maestro_sys/manager_state.pkl', 'wb') as state_file:
            pkl.dump(state, state_file)
        with open('maestro_sys/settings.pkl', 'wb') as settings_file:
            pkl.dump(settings, settings_file)

        saver.terminate()
        break
