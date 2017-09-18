#!/usr/bin/env python

'''
Wraps the execution of a command.  What it does:
- Enables you to claim resources (e.g., GPU memory).
- Kills the job if the resources are exceeded.
- Saves information about the job as it runs to a JSON file (e.g., maximum GPU usage).

Usage:

    # Prints help.
    stake.py -h

    # Prints information about the entire machine.
    stake.py

    # Grab 5 GB of GPU memory to run the command 'python main.py'.
    # Outputs run statistics to run-stats.json.
    stake.py -g 5g -s run-stats.json python main.py

@author Percy Liang
'''
from __future__ import print_function
from builtins import str
from builtins import map
from builtins import range

import argparse
import json
import os
import random
import re
import socket
import string
import subprocess
import sys
import shlex
import signal
import time

def log(s):
    print('[stake %s] %s' % (time.strftime('%Y-%m-%d %H:%M:%S'), s), file=sys.stderr)

def parse_size(s):
    """
    s: <number>[<k|m|g|t>]
    Returns the number of bytes.
    """
    if s[-1].isdigit():
        return float(s)
    n, unit = float(s[0:-1]), s[-1].lower()
    if unit == 'k':
        return n * 1024
    if unit == 'm':
        return n * 1024 * 1024
    if unit == 'g':
        return n * 1024 * 1024 * 1024
    if unit == 't':
        return n * 1024 * 1024 * 1024 * 1024
    raise ValueError('Invalid size: %s, expected <number>[<k|m|g|t>]' % s)

def size_str(size):
    """
    size: number of bytes
    Return a human-readable string.
    """
    if size is None:
        return None

    if size < 0:
        return '-' + size_str(-size)

    for unit in ('', 'k', 'm', 'g', 't'):
        if size < 100 and size != int(size):
            return '%.1f%s' % (size, unit)
        if size < 1024:
            return '%d%s' % (size, unit)
        size /= 1024.0

def get_gpu_info():
    '''
    Note: nvidia-smi output:
    | Fan  Temp  Perf  Pwr:Usage/Cap|         Memory-Usage | GPU-Util  Compute M. |
    |===============================+======================+======================|
    |   0  GeForce GTX TIT...  Off  | 0000:04:00.0     Off |                  N/A |
    | 22%   26C    P8    16W / 250W |      2MiB / 12206MiB |      0%      Default |
    ...
    | Processes:                                                       GPU Memory |
    |  GPU       PID  Type  Process name                               Usage      |
    |=============================================================================|
    |    7     29283    C   python                                         704MiB |
    '''

    '''
    Return JSON:
    {
        <gpu_num>: {
            'free_gpu_mem': ...,
            'total_gpu_mem': ...,
            'processes': [{'pid': ..., 'command': ..., 'gpu_mem': ...}, ...]
        }
    }
    '''
    def convert_to_bytes(s):
        assert s.endswith('MiB')
        return int(s[:-3]) * 1024 * 1024

    lines = subprocess.check_output('nvidia-smi').decode('utf-8').split('\n')
    curr_gpu_num = None
    in_processes_section = False
    info = {}
    for line in lines:
        if line.startswith('| Processes:'):
            in_processes_section = True
            continue

        if not in_processes_section:
            # Set current GPU
            m = re.search(r'^\|\s+(\d+) ', line)
            if m:
                curr_gpu_num = int(m.group(1))
                info[curr_gpu_num] = {}
                continue
            # Find match
            m = re.search(r'(\d+MiB) / (\d+MiB)', line)
            if m:
                info[curr_gpu_num]['free_gpu_mem'] = convert_to_bytes(m.group(1))
                info[curr_gpu_num]['total_gpu_mem'] = convert_to_bytes(m.group(2))
                continue
        else:
            if re.search(r'^\|\s+(\d+)', line):
                args = re.split('\s+', line)
                gpu_num = int(args[1])
                pid = int(args[2])
                command = ' '.join(args[4:-2])
                mem = convert_to_bytes(args[-2])
                info[gpu_num].setdefault('processes', []).append({'pid': pid, 'command': command, 'gpu_mem': mem})

        #print line
    return info

############################################################

def read_stake_info():
    if not os.path.exists(stake_path):
        return {}
    return json.load(open(stake_path))

def write_stake_info(stake_info):
    with open(stake_path, 'w') as f:
        print(json.dumps(stake_info), file=f)

def claim_exists(claim):
    pid = claim.get('pid')
    return pid is not None and os.path.exists('/proc/' + str(pid))

def read_update_stake_info():
    stake_info = read_stake_info()

    # Update GPU info
    stake_info['gpu_info'] = get_gpu_info()

    # Delete claims which are no longer there
    stake_info['claims'] = [claim for claim in stake_info.get('claims', []) if claim_exists(claim)]

    write_stake_info(stake_info)
    return stake_info

def generate_claim_id():
    return ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(16))

def is_pid_under(parent_pid, child_pid):
    # Walk up the process tree
    while child_pid != 1 and child_pid != parent_pid:
        child_pid = int(os.popen("ps -p %d -oppid=" % child_pid).read().strip())
    return parent_pid == child_pid

def join_process_claims(stake_info, gpu_num):
    '''
    Return a list of {'claim': ..., 'process': ...} structures
    by joining on the PID.
    '''
    claims = [claim for claim in stake_info.get('claims', []) if claim['gpu_num'] == gpu_num]
    processes = stake_info['gpu_info'][gpu_num].get('processes', [])
    result = []
    claimed_processes = []
    for claim in claims:
        info = {'claim': claim}
        if 'pid' not in claim:
            continue
        for process in processes:
            if is_pid_under(claim['pid'], process['pid']):
                info['process'] = process
                claimed_processes.append(process)
                break
        result.append(info)

    for process in processes:
        if processes in claimed_processes:
            continue
        info = {'process': process}
        result.append(info)
    return result

def available_gpu_mem(stake_info, gpu_num):
    '''
    available memory means not used by a process or claimed.
    In general, take the max over the two.
    '''
    items = join_process_claims(stake_info, gpu_num)
    unavailable_gpu_mem = 0
    for item in items:
        m1 = item.get('process', {}).get('gpu_mem', 0)
        m2 = item.get('claim', {}).get('gpu_mem', 0)
        unavailable_gpu_mem += max(m1, m2)
    return stake_info['gpu_info'][gpu_num]['total_gpu_mem'] - unavailable_gpu_mem

def get_claimed_gpu_mem(stake_info, gpu_num):
    claimed_gpu_mem = 0
    for claim in stake_info.get('claims', []):
        if claim['gpu_num'] == gpu_num:
            claimed_gpu_mem += claim['gpu_mem']
    return claimed_gpu_mem

def make_claim(stake_info):
    '''
    Return the claim_id.
    '''
    gpu_mem = parse_size(args.gpu_mem)

    # Go over the GPUs and find a free one
    for gpu_num in sorted(stake_info['gpu_info'].keys()):
        info = stake_info['gpu_info'][gpu_num]
        total_gpu_mem = info['total_gpu_mem']

        # See if we can put it on this GPU
        if gpu_mem > available_gpu_mem(stake_info, gpu_num):
            continue

        # Create a claim
        claim_id = generate_claim_id()
        claim = {
            'claim_id': claim_id,
            'gpu_num': gpu_num,
            'gpu_mem': gpu_mem,
            'command': args.command,
            'start_date': time.time(),
        }
        stake_info.setdefault('claims', []).append(claim)
        write_stake_info(stake_info)

        log('claim %s taking %s memory on GPU%s, where %s/%s is available' % \
            (claim_id, size_str(gpu_mem), gpu_num, size_str(available_gpu_mem(stake_info, gpu_num)), size_str(total_gpu_mem)))

        return claim_id
    return None

def get_claim(stake_info, claim_id):
    for claim in stake_info.get('claims', []):
        if claim['claim_id'] == claim_id:
            return claim
    raise Exception('Internal error')

def run_command(claim_id, stake_info):
    claim = get_claim(stake_info, claim_id)

    command = claim['command']
    if len(command) == 1:
        command = ['bash', '-c', command[0]]

    # Make the process only use the available GPU.
    os.environ['CUDA_VISIBLE_DEVICES'] = str(claim['gpu_num'])

    p = subprocess.Popen(command)
    log('Running as pid %d: %s' % (p.pid, ' '.join(command)))
    start_time = time.time()

    claim['pid'] = p.pid
    write_stake_info(stake_info)
    process = None
    max_gpu_mem = 0

    def output_stats():
        stats = {
            'claim': claim,
            'process': process,
            'exitcode': p.returncode,
            'time': time.time() - start_time,
            'max_gpu_mem': max_gpu_mem,
        }
        if args.stats_file:
            with open(args.stats_file, 'w') as f:
                print(json.dumps(stats), file=f)

    #while p.returncode is None:
    first = True
    while p.poll() is None:
        if not first:
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                log('Got Ctrl+C, killing process %d' % p.pid)
                p.terminate()
        first = False

        stake_info = read_update_stake_info()
        gpu_mem = None

        # Associate process with claim
        processes = stake_info['gpu_info'][claim['gpu_num']].get('processes', [])
        process = None
        for proc in processes:
            if is_pid_under(p.pid, proc['pid']):
                process = proc
                max_gpu_mem = max(max_gpu_mem, proc['gpu_mem'])
                break

        if process:
            if process['gpu_mem'] > claim['gpu_mem']:
                log('GPU memory usage %s exceeded claim %s, killing process %d and %d' % (size_str(process['gpu_mem']), size_str(claim['gpu_mem']), p.pid, process['pid']))
                os.kill(process['pid'], signal.SIGTERM)
                #p.terminate()

        output_stats()

    log('Process %d finished (exitcode %d, time %ds, max_gpu_mem %s)' % (p.pid, p.returncode, time.time() - start_time, size_str(max_gpu_mem)))
    output_stats()
    sys.exit(p.returncode)

def do_create():
    # Claim some resources
    start_time = time.time()
    first_time = True
    while time.time() - start_time < args.wait_time:
        stake_info = read_update_stake_info()
        claim_id = make_claim(stake_info)
        if claim_id:
            break
        if args.wait_time > 0:
            if first_time:
                log('Waiting for something to free up...')
                first_time = False
            time.sleep(1)

    if not claim_id:
        log('Failed to claim resources')
        sys.exit(1)

    # Run the command
    run_command(claim_id, stake_info)

def do_info():
    stake_info = read_update_stake_info()
    table = []
    table.append(['gpu', 'claimed', 'used', 'total', 'available'])
    for gpu_num in sorted(stake_info['gpu_info'].keys()):
        info = stake_info['gpu_info'][gpu_num]
        claimed_gpu_mem = get_claimed_gpu_mem(stake_info, gpu_num)
        table.append([
            gpu_num,
            size_str(claimed_gpu_mem),
            size_str(info['free_gpu_mem']),
            size_str(info['total_gpu_mem']),
            size_str(available_gpu_mem(stake_info, gpu_num))
        ])

        my_claims = [claim for claim in stake_info.get('claims', []) if claim['gpu_num'] == gpu_num]

        # Print out claims
        claimed_processes = []
        for claim in my_claims:
            used_gpu_mem_str = '-'
            # Find the process corresponding to this claim
            for process in info.get('processes', []):
                if is_pid_under(claim['pid'], process['pid']):
                    used_gpu_mem_str = size_str(process['gpu_mem'])
                    claimed_processes.append(process)
                    break
            table.append([gpu_num, size_str(claim['gpu_mem']), used_gpu_mem_str, 'RUN %s (pid %s)' % (' '.join(claim['command']), claim['pid'])])

        # Print out rogue processes
        for process in info.get('processes', []):
            if process in claimed_processes:
                continue
            table.append([gpu_num, '-', size_str(process['gpu_mem']), 'RUN %s (pid %s)' % (process['command'], process['pid'])])

    for row in table:
        print('\t'.join(map(str, row)))

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-b', '--base-dir', help='Directory where all the claims are stored', default='/u/nlp/machine-info/stake/var')
    parser.add_argument('-g', '--gpu-mem', help='Amount of GPU memory (e.g., 3, 3k, 3m, 3g)', default='2g')
    parser.add_argument('-s', '--stats-file', help='File to output stats about the execution')
    parser.add_argument('-w', '--wait-time', type=int, help='Number of seconds to wait for a free resource', default=10000000)
    parser.add_argument('command', nargs='*')
    args = parser.parse_args()

    hostname = socket.gethostbyaddr(socket.gethostname())[0].split('.')[0]
    stake_path = os.path.join(args.base_dir, hostname + '.json')
    log('state path: %s' % stake_path)

    if args.command:
        do_create()
    else:
        do_info()
