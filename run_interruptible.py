#!/usr/bin/env python
# run interruptible job
# dor arad

from __future__ import print_function
from base64 import b64encode
import mechanize
from bs4 import BeautifulSoup
import re
import datetime
import argparse
import os 
import subprocess
from pprint import pprint
from slackclient import SlackClient
import sys 
import shlex
import time
from termcolor import colored, cprint

def bold(txt):
    return colored(txt, attrs=["bold"])

def bcolored(txt, color):
    return colored(txt, color, attrs=["bold"])

url = 'https://nlp.stanford.edu/local/machines.shtml'
username = 'nlp'
password = 'lundard'

b64login = b64encode('%s:%s' % (username, password))

br = mechanize.Browser()

br.addheaders.append(
  ('Authorization', 'Basic %s' % b64login)
)

def log(s, color):
    prefix = bold("[run_interruptible %s]: " % (time.strftime('%Y-%m-%d %H:%M:%S')))
    print((prefix + bcolored(s, color)), file=sys.stderr)

def getMachinesInfo():
    br.open(url)
    r = br.response()
    data = r.read()

    soup = BeautifulSoup(data, "html.parser")

    stats = soup.find(id="body_content").find(class_="widewrapper weak-highlight").find(
        class_="container content")
    overallInfo = stats.find_all('p')
    machines = stats.find_all('table',recursive=False)[8:23] # todo add id to html instead..
    machinesInfo = {}   

    i = 4
    freeCount = 0
    for machine in machines:
        j = 0
        machineInfo = {}
        gpus = machine.findChildren('tr', recursive = False)[1].find_all('tr')[1:]
        for gpu in gpus:
            users = str(gpu.find_all('td')[-3].get_text())
            if users == "none":
                freeCount += 1
            else:
                if "," in users:
                    users = users.split(",")
                machineInfo["gpu" + str(j)] = users
            j += 1
        if machineInfo != {}:
            machinesInfo["jagupard" + str(i)] = machineInfo
        i += 1

    cpus = re.findall(r'[a-z]+  \(\d+.\d% cpu\)', str(overallInfo[6]))
    gpus = re.findall(r'[a-z]+: \d+', str(overallInfo[7]))
    free = re.findall(r'\d+ in total', str(overallInfo[8]))
    free = free[0] if len(free) > 0 else "99 in total"

    cpus = {re.findall(r'[a-z]+', cpu)[0]: str(float(re.findall(r'\d+.\d',cpu)[0])) + "%" for cpu in cpus}
    gpus = {re.findall(r'[a-z]+', gpu)[0]: int(re.findall(r'\d+',gpu)[0]) for gpu in gpus}
    freeWeb = int(re.findall(r'\d+',free)[0])

    if freeWeb == 99:
        info["gpu_total_usage"]["free"] = freeCount

    info = {}
    info["datetime"] = datetime.datetime.now().strftime('%Y/%m/%d %H:%M:%S')
    info["cpu_total_usage"] = cpus
    info["gpu_total_usage"] = gpus
    info["free_gpus"] = freeWeb
    info["machine_users"] = machinesInfo

    return info

def runProcess(command, threshold, waitTime, slackClient):
    if len(command) == 1:
        command = ["bash", "-c", command[0]]

    p = subprocess.Popen(command)
    log("Running as pid %d: %s" % (p.pid, ' '.join(command)), "blue")
    start_time = time.time()

    first = True
    while p.poll() is None:
        if not first:
            try:
                time.sleep(waitTime)
            except KeyboardInterrupt:
                log("Got Ctrl+C, killing process %d" % p.pid, "blue")
                p.terminate()
        first = False

        freeNum = int(getMachinesInfo()["free_gpus"])

        if freeNum < threshold:
            log("Number of free GPUs %d got below %d - killing process %d" % (freeNum, threshold, p.pid), "red")
            p.terminate()
            if slackClient != None:
                sc.api_call(
                  "chat.postMessage",
                  channel="#myCluster",
                  text="Job got %d killed. Free GPUs: %d " % (p.pid, freeNum)
                )

    log('Process %d finished (exitcode %d, time %ds)' % (p.pid, p.returncode, time.time() - start_time), "blue")
    sys.exit(p.returncode)

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", "--minimal-gpu-num", type=int, help="Minimal number of gpus to maintain", default=8)
    parser.add_argument("-t", "--wait-time", type=int, help="Number of minutes to wait between gpu threshold check", default=10)
    parser.add_argument("-s", "--slack-api-token", type=str, help="Number of minutes to wait between gpu threshold check", default="")
    parser.add_argument("-c", "--command", type=str, help="command to run", default="")
    args = parser.parse_args()

    sc = None
    if args.slack_api_token != "":
        sc = SlackClient(args.slack_api_token)

    if args.command != "":
        runProcess(shlex.split(args.command), args.minimal_gpu_num, args.wait_time * 60, sc)
    else:
        pprint(getMachinesInfo())
