#!/usr/bin/python

from commands import getoutput
import os
import re
import sys

gpu_to_pid = {}

status_dir = sys.argv[1]

for machine_name in os.listdir(status_dir):
  if not os.path.isdir(status_dir+"/"+machine_name):
    continue
  if machine_name == "machine-info":
    continue
  if not os.path.exists(status_dir+"/"+machine_name+"/nvidia-smi"):
    continue
  nvidia_contents = file(status_dir+"/"+machine_name+"/nvidia-smi").read().rstrip("\n").lstrip("\n")
  if nvidia_contents == "none":
    continue
  #else:
    #print "---"
    #print machine_name
    #print nvidia_contents
  gpu_to_pid[machine_name] = {}


for machine_name in gpu_to_pid:
  nvidia_contents = getoutput("cat "+status_dir+"/"+machine_name+"/nvidia-smi | egrep \"MiB \\|$\"").split("\n")
  for line in nvidia_contents:
    clean_line = line[1:-1].rstrip("\t ").lstrip("\t ")
    if not clean_line:
      continue
    #print clean_line.split()
    line_entries = clean_line.split()
    gpu = line_entries[0]
    pid = line_entries[1]
    if not gpu_to_pid[machine_name].get(gpu):
      gpu_to_pid[machine_name][gpu] = []
    gpu_to_pid[machine_name][gpu].append(pid)

#print gpu_to_user

pid_to_user = {}

for machine_name in gpu_to_pid:
  pid_to_user[machine_name] = {}
  process_lines = file(status_dir+"/"+machine_name+"/ps-axuwww").read().split("\n")
  for pl in process_lines:
   if len(pl.split()) > 1:
     user = pl.split()[0]
     pid = pl.split()[1]
     pid_to_user[machine_name][pid] = user

gpu_to_user = {}

for machine_name in gpu_to_pid:
  gpu_to_user[machine_name] = {}
  for gpu in gpu_to_pid[machine_name]:
    gpu_to_user[machine_name][gpu] = []
    for pid in gpu_to_pid[machine_name][gpu]:
      gpu_to_user[machine_name][gpu].append(pid_to_user[machine_name][pid])

for machine_name in gpu_to_user:
  for gpu in gpu_to_user[machine_name]:
    original_list = gpu_to_user[machine_name][gpu]
    gpu_to_user[machine_name][gpu] = list(set(original_list))
    gpu_to_user[machine_name][gpu].sort()

print ":gpu_to_user:"
for machine_name in gpu_to_user:
  print "  "+machine_name+":"
  for gpu in gpu_to_user[machine_name]:
    print "  - - "+gpu
    for user in gpu_to_user[machine_name][gpu]:
      print "    - "+(",".join(gpu_to_user[machine_name][gpu]))
