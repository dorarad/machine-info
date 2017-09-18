#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Print information about CodaLab usage as YAML.
The output can be appended to `status.yaml`.

- Detect all CodaLab processes
- Try to identify the corresponding users
- Print the user mapping
"""

import sys, os, shutil, re, subprocess

STATUS_DIR = sys.argv[1]
CL = '/u/nlp/bin/cl'

def get_machines():
    """Get the list of all machines"""
    machine_to_path = {}
    for machine_name in os.listdir(STATUS_DIR):
        path = os.path.join(STATUS_DIR, machine_name)
        if machine_name == 'machine-info' or not os.path.isdir(path):
            continue
        machine_to_path[machine_name] = path
    return machine_to_path

def get_codalab_stuff(path):
    """Get all processes with user = codalab"""
    path = os.path.join(path, 'ps-axuwww')
    if not os.path.exists(path):
        return []
    codalab_stuff = []
    with open(path) as fin:
        header = fin.readline().rstrip('\n').split()
        for line in fin:
            if line.startswith('codalab'):
                codalab_stuff.append(dict(zip(header,
                    line.rstrip('\n').split(None, len(header) - 1))))
    return codalab_stuff

def identify_uuid(ps_fields):
    """Get CodaLab uuids from the COMMAND field"""
    pid, command = ps_fields['PID'], ps_fields['COMMAND']
    match = re.search('0x[0-9a-f]{32}', command)
    if match:
        return match.group()

def map_uuid_to_user(possible_uuids):
    """Call cl to get the usernames."""
    #print possible_uuids
    #process = subprocess.Popen([CL, 'help'], stdin=subprocess.PIPE, stdout=subprocess.PIPE)
    #print >> sys.stderr, process.communicate('\n\n')
    #_ = subprocess.check_output([CL, 'work', 'http://codalab.stanford.edu:2800::'])
    uuids = subprocess.check_output([CL, 'search', '-u',
        '.limit=9000', 'uuid=' + ','.join(possible_uuids)]).strip().split()
    if not uuids:
        return {}
    #print >> sys.stderr, uuids
    output = subprocess.check_output([CL, 'info', '-f', 'uuid,owner,state']
            + uuids)
    #print >> sys.stderr, output
    uuid_to_user = {}
    for line in output.strip().split('\n'):
        uuid, user, state = line.split('\t')
        user = re.search("u?['\"]user_name['\"]: u?['\"]([^'\"]*)['\"]", user).group(1)
        uuid_to_user[uuid] = [user, state]
    print >> sys.stderr, uuid_to_user
    for uuid in set(possible_uuids) - set(uuids):
        uuid_to_user[uuid] = ['unknown', 'zombie']
    return uuid_to_user

def main():
    print ':codalab_to_user:'
    try:
        machine_to_path = get_machines()
        # Get the list of all CodaLab uuids
        machine_to_possible_uuids = {}
        all_uuids = set()
        for machine, path in machine_to_path.items():
            codalab_stuff = get_codalab_stuff(path)
            if codalab_stuff:
                for ps_fields in codalab_stuff:
                    uuid = identify_uuid(ps_fields)
                    if uuid:
                        machine_to_possible_uuids.setdefault(machine, set()).add(uuid)
                        all_uuids.add(uuid)
        # Map uuids to users
        uuid_to_user = map_uuid_to_user(all_uuids)
        for machine, possible_uuids in machine_to_possible_uuids.items():
            users = {}
            for uuid in possible_uuids:
                if uuid in uuid_to_user:
                    user, state = uuid_to_user[uuid]
                    users.setdefault(user, set()).add((uuid, state))
            if users:
                print '  %s:' % machine
                for user, uuids in users.items():
                    print '    %s:' % user
                    for uuid, state in sorted(uuids):
                        print '    - - "%s"' % uuid
                        print '      - "%s"' % state
        print '  :success: true'
    except Exception, e:
        print >> sys.stderr, e
        print '  :success: false'

if __name__ == '__main__':
    main()

