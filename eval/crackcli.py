#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
import cockroach
import cobra_time

sys.path.insert(0, os.path.abspath('..'))

from clint.textui import prompt, puts, colored, validators

def init():
    cockroach.nodes_running = dict()
    cockroach.cockroach_config = {}
    cockroach.read_config()
    cockroach.clean_all_environment()
    cockroach.prepare_utils()
    cobra_time.all_sync_ntp()
    cockroach.start_cluster()
    cockroach.create_user_database()

def show_status():
    cockroach.sync_status_remote()
    for i in range(cockroach.NODE_NUMBER):
        if cockroach.nodes_running[i]:
            puts(colored.green("[{0}] RUNNING".format(cockroach.get_node_name(i))))
        else:
            puts(colored.red("[{0}] STOP".format(cockroach.get_node_name(i))))


def mainLoop():
    cmd_status = 'status'
    cmd_randkill = 'randkill'
    cmd_randstart = 'randstart'
    cmd_deeprestart = 'deeprestart'
    cmd_randworkload = 'randworkload'
    cmd_exit = 'exit'
    cmd_start = 'start'
    cmd_kill = 'kill'
    cmd_strobetime = 'strobetime'

    init()

    while True:
        # cmds
        cmd_options = [
            {'selector':'0','prompt':'start/restart the cluster','return':cmd_deeprestart},
            {'selector':'9','prompt':'(gracefully) exit','return':cmd_exit},
            {'selector':'1','prompt':'show status','return':cmd_status},
            {'selector':'2','prompt':'random kill/start workload','return':cmd_randworkload},
            {'selector':'3','prompt':'random kill one node','return':cmd_randkill},
            {'selector':'4','prompt':'random start one node','return':cmd_randstart},
            {'selector':'5','prompt':'start one node','return':cmd_start},
            {'selector':'6','prompt':'kill one node','return':cmd_kill},
            {'selector':'7','prompt':'strobe time yak','return':cmd_strobetime},
        ]
        cmd = prompt.options("Cockroach cracker commands:", cmd_options)

        if cmd == cmd_status:
            show_status()
        elif cmd == cmd_randkill:
            cockroach.random_kill()
        elif cmd == cmd_randstart:
            cockroach.random_start()
        elif cmd == cmd_randworkload:
            cockroach.kill_start_workload(30)
        elif cmd == cmd_deeprestart:
            init()
        elif cmd == cmd_start:
            i = prompt.query('which node id?', validators=[validators.IntegerValidator()])
            cockroach.start_node(i)
        elif cmd == cmd_kill:
            i = prompt.query('which node id?',
                             validators=[validators.IntegerValidator()])
            cockroach.kill_node(i)
        elif cmd == cmd_exit:
            cockroach.gracefully_exit()
            break
        elif cmd == cmd_strobetime:
            cobra_time.strobe_time('yak', 1000, 1000, 60)

        puts(colored.blue("-------"))


if __name__ == '__main__':
    mainLoop()
