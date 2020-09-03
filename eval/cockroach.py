#!/bin/python
from subprocess import Popen, run
import time
import sys
#import signal
import random
import subprocess
import yaml
import fabric
import os
import invoke

NODE_NUMBER = -1
COBRA_ROOT_DIR = '/tmp/cobra'
STORE_DIR = '/tmp/cobra/cockroach/store'
LOG_DIR = '/tmp/cobra/cockroach'
UTIL_DIR = '/tmp/cobra/utils'
nodes_running = dict()
cockroach_config = {}

## parameters
RAND_KILL_SLEEP = 3

## helper functions
def read_config(fname=''):
    global cockroach_config, NODE_NUMBER
    if fname == '':
        fname = os.path.dirname(
            os.path.abspath(__file__)) + '/cockroach_config.yaml'
    with open(fname, 'r') as f:
        cockroach_config = yaml.safe_load(f)
        NODE_NUMBER = len(cockroach_config['nodes'])

def get_node_name(id):
    return 'node{}'.format(id)

def get_url(id):
    node = cockroach_config['nodes'][id]
    return '{}:{}'.format(node['ip'], node['listen-addr'])

def fail(msg):
    print("[ERROR] {0}".format(msg))
    kill_all()
    exit(1)

def run_shell(cmd, checkFail=True, failStop=True, out=None, err=None):
    try:
        run(cmd, shell=True, check=checkFail, stdout=out, stderr=err)
    except: #subprocess.CalledProcessError:
        if failStop:
            fail("Run cmd <{0}>".format(cmd))
        else:
            return False
    return True

def send_file(host:str, local_file, remote_file):
    cmd = "scp {0} {1}:{2}".format(local_file, host, remote_file)
    print("[INFO] send file ({0})".format(cmd))
    run_shell(cmd, out=sys.stdout, err=sys.stderr)

# catch exception and add support for return value
def run_remote(host, cmd, retval: bool=False):
    try:
        conn = fabric.Connection(host)
        ret = conn.run(cmd)
        return ret if retval else True
    except invoke.exceptions.UnexpectedExit as e:
        print("[ERROR] Remote run on host[{0}] cmd[{1}]".format(host, cmd))
        print("[ERROR] Exception: " + str(e))
        return None if retval else False
    except:
        print("[ERROR] Remote run on host[{0}] cmd[{1}]".format(host, cmd))
        print("[ERRPR] Some unknown error happend")
        return None if retval else False

def sudo_remote(host, cmd):
    try:
      config = fabric.Config(
          overrides={'sudo': {
              'password': cockroach_config['password']
          }})
      conn = fabric.Connection(host, config=config)
      conn.sudo(cmd)
      return True
    except invoke.exceptions.UnexpectedExit as e:
        print("[ERROR] Remote run on host[{0}] cmd[{1}]".format(host, cmd))
        print("[ERROR] Exception: " + str(e))
        return False
    except:
        print("[ERROR] Remote run on host[{0}] cmd[{1}]".format(host, cmd))
        print("[ERRPR] Some unknown error happend")
        return False

def run_sql(sql, url):
    run_shell('cockroach sql --insecure --host={0} --execute "{1}"'.format(url, sql))

def check_db_connections():
    for i in range(NODE_NUMBER):
        check_db_connection(i)

def check_db_connection(id:int):
    url = get_url(id)
    print("---> Check connection of {0}".format(url))
    for i in range(10):
        succ = run_shell(
            'cockroach sql --insecure --host={1} --execute="{0}"'.format("SELECT 1", url),
            failStop=False,
            out=subprocess.DEVNULL,
            err=subprocess.DEVNULL)
        if succ:
            return  # successfully connect
        else:
            time.sleep(1)
            print("  ---> retry {0} times".format(i))
    fail("Cannot connect to CockroachDB")


def install_cockroackdb(host):
    run_remote(
        host,
        'wget -qO- https://binaries.cockroachdb.com/cockroach-v19.1.3.linux-amd64.tgz | tar  xvz'
    )
    sudo_remote(
        host, 'cp -i cockroach-v19.1.3.linux-amd64/cockroach /usr/local/bin')

def prepare_utils():
    util_files = ['bump-time', 'strobe-time', 'strobe-time.sh']
    local_util_dir = os.path.dirname(os.path.abspath(__file__)) \
                + '/utils_cracker/'
    remote_util_dir = UTIL_DIR

    for node in cockroach_config['nodes']:
        host = node['ssh-name']
        for f in util_files:
            send_file(host, local_util_dir + f,\
                  remote_util_dir + "/" + f)


## main functions
def sync_status_remote():
    nodes = cockroach_config['nodes']
    for i in range(len(nodes)):
        pid = get_pid_by_id(i)
        if pid:
            nodes_running[i] = True
        else:
            nodes_running[i] = False

def clean_all_environment():
    for node in cockroach_config['nodes']:
        clean_environment(node['ssh-name'])

def clean_environment(host: str):
    print("---> Cleaning environment of node[{}]...".format(host))
    sudo_remote(host, 'killall cockroach || true')
    sudo_remote(host, 'killall cockroach || true')
    time.sleep(1)
    # earse the whole folder; need to use sudo, in case there is one folder created by other users
    sudo_remote(host, "rm -r {0} || true".format(COBRA_ROOT_DIR))
    run_remote(host, "mkdir -p {0} {1} {2}".format(STORE_DIR, LOG_DIR, UTIL_DIR))

def create_user_database():
    check_db_connection(0)
    url = get_url(0)
    print("---> Create User and Database")
    run_sql("CREATE USER IF NOT EXISTS maxroach;", url)
    run_sql("CREATE DATABASE cobra;", url)
    run_sql("GRANT ALL ON DATABASE cobra TO maxroach;", url)
    print("---> Setup cracker default config")
    run_sql(
        "ALTER DATABASE cobra CONFIGURE ZONE USING range_min_bytes = 16000, range_max_bytes = 65536;",
        url
    )

def start_node(i: int):
    nodes = cockroach_config['nodes']
    if i < 0 or i >= len(nodes):
        return
    if i in nodes_running and nodes_running[i]:
        return
    node_info = nodes[i]
    print("starting node{} on {}".format(i, node_info['ssh-name']))
    node_name = get_node_name(i)
    cmd = [
        "cockroach", "start", "--insecure",
        "--store={0}/{1}".format(STORE_DIR, node_name),
        "--listen-addr={0}:{1}".format(node_info['ip'],
                                       node_info['listen-addr']),
        "--http-addr={0}:{1}".format(node_info['ip'], node_info['http-addr'])
    ]
    if i != 0:
        join = '{}:{}'.format(nodes[0]['ip'], nodes[0]['listen-addr'])
        cmd.append('--join={0}'.format(join))

    log_file = "{0}/{1}.log".format(LOG_DIR, node_name)
    run_remote(node_info['ssh-name'],
               (' '.join(cmd)) + ' > {} 2>&1 &'.format(log_file))

    nodes_running[i] = True


def start_node_local(i: int):
    node_name = get_node_name(i)
    cmd = [
        "cockroach", "start", "--insecure",
        "--store={0}/{1}".format(STORE_DIR, node_name),
        "--listen-addr=localhost:{0}".format(26257 + i),
        "--http-addr=localhost:{0}".format(8080 + i)
    ]
    if i != 0:
        cmd.append('--join=localhost:26257')

    f = open("{0}/node{1}.log".format(LOG_DIR, i + 1), 'a+')
    Popen(cmd, stdout=f, stderr=f)
    print("start node {0}".format(i))
    nodes_running[i] = True
    return node_name

def start_cluster():
    nodes = cockroach_config['nodes']
    for i in range(len(nodes)):
        start_node(i)
        nodes_running[i] = True
    print(
        "---> Cluster of {0} nodes started successfully?".format(NODE_NUMBER))

def get_pid_by_id(id: int):
    name = get_node_name(id)
    host = cockroach_config['nodes'][id]['ssh-name']
    ret = run_remote(
        host,
        'pgrep -f "[c]ockroach start .*{0}/{1}"'.format(STORE_DIR, name),
        True)
    if ret != None:
        pid = ret.stdout
        pid = pid.replace('\n', '')
        return int(pid)
    else:
        return None

# kill the node process immediately, with no cleanup work
def kill_node(id: int):
    if id < 0 or id >= NODE_NUMBER:
        return
    if not nodes_running[id]:
        return
    host = cockroach_config['nodes'][id]['ssh-name']
    print("kill {0} on {1}".format(get_node_name(id), host))
    pid = get_pid_by_id(id)
    if pid:
        # Send SIGKILL (on Linux)
        cmd = "kill -9 {0}".format(pid)
        run_remote(host, cmd)
    nodes_running[id] = False

# send SIGTERM
def graceful_shutdown(id: int):
    if not nodes_running[id]:
        return
    print("graceful shutdown node {0}".format(id))
    pid = get_pid_by_id(id)
    if pid:
        # Send SIGTERM (on Linux)
        cmd = "kill -15 {0}".format(pid)
        host = cockroach_config['nodes'][id]['ssh-name']
        run_remote(host, cmd)
    nodes_running[id] = False






def random_kill():
    id = random.randint(0, NODE_NUMBER - 1)
    if nodes_running[id]:
        kill_node(id)

def random_start():
    id = random.randint(0, NODE_NUMBER - 1)
    if not nodes_running[id]:
        start_node(id)


## convenience functions
def kill_all():
    for i in range(NODE_NUMBER):
        kill_node(i)

def gracefully_exit():
    for i in range(NODE_NUMBER):
        graceful_shutdown(i)

def kill_start_workload(loop):
    for i in range(loop):
        random_kill()
        random_start()
        time.sleep(RAND_KILL_SLEEP)

def main():
    read_config()

    clean_all_environment()
    start_cluster()

    # init the cluster
    time.sleep(1)

    check_db_connections()

    # randomly start and kill
    for i in range(10):
        random_kill()
        random_start()
        time.sleep(1)

    # cleanup
    print("---> Cleaning up...")
    time.sleep(1)
    for i in range(NODE_NUMBER):
        kill_node(i)


if __name__ == '__main__':
    main()
