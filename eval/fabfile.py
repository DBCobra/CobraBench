from fabric import Connection
from fabric import ThreadingGroup
from fabric import task
import fabric
import time
import datetime
import os

from utils import *

dir_path = os.path.dirname(os.path.realpath(__file__))

@task
def install_docker(conn, uname):
    conn.run('curl -fsSL https://get.docker.com -o get-docker.sh')
    conn.run('sudo sh get-docker.sh', pty=True)
    conn.run('sudo usermod -aG docker {}'.format(uname), pty=True)

@task
def copy_file(conn, i=None):
    printG('{0.host} copying files'.format(conn))
    conn.run('rm -r txnBench || true')
    conn.run('mkdir -p txnBench/target')
    conn.put(dir_path + '/../target/txnTest-1-jar-with-dependencies.jar',
             'txnBench/target')
    config_file = ''
    if i is None:
        config_file = '/../config.yaml'
    else:
        config_file = '/../conf{}.yaml'.format(i)
    conn.put(dir_path + config_file, 'txnBench/config.yaml')
    conn.put(dir_path + '/../cobra_key.json', 'txnBench/cobra_key.json')
    conn.put(dir_path + '/../Dockerfile', 'txnBench')

@task
def docker_build(conn):
    cd = 'cd txnBench && '
    conn.run(cd + 'docker build . -t txnbench')

def docker_run(conn, container_name, run_type):
    cd = ' cd txnBench && '
    tmux = ' tmux new-session -d -s '+container_name+' '

    printG("stopping tmux process: ")
    conn.run(" tmux kill-session -t " + container_name + " || true")
    printG("stopping container: " + container_name)
    conn.run('docker stop /'+ container_name +' || true')

    bindPort = " -p 8980:8980" if run_type == "server" else ""

    docker_cmd = 'docker run' + bindPort + ' --rm --name=' +\
    container_name + ' -v $HOME/txnBench:/usr/src/txnbench ' + \
    ' -v $HOME/cobra_tmp:/tmp ' + \
    ' txnbench ' + run_type + ' > '+container_name+'.txt 2>&1'

    printB(docker_cmd)
    conn.run('echo "' + docker_cmd + '" > '+container_name+'.sh')
    cmd = tmux + '"sh %s.sh"' % container_name
    printB(cmd)
    conn.run(cmd)

def mv_cobra_tmp(conn, mv_to='cobra_tmp_trash', new_name=None):
    conn.run('mkdir -p {}'.format(mv_to))
    if new_name is None:
        new_name = datetime.datetime.now().strftime("%m%d-%H:%M:%S")
    conn.run('mv client.txt cobra_tmp || true')
    conn.run('mv server.txt cobra_tmp || true')
    conn.run('mv verifier.txt cobra_tmp || true')

    conn.run('mv cobra_tmp {}/{} || true'.format(mv_to, new_name))
    conn.run('mkdir -p cobra_tmp/cobra/log')

def run_server(conn):
    mv_cobra_tmp(conn)
    docker_run(conn, "server", "server")

def run_client(conn):
    docker_run(conn, "client", "client")

def run_local(conn):
    mv_cobra_tmp(conn)
    docker_run(conn, "client", "local")

def server_is_up(conn):
    container_name = 'server'
    expected_string = 'INFO: Server started, listening'
    res = conn.run('cat {}.txt | grep "{}" || true'.format(container_name, expected_string))
    return expected_string in res.stdout

def wait_for_server_up(conn):
    printG("waiting for server at {0.host} to start...".format(conn))
    while not server_is_up(conn):
        time.sleep(1)

def bench_is_finished(conn):
    res = conn.run('cat client.txt | grep "The test is finished! \\[rejungofszbj\\]" || true')
    return "rejungofszbj" in res.stdout

def wait_for_bench_finish(conn):
    printG("waiting for client at {0.host} to finish...".format(conn))
    while not bench_is_finished(conn):
        time.sleep(1)
    printG("client at {0.host} finished".format(conn))


@task
def rebuild(host):
    copy_file(host);
    docker_build(host);

def restart_l(host):
    try:
        iterator = iter(host)
    except TypeError:
        copy_file(host);
        printG("run local:")
        run_local(host);
    else:
        i = 1
        for h in host:
            copy_file(h, i);
            i += 1
        printG("run local:")
        for h in host:
            run_local(h);

def _restart_v(hostname, bench_num):
    host = Connection(hostname)
    restart_v(host, bench_num)

def restart_v(host, bench_num):
    printG("stopping tmux process: ")
    host.run(" tmux kill-session -t verifier || true")

    tmux = ' tmux new-session -d -s verifier '
    host.run('cd CobraVerifier && cp cobra.conf1 cobra.conf && echo "BENCH_TYPE={}" >> cobra.conf; '.format(bench_num))

    cmd = tmux + ' "cd CobraVerifier; ./run1.sh fetcher audit > ../verifier.txt 2>&1;" '
    printB(cmd)

    host.run(cmd)

@task
def restart_s(host):
    # copy_file(host);
    printG("run server:")
    run_server(host);
    wait_for_server_up(host)

def _monitor_network(hostname, log_file, device=''):
    ## clear space first
    #_stop_monitor(hostname)

    if device == '':
        return
    host = Connection(hostname)
    #tmux = ' tmux new-session -d -s monitor '
    #cmd = tmux + ' "nethogs -t | grep --line-buffered \'5432\' > {}" '.format(log_file)
    #printB(cmd)
    #host.run(cmd)

    host.run('mkdir -p netstats')
    host.run('ifconfig {} | grep "RX packets" >> {}'.format(device, log_file))
    host.run('ifconfig {} | grep "TX packets" >> {}'.format(device, log_file))

def _stop_monitor(hostname, log_file, device=''):
    if device == '':
        return
    host = Connection(hostname)
    host.run('mkdir -p netstats')
    # host.run(' tmux kill-session -t monitor || true')
    host.run('ifconfig {} | grep "RX packets" >> {}'.format(device, log_file))
    host.run('ifconfig {} | grep "TX packets" >> {}'.format(device, log_file))

@task
def restart_c(host):
    copy_file(host);
    printG("run client:")
    run_client(host);

@task
def stop_all(host):
    host.run("tmux kill-server || true")

def _stop_all(host):
    host.run("tmux kill-server || true")

''' ------------- databases related --------------- '''

# postgres
def _clear_database(hostname, table):
    host = Connection(hostname)
    clear_database(host, table)

@task
def clear_database(host, table):
    sql = 'DROP DATABASE testdb; CREATE DATABASE testdb; ALTER DATABASE testdb OWNER TO cobra'
    host.run('echo "'+sql+'" > /tmp/resetDB.sql')
    host.run("psql postgres -a -f /tmp/resetDB.sql")
    sql = 'DROP TABLE {0}; CREATE TABLE {0} (key varchar(255) PRIMARY KEY, value varchar(4095)); ALTER TABLE {0} OWNER to cobra;'.format(table)
    host.run('echo "'+sql+'" > /tmp/clearDB.sql')
    host.run("psql -d testdb -a -f /tmp/clearDB.sql")
    # host.run('rm /tmp/clearDB.sql')

def _reset_database(hostname, fname):
    host = Connection(hostname)
    reset_database(host, fname)

def reset_database(host, fname):
    sql = 'DROP DATABASE testdb; CREATE DATABASE testdb; ALTER DATABASE testdb OWNER TO cobra'
    host.run('echo "'+sql+'" > /tmp/resetDB.sql')
    host.run("psql postgres -a -f /tmp/resetDB.sql")
    # host.run("psql testdb < ~/hdd/pg_backup/" + fname)
    host.run("pg_restore -Fc -j24 -d testdb /home/changgeng/hdd/pg_backup/{}".format(fname))

def delete_traces(hostname,table):
    host = Connection(hostname)
    sql = "DELETE FROM \"{}\" WHERE \"key\" LIKE 'cid[_]_T%CL';".format(table)
    sql += "DELETE FROM \"{}\" WHERE \"key\" LIKE 'cid[_]_T%WO';".format(table)
    sql += "DELETE FROM \"{}\" WHERE \"key\" LIKE 'HISTORY:%';".format(table)
    host.run('echo "' + sql + '" > /tmp/deleteTraces.sql')
    host.run("psql -d testdb -a -f /tmp/deleteTraces.sql")

def main():
    host = Connection("ye");
    copy_file(host); docker_build(host);
    printG("run server:")
    run_server(host);
    wait_for_server_up(host)
    printG("run client:")
    run_client(host);

if __name__ == "__main__":
    main()
