# Cobra Bench

Cobra bench is a component of 
[Cobra](https://github.com/DBCobra/CobraHome) project.
It includes benchmarks to generate histories for 
[Cobra verifier](https://github.com/DBCobra/CobraVerifier) and 
has tools to measure the client-side overheads. 

This tutorial introduces how to build Cobra bench and run it with three different databases: [RocksDB](https://rocksdb.org/), [PostgreSQL](https://www.postgresql.org/), and [Google Cloud Datastore](https://cloud.google.com/datastore).

The following commands have been tested under Ubuntu 18.04.

Build Cobra bench
---

Install maven and java

    $ sudo apt install maven openjdk-8-jdk

Compile the code:

    $ mvn install

Now you can run a test for rocksdb manually:

    $ mkdir -p /tmp/cobra/log
    $ java -ea -jar target/txnTest-1-jar-with-dependencies.jar local config-historysize.yaml

You can change the config file (`config-historysize.yaml`) for different tests.
See [Cobra bench configuration](#config) for more information.

Run Cobra bench with RocksDB (single machine)
---

One can run Cobra bench with RocksDB on a single machine.
The clients and the RocksDB are in the same machine.

First, specify in the config file to use RocksDB as the backend:

    $ cd $COBRA_HOME/CobraBench
    $ cp config.yaml.default config.yaml
    # set config.yaml line 5 to: "LIB_TYPE: 2"

Next, clear the existing logs and databases, and run Cobra bench:

    $ rm -r /tmp/cobra/ /tmp/rocksdb/
    $ mkdir -p /tmp/cobra/log
    $ java -ea -jar target/txnTest-1-jar-with-dependencies.jar local config.yaml



Run Cobra bench with PostgreSQL (multiple machines)
---

This section introduces how to run tests with PostgreSQL on multiple machines. Running the following experiments requires at least two machines: one for clients and one for the database.

**Note**: running the auto-scripts in this chapter will create temporary files under the home folder of your machines (both clients and the database).


### Step 1: Environment for auto-scripts


On _the machine_ that you want to run the database and control the test:

#### (1) config SSH

Our auto-scripts require to login clients' machines without interruption. One needs to setup the ssh keys among machines.

First, add your public key (`~/.ssh/id_rsa.pub`) to your local `~/.ssh/authorized_keys`. Make sure you can run `ssh localhost` without using password.

Second, add these lines to your `~/.ssh/config` , change `[you]` to your username, change `[hostname]` to the machine's ip address,
and `[path_id_rsa]` is the path to your private key  (for example, `~/.ssh/id_rsa`).

``` 
Host client1
	Hostname [hostname]
	User [you] 
	IdentityFile [path_id_rsa]
```

Finally, add your public key (`~/.ssh/id_rsa.pub`) to the machine of "client1"'s file `~/.ssh/authorized_keys`.

Now, you should be able to login to "client1" without password:

    $ ssh [you]@[hostname] -i [path_id_rsa]


#### (2) Install Python


Install [Anaconda](https://www.anaconda.com/products/individual):

    $ cd $COBRA_HOME/CobraBench/
    $ wget https://repo.anaconda.com/archive/Anaconda3-2020.07-Linux-x86_64.sh
    $ bash Anaconda3-2020.07-Linux-x86_64.sh

Then, install required python packages:

``` 
$ conda create --name txn python=3.7.5
$ conda activate txn
$ which python
# You should see something like "/home/ubuntu/anaconda/envs/txn/bin/python"

$ cd $COBRA_HOME/CorbraBench/eval/
$ pip install -r requirements.txt
```

#### (3) Install Redis 

We use Redis to start multiple clients at the same time.


``` 
$ sudo apt install redis
$ pip install redis
$ sudo vim /etc/redis/redis.conf
# comment the line "bind 127.0.0.1 ::1"
# change the line from "protected-mode yes" to "protected-mode no"

$ sudo service redis-server restart
```

Config Redis:

```
$ cd $COBRA_HOME/CobraBench/
$ vim config.yaml.default
# line 38 (REDIS_ADDRESS: "redis://<ip>/0"): replace the <ip> to this machine's ip

$ vim eval/main.py
# line 17 (redis_ip = '192.168.1.176'): change redis_ip in line 17 to this machine's ip
```


#### (4) Setup Docker

Install (change `[username]` to your remote username on the client machines):

``` 
$ cd $COBRA_HOME/CobraBench/
$ fab -r eval/fabfile.py -H localhost install-docker --uname="[username]"
$ fab -r eval/fabfile.py -H client1 install-docker --uname="[username]"
```

Build docker image:

``` 
$ cp config.yaml.default config.yaml
$ fab -r eval/fabfile.py -H localhost rebuild
$ fab -r eval/fabfile.py -H client1 rebuild
```

### Step 2: Setup PostgreSQL

#### (1) Install PostgreSQL

```
$ wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo apt-key add -
$ sudo sh -c 'echo "deb http://apt.postgresql.org/pub/repos/apt/ $(lsb_release -sc)-pgdg main" > /etc/apt/sources.list.d/PostgreSQL.list'
$ sudo apt update
$ sudo apt-get install postgresql-10
```

#### (2) config  PostgreSQL

Add one line below at the end of the file `/etc/postgresql/10/main/postgresql.conf` : 

```
listen_addresses = '*'
```

Add two lines below at the end of the file `/etc/postgresql/10/main/pg_hba.conf`:

``` 
host    all             all              0.0.0.0/0                       md5
host    all             all              ::/0                            md5
```

Restart PostgreSQL:

    $ sudo service postgresql restart

Set the ip address (`DB_URL` below) in the file `config.yaml.default` to the database's ip address.

    DB_URL: "jdbc:postgresql://<ip>:5432/testdb"


#### (3) create a PostgreSQL user

``` 
$ sudo -u postgres psql
$ create user cobra with password 'Cobra<318';
$ create user [yourusername];
$ alter user [yourusername] with superuser;
$ \q
```

### <a name='autorun' /> Step 3: Run experiments with auto-scripts

#### (1) Client configuration

Edit `$COBRA_HOME/CobraBench/eval/main.py` and set line 19 (`client_machine` below) to the list of clients' hostnames. For our two machine setup, we should write:

    client_machine = ['client1']

#### (2) Run experiments

Edit `eval/main.py` (line 269) to select the parameters for an experiment:

``` python
run_one_series(database, workload, contention, inst_level)
# database: one of ['rocksdb', 'postgres', 'google']
# workload: one of ['cheng', 'twitter', 'rubis', 'tpcc', 'ycsb'] (cheng stands for BWrite-RW in the paper)
# contention: one of ['low', 'high']
# inst_level: one of ['no', 'local']
```

Run the script to start the experiment:

    $ python eval/main.py recompile

Then, Cobra bench will be executed in a tmux session in each client machine. The runtime log will be printed to `$HOME/client.txt` . 
If you run a series of evaluations, the logs will be copied to the folder `~/trials` with corresponding names. 


Note: we will use localhost as client for `rocksdb` and `google` , while we use the clients
in the list to run `postgres` . 


To collect numbers from the logs, you can run:

    $ cd eval
    $ ln -s ~/trials trials
    $ python report.py

It will generate csv files containing throughput and latency under `eval/data`.

Run Cobra bench with Google Cloud Datastore
---

1. Create datastore in google.
2. Download the credentials and save it in "cobra_key.json" (`$COBRA_HOME/CobraBench/cobra_key.json` is a dummy file).
3. Please refer to [this link](https://cloud.google.com/datastore/docs/reference/libraries) for further instructions. 
4. Follow [Step 3: Run experiments with auto scripts](#autorun) in the PostgreSQL chapter, and choose `database` to be `google` in the file `main.py`.


Reproducing results
---

#### Latency and throughput overheads

This is an experiment in Cobra paper (to appear) Section 6.3.
One can reproduce the results by running auto-scripts as described above. In particular, 
follow [Step 3: Run experiments with auto scripts](#autorun);
choose a database, set the workload to `twitter`, and choose `inst_level` as `no` for legacy systems and `local` for Cobra.


#### Network cost and history size

**Collect network traffic:** run benchmarks as described above, set the database to `postgres`, vary the workload and `inst_level`. Results will be stored in `./netstats` . 

**Calculate history size:** run one trial manually as below.

    $ rm -r /tmp/cobra/ /tmp/rocksdb/
    $ mkdir -p /tmp/cobra/log
    $ cp config-historysize.yaml config.yaml # you can edit this file to select workloads
    $ java -ea -jar target/txnTest-1-jar-with-dependencies.jar local

Then you can calculate the total size of `/tmp/cobra/log/*.log`.

<a name='conifg' /> Cobra bench configuration
---

**[under construction]**