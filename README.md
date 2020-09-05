# Cobra Bench

Cobra bench is a component of 
the [Cobra](https://github.com/DBCobra/CobraHome) project.
It includes benchmarks to generate histories for 
[Cobra verifier](https://github.com/DBCobra/CobraVerifier) and 
has tools to measure client-side overheads. 

This tutorial introduces how to build Cobra bench and run it with three different databases: [RocksDB](https://rocksdb.org/), [PostgreSQL](https://www.postgresql.org/), and [Google Cloud Datastore](https://cloud.google.com/datastore).

The following commands have been tested under Ubuntu 18.04.

Build Cobra bench
---

Install maven:

    $ sudo apt install maven

Compile the code:

    $ mvn install

Now you can run a simple test:

    $ mkdir -p /tmp/cobra/log
    $ java -ea -jar target/txnTest-1-jar-with-dependencies.jar local config-historysize.yaml

The history will be stored in the folder `/tmp/cobra/log`.

You can specify workload parameters in Cobra's config file (`config-historysize.yaml` in the above case).
See [Cobra bench configuration](#config) for more information.

Run Cobra bench with RocksDB (single machine)
---

You can run Cobra bench with RocksDB on a single machine --
both RocksDB and its clients will run on the same machine.
(One can also run RocksDB and its clients on different machines, see [Step 3](#autorun) in the next section for more information.)

First, specify using RocksDB as the backend in the config file:

    $ cd $COBRA_HOME/CobraBench
    $ cp config.yaml.default config.yaml
    # set config.yaml line 5 to: "LIB_TYPE: 2"

Next, clear the existing logs and databases, and run Cobra bench:

    $ rm -r /tmp/cobra/ /tmp/rocksdb/
    $ mkdir -p /tmp/cobra/log/
    $ java -ea -jar target/txnTest-1-jar-with-dependencies.jar local config.yaml

The history will be stored in the folder `/tmp/cobra/log/`.


Run Cobra bench with PostgreSQL (multiple machines)
---

This section describes how to run experiments with PostgreSQL on multiple machines using Cobra's auto-scripts. Running them requires at least two machines: one for clients and one for the database.

**Note**: running the auto-scripts will create temporary files under the home folder of clients' machines.


### Step 1: Environment for auto-scripts


On the machine that you want to host the database and control the experiments:

#### <a name='ssh' /> (1) config SSH

Cobra's auto-scripts require logging into clients' machines without passwords. You need to setup ssh keys among machines.

First, add your public key (`~/.ssh/id_rsa.pub`) to your local `~/.ssh/authorized_keys`. Make sure you can run `ssh localhost` without using a password.

Second, add lines below to your `~/.ssh/config`, change `[username]` to your Linux username, change `[hostname]` to the machine's IP address (or alias of the client's machine),
and `[path_id_rsa]` is the path to your private key  (for example, `~/.ssh/id_rsa`).

``` 
Host client1
    Hostname [hostname]
    User [username] 
    IdentityFile [path_id_rsa]
```

Finally, add your public key (`~/.ssh/id_rsa.pub`) to `client1`'s  file `~/.ssh/authorized_keys`.

Now, you should be able to log in `client1` without password:

    $ ssh client1


#### (2) Install Anaconda and required packages


Install [Anaconda](https://www.anaconda.com/products/individual):

    $ cd $COBRA_HOME/CobraBench/
    $ wget https://repo.anaconda.com/archive/Anaconda3-2020.07-Linux-x86_64.sh
    $ bash Anaconda3-2020.07-Linux-x86_64.sh
    $ source ~/.bashrc # or any shell you are using

Then, create Anaconda environment and install required packages:

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
# comment out the line "bind 127.0.0.1 ::1"
# change the line "protected-mode yes" to "protected-mode no"

$ sudo service redis-server restart
```

Config Redis:

```
$ cd $COBRA_HOME/CobraBench/
$ vim config.yaml.default
# line 38 (REDIS_ADDRESS: "redis://[ip]/0"): replace [ip] with this machine's IP address

$ vim eval/main.py
# line 17 (redis_ip = [ip]): change [ip] to this machine's IP address
```


#### (4) Setup Docker images

Install Docker packages:

``` 
$ cd $COBRA_HOME/CobraBench/
$ fab -r eval/fabfile.py -H localhost install-docker --uname="[username]"
$ fab -r eval/fabfile.py -H client1 install-docker --uname="[username]"
```

Note that the above `[username]`s are Linux usernames for `localhost` and `client1`.


Build Docker image:

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

Modify the file `/etc/postgresql/10/main/postgresql.conf`: 

```
# add this line to the end of the file.
listen_addresses = '*'
# find max_connections, set it to 500.
max_connections = 500
```

Add the two lines below at the end of the file `/etc/postgresql/10/main/pg_hba.conf`:

``` 
host    all             all              0.0.0.0/0                       md5
host    all             all              ::/0                            md5
```

Restart PostgreSQL:

    $ sudo service postgresql restart

Set the database's IP address `config.yaml.default` in the config file:

    $ vim $COBRA_HOME/CobraBench/config.yaml.default
    # line 6 (DB_URL: "jdbc:postgresql://[ip]:5432/testdb"): replace [ip] with the database's IP address


#### (3) create a PostgreSQL user

``` 
$ sudo -u postgres psql
$ create user cobra with password 'Cobra<318';
$ create user [username];
$ alter user [username] with superuser;
$ \q
```

Note that the above `[username]` should be the username associated with the private key mentioned in [config SSH](#ssh).

### <a name='autorun' /> Step 3: Run experiments with auto-scripts

#### (1) Client configuration

In Cobra's scripts, specify which clients are involved in this experiment. For our two-machine setup, we have only one client, `client1`:

    $ vim $COBRA_HOME/CobraBench/eval/main.py
    # line 20 (client_machine = ['client1']): include the clients' hostnames here

#### (2) Run experiments

Edit `eval/main.py` (line 270) to select the parameters for an experiment:

``` python
run_one_series(database, workload, contention, inst_level)
# database: one of ['rocksdb', 'postgres', 'google']
# workload: one of ['cheng', 'twitter', 'rubis', 'tpcc', 'ycsb'] (cheng stands for BWrite-RW in the paper)
# contention: one of ['low', 'high']
# inst_level: one of ['no', 'local']
```

Run the script to start the experiment:

    $ python eval/main.py recompile

Then, Cobra bench will be executed in a tmux session in each client machine. The runtime log will be printed to `$HOME/client.txt`. 
If you run a series of evaluations, the logs for each run will be automatically copied to the folder `eval/trials` with corresponding names. 


Note: to experiment with different databases, auto-scripts use the machine `localhost` as the client for `rocksdb` and `google`, while using the `client_machine` list (in file `eval/main.py`) to run `postgres` . 


To collect results from the logs (which are stored under `eval/trials`), you can run:

    $ cd $COBRA_HOME/CobraBench/eval
    $ python report.py

The script `report.py` will generate csv files (under `eval/data`) containing clients' throughput and latency for each experiment.

Run Cobra bench with Google Cloud Datastore
---

1. Create a Cloud Datastore account.
2. Download the credentials and save it in "cobra_key.json" (`$COBRA_HOME/CobraBench/cobra_key.json` in the repository is a dummy file).
3. Please refer to [this link](https://cloud.google.com/datastore/docs/reference/libraries) for further instructions. 
4. Follow [Step 3: Run experiments with auto scripts](#autorun) in the PostgreSQL chapter, and choose `database` to be `google` in the file `main.py`.


Reproduce results
---

The following instructions describe how to reproduce the results in the Cobra paper [[1]](#cobrapaper), Section 6.3.

#### Latency and throughput overheads (Figure 10)


You can reproduce Figure 10 by running auto-scripts as described above.
In particular, follow [Step 3: Run experiments with auto-scripts](#autorun):
choose a database, set the workload to `twitter`, set `inst_level` to `no` for legacy systems and `local` for Cobra,
and get throughput latency results by running the script `report.py`.


#### Network cost and history sizes (Figure 11)

* Collect network traffic:
    * First make sure you can run `ifconfig` on the database machine, then change `eval/main.py` (line 19), set `nic_device` to your NIC name (for example, eno1).
    * Follow [Step 3: Run experiments with auto-scripts](#autorun): set the database to `postgres`, choose the workload you'd like to experiment, and and set `inst_level` to `no` for legacy systems and `local` for Cobra. 
    * Run `python eval/main.py recompile`
    * Each experiment's network costs will be stored as a file under folder `~/netstats/` in the DB's machine. 

* Calculate history size: you can measure the history size of a workload as follows.

    ```
    $ rm -r /tmp/cobra/ /tmp/rocksdb/
    $ mkdir -p /tmp/cobra/log
    $ cp config-historysize.yaml config.yaml
    $ vim config.yaml
    # update config.yaml and choose a workload you'd like to experiment
    
    $ java -ea -jar target/txnTest-1-jar-with-dependencies.jar local config.yaml
    ```

See [Cobra bench configuration](#config) for how to update `config.yaml` and specify workload parameters.
The history is stored under `/tmp/cobra/log/`, and you can sum file sizes of `/tmp/cobra/log/*.log` to calculate the total history size.

The above two experiments reproduce Figure 11 in Section 6.3.

<a name='config' /> Cobra bench configuration
---

Cobra bench uses a config file (for example, `$COBRA_HOME/CobraBench/config.yaml.default`) to specify parameters for an experiment.
Here are the important parameters and their possible values:

| parameters  |  meaning and values | 
|---|:---|
| `LIB_TYPE`  |  which database library is used by clients (which database to connect); `1` for Google Datastore, `2` for RocksDB, and `3` for PostgreSQL |
| `BENCH_TYPE`  | the benchmark to run; `0` for BlindW, `1` for TPC-C, `3` for C-RUBiS, and `4` for C-Twitter |
| `DB_URL`  |  the ip address of the remote database; see [Step 3: Run experiments with auto scripts](#autorun) |
|`REDIS_ADDRESS`| the ip address of Redis; see [Step 3: Run experiments with auto scripts](#autorun) |
|`TXN_NUM`| the size of the workload (number of transactions); should be an integer|
|`THREAD_NUM`| the number of clients; should be an integer|


<a name="cobrapaper" /> Reference
---

[1] Cheng Tan, Changgeng Zhao, Shuai Mu, and Michael Walfish. Cobra: Making Transactional Key-Value Stores Verifiably Serializable. OSDI 2020.

