# CobraBench

[Cobra](https://github.com/DBCobra) is a (research) system that checks serializability of a transaction history. 
CobraBench is the client side benchmark to generate logs for 
[CobraVerifier](https://github.com/DBCobra/CobraVerifier) and 
test the client-side overhead. 
[This paper](XXX) defines the problem and gives context. 

How to run CobraBench
---

## Step 0: Compile and quick run

Install mvn and java

    $ sudo apt install maven openjdk-8-jdk

Switch default Java: 

    $ # requrie Java1.8
    $ sudo update-alternatives --config java

Install dependency

    $ mvn install:install-file -Dfile=./include/tapir.jar -DgroupId=tapir\
      -DartifactId=tapir -Dversion=1.4.0 -Dpackaging=jar -DgeneratePom=true

Compile the code:

    $ mvn install

Now you can run a test for rocksdb manually:

    $ mkdir -p /tmp/cobra/log
    $ java -ea -jar target/txnTest-1-jar-with-dependencies.jar local config-historysize.yaml

You can change the config file for different tests. 

----

How to run the test scripts
---

The rest of this documents is about how to automatically run many tests on postgreSQl, 
Google Datastore and on multiple client machines. This might be annoying because it
uses multiple machines. 

## Step 1: Environment for auto-scripts:

> Important: don't put CobraBench folder directly under your home!
>  
> Note: sorry we will put some tempory files under the home folder of your machines
>
> You need no less than 2 machines to simulate the clients and the database for postgres. 

On the machine that you want to run the database and control the test:

### 1. ssh config

First add your public key to your local `.ssh/authorized_keys` to make sure you can run `ssh localhost` without using password. 

Then add these lines to your `~/.ssh/config` , change `[you]` to your username and change `[hostname]` to the machine's ip address. 

``` 
Host client1
	Hostname [hostname]
	User [you]
	IdentityFile [your id_rsa]
Host client2
	Hostname [hostname]
	User [you]
	IdentityFile [your id_rsa]
Host client3
	Hostname [hostname]
	User [you]
	IdentityFile [your id_rsa]
```

### 2. Install python

Install [Anaconda](https://www.anaconda.com/products/individual), then

``` 
$ conda create --name txn python=3.7.5
$ conda activate txn
$ which python
$ # /home/ubuntu/anaconda/envs/txn/bin/python
$ cd eval/
$ pip install -r requirements.txt
```

### 3. Install redis. We use redis to start multiple clients at the same time

``` 
$sudo apt install redis
$ pip install redis
$ sudo vim /etc/redis/redis.conf
# comment the line "bind 127.0.0.1 ::1"
# change the line from "protected-mode yes" to "protected-mode no"
$ sudo service redis-server restart
$ vim config.yaml.default
# REDIS_ADDRESS: replace the ip to this machine's ip
$ vim eval/main.py
# change redis_ip in line 17 to this machine's ip
$ QUIT
```

### 4. Setup docker

Install (change [username] to your remote username on the client machine):

``` 
$ fab -r eval/fabfile.py -H localhost install-docker --uname="[username]"
$ fab -r eval/fabfile.py -H client1 install-docker --uname="[username]"
$ fab -r eval/fabfile.py -H client2 install-docker --uname="[username]"
$ fab -r eval/fabfile.py -H client3 install-docker --uname="[username]"
```

Build docker image:

``` 
$ cp config.yaml.default config.yaml
$ fab -r eval/fabfile.py -H localhost rebuild
$ fab -r eval/fabfile.py -H client1 rebuild
$ fab -r eval/fabfile.py -H client2 rebuild
$ fab -r eval/fabfile.py -H client3 rebuild
```

## Step 2: PostgreSQL

### 1. Install:

    $ sudo apt install postgresql postgresql-contrib

Add one line in `/etc/postgresql/10/main/postgresql.conf` : 

```
listen_addresses = '*'
```

Add 2 lines at the end of the file `listen_addresses = '*'` :

``` 
host    all             all              0.0.0.0/0                       md5
host    all             all              ::/0                            md5
```

    $ sudo service postgresql restart

Set the IP address in DB_URL of `config.yaml.default` to your IP. 

### 2. create users

``` 
$ sudo -u postgres psql
$ create user cobra with password 'Cobra<318';
$ create user [yourusername];
$ alter user [yourusername] with superuser;
$ \q
```

## Step 3: Google Datastore (optional)

1. Create datastore in google
2. Download the credentials and save it in "cobra_key.json"
3. Please refer to [this link](https://cloud.google.com/datastore/docs/reference/libraries) for further instructions. 

## Step 4: Run benchmarks with auto scripts

### 1. Configuration

Edit `./eval/main.py` and set line19: `client_machine` to the list of clients' hostnames

### 2. Throughput and latency benchmark

Edit `eval/main.py` (line 269) to select the test:

``` python
run_one_series(database, workload, contention, inst_level)
# database: one of ['rocksdb', 'postgres', 'google']
# workload: one of ['cheng', 'twitter', 'rubis', 'tpcc', 'ycsb'] (cheng stands for BWrite-RW in the paper)
# contention: one of ['low', 'high']
# inst_level: one of ['no', 'local']
```

Run the script to start benchmark:

    $ python eval/main.py recompile

Then the program will be executed in a tmux session of each client machine. 
We will use localhost as client for `rocksdb` and `google` , while we use the clients
in the list to run `postgres` . 

The runtime log will be printed to `$HOME/client.txt` . 
If you run a series of evaluations, the logs will be copied to the folder `~/trials` with corresponding names. 

To collect numbers from the logs, you can run:

    $ cd eval
    $ ln -s ~/trials trials
    $ python report.py

It will generate csv files containing throughput and latency under `eval/data` 

## Step 5. Reproducing results:

### Figure 10:
Run benchmakrs as described above, set the workload to `twitter` , vary the database and inst_level. 

### Figure 11:

**Network traffic:**

Run benchmakrs as described above, set the database to `postgres` , vary the workload and inst_level. Results will be stored in `./netstats` . 

**History size:**: 

Run one trial manually:

    $ rm -rf /tmp/cobra || true
    $ mkdir -p /tmp/cobra/log
    $ cp config-historysize.yaml config.yaml # you can edit this file to select workload
    $ java -ea -jar target/txnTest-1-jar-with-dependencies.jar local

Then you can calculate the total size of `/tmp/cobra/log/*.log` 
