# Evaluation

## Configuration:

First add this to your `~/.ssh/config` and change [you] to your username.
```
Host ye
  Hostname ye-cheng.duckdns.org
  User [you]
  IdentityFile /Users/[you]/.ssh/id_rsa
Host yak
  Hostname yak-cheng.duckdns.org
  User [you]
  IdentityFile /Users/[you]/.ssh/id_rsa
Host boa
  Hostname boa-cheng.duckdns.org
  User [you]
  IdentityFile /Users/[you]/.ssh/id_rsa
```

Then use a python3.6 virtual environment and install these:
```sh
$ cd eval; pip install -r requirements.txt;
```

And you may want to do some changes to `../config.yaml`

## HOW TO

### run a client-server test
```sh
$ python fabfile.py
```
Or you can manually do:
- build docker image
- start the server
- start the client

### build Docker image
```sh
$ cd ..; mvn install;
$ fab -r eval/fabfile.py -H ye rebuild # ye is the server's nickname
```

### start/restart the server
```sh
$ fab -H ye restart-s
```

### start/restart the client
```sh
$ fab -H ye restart-c
```

### stop all
```sh
$ fab -H ye stop-all
```