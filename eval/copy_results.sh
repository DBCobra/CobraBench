#!/usr/bin/env bash
mkdir -p trials;
for host in ye yak; do
    scp -r $host:~/trials/* ./trials/;
done;