#!/bin/sh

for((i=1;i<=10;i++)); do
    ab -c100 -n100000 -k 'http://127.0.0.1:8080/ping'
done
