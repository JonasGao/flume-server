#!/bin/bash

kill $(cat pid)
sleep 2
cd apache-flume-1.7.0-bin
nohup bin/flume-ng agent -n agent -c conf -f conf/flume-test-conf.properties -Dflume.root.logger=DEBUG,console > ../output 2>&1 &
echo $! > ../pid
tail -f ../output
