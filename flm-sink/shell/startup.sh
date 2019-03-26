#!/bin/bash

export FLUME_HOME="/opt/flume-apilog/apache-flume-1.7.0-bin"

kill $(cat pid)
sleep 2
cd apache-flume-1.7.0-bin
nohup bin/flume-ng agent -n agent -c conf -f conf/flume-conf.properties \
    -Dflume.monitoring.type=http \
    -Dflume.monitoring.port=34545 \
    -Dflume.root.logger=INFO,console > ../output 2>&1 &
echo $! > ../pid
tail -f ../output
