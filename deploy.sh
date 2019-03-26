#!/usr/bin/env bash

user="xxx"
host="x.x.x.x"

if [[ $1 == "sink" ]]; then
    path="/opt/flume-apilog/apache-flume-1.7.0-bin/lib"
    to_server="$user@$host:$path"

    echo "目标地址: $to_server"

    mvn clean install -Dmaven.test.skip=true | grep "\[INFO\] BUILD"

    jar=`ls flm-sink/target/ | grep "^flm-sink-\d.\d-\w\+.jar$" | tail -1`
    scp "flm-sink/target/$jar" "$to_server/flm-sink.jar"
fi

if [[ $1 == "sh" ]]; then
    scp flm-sink/shell/*.sh "$user@$host:/opt/flume-apilog/"
fi

if [[ $1 == "t" ]]; then
    path="/opt/flume-apilog/test-helper"
    to_server="$user@$host:$path"

    echo "目标地址: $to_server"

    mvn clean install -Dmaven.test.skip=true | grep "\[INFO\] BUILD"

    jar=`ls flm-test-helper/target/ | grep "^flm-test-helper-\d.\d-\w\+.jar$" | tail -1`
    scp "flm-test-helper/target/$jar" "$to_server"
fi
