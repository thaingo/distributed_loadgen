#/bin/bash

INTERVAL=1666;
MAX_OPS=24000;

valuesize=2048;
threads=16;
opcount=10000000
recordcount=10000000
dotransactions=true;
cur_ops=1666;

if [ $# -ne 1 ]; then
    echo "Invalid arguments: Must specify ip address as first and only argument";
    exit 0;
fi

HOST=$1

echo "Setting  host to 10.2.1.15";
sh ../lib/cli.sh set-value $HOST memcached.address 10.2.1.15;
sh ../lib/cli.sh set-value $HOST memcached.port 11210;
echo "Value size set to $valuesize"
sh ../lib/cli.sh set-value $HOST valuelength $valuesize;
echo "Setting number of threads to $threads";
sh ../lib/cli.sh set-value $HOST threadcount $threads;
echo "Setting number of operations to $opcount";
sh ../lib/cli.sh set-value $HOST operationcount $opcount;
echo "Setting number of records to $recordcount";
sh ../lib/cli.sh set-value $HOST recordcount $recordcount;
echo "Telling load generator to do transactions";
sh ../lib/cli.sh set-value $HOST dotransactions $dotransactions;
echo "Starting Load Generator";
sh ../lib/cli.sh run $HOST;

while [ $cur_ops -le $MAX_OPS ]; do
    sh ../lib/cli.sh set-value $HOST target $cur_ops;
    echo $cur_ops;
    sleep 15;
    cur_ops=$(($cur_ops + $INTERVAL)); 
done