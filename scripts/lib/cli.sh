#/bin/bash
PORT=8182

function printhelp {
    echo "";
    echo "Commands:";
    echo "run";
    echo "    Starts generating load using the clusters parameter list";
    echo "stop";
    echo "    Stops execution of the load generator"
    echo "server-list";
    echo "    Prints a list of servers that are part of the cluster";
    echo "get-config";
    echo "    Prints the load generators configuration in json";
    echo "set-config";
    echo "    Not Implemented";
    echo "get-value [parameter]";
    echo "    Gets the value for the parameter specified as the argument";
    echo "set-value [parameter] [value]";
    echo "    Sets a parameter with a given value in the load generator";
    echo "help";
    echo "    Prints help message";
}

if [ $# -lt 2 ]; then
    echo "Must specify a command and host";
    printhelp;
    exit 0;
fi

COMMAND=$1
HOST=$2

if [ "run" == "$COMMAND" ]; then
    curl "http://$HOST:$PORT/cluster/run";
elif [ "stop" == "$COMMAND" ]; then
    curl "http://$HOST:$PORT/cluster/stop";
elif [ "server-list" == "$COMMAND" ]; then
    curl "http://$HOST:$PORT/cluster/server-list";
elif [ "get-config" == "$COMMAND" ]; then
    curl "http://$HOST:$PORT/cluster/get-config";
elif [ "set-config" == "$COMMAND" ]; then
    echo "NOT IMPLEMENTED";
elif [ "get-value" == "$COMMAND" ]; then
    if [ $# -lt 3 ]; then
	echo "Must specify a parameter to get";
	exit 0;
    fi
    curl "http://$HOST:$PORT/cluster/get-value?name=$3";
elif [ "set-value" == "$COMMAND" ]; then
    if [ $# -lt 4 ]; then
	echo "Must specify a parameter and value to set";
	exit 0;
    fi
    curl "http://$HOST:$PORT/cluster/set-value?name=$3&value=$4";
elif [ "set-config" == "$COMMAND" ]; then
    printhelp;
else
    echo "Command $1 not valid";
    printhelp;
fi

