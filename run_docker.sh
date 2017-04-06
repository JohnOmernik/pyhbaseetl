#!/bin/bash

IMG="dockerregv2-shared.marathon.slave.mesos:5005/pyhbaseetl:1.0.0"

BIN="-v=`pwd`/bin:/app/bin:rw"
MAPR="-v=/opt/mapr:/opt/mapr:ro"

CMD="/bin/bash"

# Alternatively, you can just set the environmental variables set in ./bin/run_pyetl.sh and then call /app/code/pyparq.py directly
echo "from pychbase import Connection, Table, Batch"
sudo docker run -it --rm $BIN $MAPR $DATA $IMG $CMD
