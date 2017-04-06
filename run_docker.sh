#!/bin/bash

IMG="pyhbaseetl"

BIN="-v=`pwd`/bin:/app/bin:rw"
DATA="-v=/your/path/to/data:/app/data:rw"
MAPR="-v=/opt/mapr:/opt/mapr:ro"

CMD="/bin/bash"

# Alternatively, you can just set the environmental variables set in ./bin/run_pyetl.sh and then call /app/code/pyparq.py directly
echo "from pychbase import Connection, Table, Batch"
sudo docker run -it --rm $BIN $MAPR $DATA $IMG $CMD
