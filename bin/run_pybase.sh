#!/bin/bash

# You must provide Bootstrap servers (kafka nodes and their ports OR Zookeepers and the kafka ID of the chroot for your kafka instance
export ZOOKEEPERS="node1:2181,node2:2181,node3:2181"

# Either provide the ZK CHROOT of your Kafka Instance, or provide Bootstrap Servers
export KAFKA_ID="kafkainstance"
# OR
# export BOOTSTRAP_BROKERS="node1:9000,node2:9000"

# When registering a consumer group, do you want to start at the first data in the queue (earliest) or the last (latest)
export OFFSET_RESET="earliest"

# This is the name of the consumer group your client will create/join. If you are running multiple instances this is great, name them the same and Kafka will partition the info 
export GROUP_ID="mylogs_groups"

# The Topic to connect to
export TOPIC="mylogs"

# This is the loop timeout, so that when this time is reached, it will cause the loop to occur (checking for older files etc)
export LOOP_TIMEOUT="5.0"

# The next three items has to do with the cacheing of records. As this come off the kafka queue, we store them in a list to keep from making smallish writes and dataframes
# These are very small/conservative, you should be able to increase, but we need to do testing at volume

export ROWMAX=500 # Total max records cached. Regardless of size, once the number of records hits this number, the next record will cause a flush and write to parquet
export SIZEMAX=256000  # Total size of records. This is a rough running size of records in bytes. Once the total hits this size, the next record will cause a flush and write to parquet
export TIMEMAX=60  # seconds since last write to force a write # The number of seconds since the last flush. Once this has been met, the next record from KAfka will cause a flush and write. 

# MapR DB items
# This is where you want to write the maprdb table.  
export TABLE_BASE="/app/data/mytable"

export ROW_KEY_FIELD='id' # This is the field in your json you want to use as the hbase/maprdb row key

export FAMILY_MAPPING='cf1:col1,col2;cf2:col3,col4;cf3:col5,col6" # this is the mapping of fields to column families you want to use. 

export REMOVE_FIELDS_ON_FAIL=0 # Set to 1 to remove fields, starting left to right in the variable REMOVE_FIELDS to see if the json parsing works
export REMOVE_FIELDS="col1,col2,col3" # If REMOVE_FIELDS_ON_FAIL is set to 1, and there is an error, the parser will remove the data from fields left to right, once json conversion works, it will break and move on (so not all fields may be removed)

# Turn on verbose logging.  For Silence export DEBUG=0
export DEBUG=1

# Run Py ETL!
python3 -u /app/code/pybase.py

