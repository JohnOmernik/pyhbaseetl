#!/usr/bin/python
from confluent_kafka import Consumer, KafkaError
import json
import re
from pychbase import Connection, Table, Batch
import time
import os
import sys

# Variables - Should be setable by arguments at some point

envvars = {}
# envars['var'] = ['default', 'True/False Required', 'str/int']
# Zookeeper Conf
envvars['zookeepers'] = ['', False, 'str']

# Kafka Conf
envvars['kafka_id'] = ['', False, 'str']
envvars['bootstrap_brokers'] = ['', False, 'str']
envvars['offset_reset'] = ['earliest', False, 'str']
envvars['group_id'] = ['', True, 'str']
envvars['topic'] = ['', True, 'str']
envvars['loop_timeout'] = ["5.0", False, 'flt']

# MapR DB Conf
envvars['table_base'] = ['', True, 'str']
envvars['uniq_env'] = ['HOSTNAME', False, 'str']
envvars['row_key_field'] = ['', True, 'str']
envvars['family_mapping'] = ['', True, 'str']

# Loop Caching
envvars['rowmax'] = [50, False, 'int'] # Setting rowmax to 1 will disable batching of data and send each record as it comes in. 
envvars['timemax'] = [60, False, 'int'] # If ROWMAX=1 this doesn't matter
envvars['sizemax'] = [256000, False, 'int'] # If ROWMAX+1 this doesn't matter

# Data Munging
envvars['remove_fields_on_fail'] = [0, False, 'int'] # If Json fails to import, should we try to remove_fields based on 'REMOVE_FIELDS' 
envvars['remove_fields'] = ['', False, 'str'] # Comma Sep list of fields to try to remove if failure on JSON import

# Debug
envvars['debug'] = [0, False, 'int']

loadedenv = {}


def main():

    global loadedenv
    loadedenv = loadenv(envvars)
    loadedenv['tmp_part'] = loadedenv['table_base'] + "/" + loadedenv['tmp_part_dir']
    loadedenv['uniq_val'] = os.environ[loadedenv['uniq_env']]
    if loadedenv['debug'] == 1:
        print json.dumps(loadedenv, sort_keys=True, indent=4, separators=(',', ': '))

    # Get the Boostrap brokers if it doesn't exist
    if loadedenv['bootstrap_brokers'] == "":
        if loadedenv['zookeepers'] == "":
            print"Must specify either Bootstrap servers via BOOTSTRAP_BROKERS or Zookeepers via ZOOKEEPERS"
            sys.exit(1)

        mybs = boostrap_from_zk(loadedenv['zookeepers'], loadedenv['kafka_id'])
    if loadedenv['debug'] >= 1:
        print mybs

    # Create Consumer group to listen on the topic specified
    c = Consumer({'bootstrap.servers': mybs, 'group.id': loadedenv['group_id'], 'default.topic.config': {'auto.offset.reset': loadedenv['offset_reset']}})
    c.subscribe([loadedenv['topic']])

    # Initialize counters
    rowcnt = 0
    sizecnt = 0
    lastwrite = int(time.time()) - 1
    dataar = []

    # Listen for messages
    running = True
    while running:
        curtime = int(time.time())
        timedelta = curtime - lastwrite
        try:
            message = c.poll(timeout=loadedenv['loop_timeout'])
        except KeyboardInterrupt:
            print "\n\nExiting per User Request"
            c.close()
            sys.exit(0)
        if message == None:
            # No message was found but we still want to check our stuff
            pass
        elif not message.error():
            rowcnt += 1
            # This is a message let's add it to our queue
            try:
                # This may not be the best way to approach this.
                val = message.value().decode('ascii', errors='replace')
            except:
                print message.value()
                val = ""
            # Only write if we have a message
            if val != "":
                #Keep  Rough size count
                sizecnt += len(val)
                failedjson = 0
                try:
                    dataar.append(json.loads(val))
                except:
                    failedjson = 1
                    if loadedenv['remove_field_on_fail'] == 1:
                        print "JSON Error likely due to binary in request - per config remove_field_on_fail - we are removing the the following fields and trying again"
                        while failedjson == 1:
                            for f in loadedenv['remove_fields'].split(","):
                                try:
                                    print "Trying to remove: %s" + f
                                    dataar.append(json.loads(re.sub(b'"' + f + '":".+?","', b'"' + f + '":"","', message.value()).decode("ascii", errors='ignore')))
                                    failedjson = 0
                                    break
                                except:
                                    print "Still could not force into json even after dropping %s" % f

                    if loadedenv['debug'] >= 1 and failedjson == 1:
                        print ("JSON Error - Debug - Attempting to print")
                        print("Raw form kafka:")
                        try:
                            print(message.value())
                        except:
                            print("Raw message failed to print")
                        print("Ascii Decoded (Sent to json.dumps):")
                        try:
                            print(val)
                        except:
                            print("Ascii dump message failed to print")

        elif message.error().code() != KafkaError._PARTITION_EOF:
            print("MyError: " + message.error())
            running = False
            break


            # If our row count is over the max, our size is over the max, or time delta is over the max, write the group to the parquet.
        if (rowcnt >= loadedenv['rowmax'] or timedelta >= loadedenv['timemax'] or sizecnt >= loadedenv['sizemax']) and len(dataar) > 0:


            if loadedenv['debug'] >= 1:
                print "Write Dataframe to %s at %s records - Size: %s - Seconds since last write: %s" % (curfile, rowcnt, sizecnt, timedelta)



            parqar = []
            rowcnt = 0
            sizecnt = 0
            lastwrite = curtime


    c.close()

def loadenv(evars):
    print("Loading Environment Variables")
    lenv = {}
    for e in evars:
        try:
            val = os.environ[e.upper()]
        except:
            if evars[e][1] == True:
                print("ENV Variable %s is required and not provided - Exiting" % (e.upper()))
                sys.exit(1)
            else:
                print("ENV Variable %s not found, but not required, using default of '%s'" % (e.upper(), evars[e][0]))
                val = evars[e][0]
        if evars[e][2] == 'int':
            val = int(val)
        if evars[e][2] == 'flt':
            val = float(val)
        if evars[e][2] == 'bool':
            val=bool(val)
        lenv[e] = val


    return lenv


# Get our bootstrap string from zookeepers if provided
def boostrap_from_zk(ZKs, kafka_id):
    from kazoo.client import KazooClient
    zk = KazooClient(hosts=ZKs,read_only=True)
    zk.start()

    brokers = zk.get_children('/%s/brokers/ids' % kafka_id)
    BSs = ""
    for x in brokers:
        res = zk.get('/%s/brokers/ids/%s' % (kafka_id, x))
        dj = json.loads(res[0].decode('utf-8'))
        srv = "%s:%s" % (dj['host'], dj['port'])
        if BSs == "":
            BSs = srv
        else:
            BSs = BSs + "," + srv

    zk.stop()

    zk = None
    return BSs



if __name__ == "__main__":
    main()
