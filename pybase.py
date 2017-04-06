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
envvars['row_key_fields'] = ['', True, 'str']
envvars['row_key_delim'] = ['_', False, 'str']
envvars['family_mapping'] = ['', True, 'str']
envvars['create_table'] = [0, False, 'int']
envvars['bulk_enabled'] = [0, False, 'int']
envvars['print_drill_view'] = [0, False, 'int']
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


    table_schema = {}
    cf_schema = {}
    cf_lookup = {}
    for x in loadedenv['family_mapping'].split(";"):
        o = x.split(":")
        table_schema[o[0]] = o[1].split(",")
        cf_schema[o[0]] = {}
    for x in table_schema.iterkeys():
        for c in table_schema[x]:
            cf_lookup[c] = x
    myview = drill_view(table_schema)
    if loadedenv['debug'] >= 1 or loadedenv['print_drill_view'] == 1: 
        print "Drill Shell View:"
        print myview
    if loadedenv['print_drill_view'] == 1:
        sys.exit(0)


    if loadedenv['debug'] >= 1:
        print "Schema provided:"
        print table_schema
        print ""
        print "cf_lookip:"
        print cf_lookup


    connection = Connection()
    try:
        table = connection.table(loadedenv['table_base'])
    except:
        if loadedenv['create_table'] != 1:
            print "Table not found and create table not set to 1 - Cannot proceed"
            sys.exit(1)
        else:
            print "Table not found: Creating"
            connection.create_table(loadedenv['table_base'], cf_schema)
            try:
                table = connection.table(loadedenv['table_base'])
            except:
                print "Couldn't find table, tried to create, still can't find, exiting"
                sys.exit(1)
    
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
                val = message.value().decode('ascii', errors='ignore')
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
                    if loadedenv['remove_fields_on_fail'] == 1:
                        print "JSON Error likely due to binary in request - per config remove_field_on_fail - we are removing the the following fields and trying again"
                        while failedjson == 1:
                            for f in loadedenv['remove_fields'].split(","):
                                try:
                                    print "Trying to remove: %s" % f
                                    dataar.append(json.loads(re.sub(b'"' + f + '":".+?","', b'"' + f + '":"","', message.value()).decode("ascii", errors='ignore')))
                                    failedjson = 0
                                    break
                                except:
                                    print "Still could not force into json even after dropping %s" % f
                                    if loadedenv['debug'] == 1:
                                        print message.value().decode("ascii", errors='ignore')
                            if failedjson == 1:
                                failedjson = 2

                    if loadedenv['debug'] >= 1 and failedjson >= 1:
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
            if loadedenv['bulk_enabled'] == 1:

                batch = table.batch()

                for r in dataar:
                    batch.put(db_rowkey(r), db_row(r, cf_lookup))
#                    print "batch.put(%s, %s)" % (db_rowkey(r), db_row(r, cf_lookup))
                errors = batch.send()

                if errors == 0:
                    if loadedenv['debug'] >= 1:
                        print "Write Dataframe to %s at %s records - Size: %s - Seconds since last write: %s - NO ERRORS" % (loadedenv['table_base'], rowcnt, sizecnt, timedelta)
                else:
                    print "Multiple errors on write - Errors: %s" % errors
                    sys.exit(1)
            else:
                bcnt = 0
                for r in dataar:
                    bcnt += 1
                    try:
                        table.put(db_rowkey(r), db_row(r, cf_lookup))
                    except:
                        print "Failed on record with key: %s" % db_rowkey(r)
                        print db_row(r, cf_lookup)
                        sys.exit(1)
                if loadedenv['debug'] >= 1:
                    print "Pushed: %s rows" % rowcnt

            dataar = []
            rowcnt = 0
            sizecnt = 0
            lastwrite = curtime


    c.close()
def drill_view(tbl):
    tbl_name = loadedenv['table_base']

    out = "CREATE OR REPLACE VIEW MYVIEW_OF_DATA as \n"
    out = out + "select \n"
    for cf in tbl.iterkeys():
        for c in tbl[cf]:
            out = out + "CONVERT_FROM(t.`%s`.`%s`, 'UTF8') as `%s`, \n" % (cf, c, c)

    out = out[:-3] + "\n"
    out = out + "FROM `%s` t\n" % tbl_name
    return out


def db_rowkey(jrow):
    out = ""
    for x in loadedenv['row_key_fields'].split(","):
        v = ''
        if jrow[x] == None:
            v = ''
        else:
            try:
                v = str(jrow[x])
            except:
                print jrow
                sys.exit(1)

        if out == "":
            out = v
        else:
            out = out + loadedenv['row_key_delim'] + v
    return out

def db_row(jrow, cfl):
    out ={}
    for r in jrow:
        v = ''
        if jrow[r] == None:
            v = ''
        else:
            try:
                v = str(jrow[r])
            except:
                print "Field: %s" % r
                print jrow[r].decode('ascii', errors='ignore')
                print jrow
        out[cfl[r] + ":" + r] = v
    return out


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
