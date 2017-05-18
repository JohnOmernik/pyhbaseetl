# pyhbaseetl
Similar to PyETL but to Hbase/MapR DB 

## Generic ETL Kafka -> Hbase/MapRDB
More Documentation to come



## Using with Mapr Streams instead of Apache Kafka
- Using MapR's librdkakfa, it is possible to use this MapR Streams.
- Instructions:
  - Go to https://github.com/JohnOmernik/maprlibrdkafka clone and build that image
  - Instead of from Ubuntu, change to from the image you built from maprlibrdkafka
  - Comment out the line that builds the normal librdkafka
  - To use, specify "mapr" as your bootstrap brokers config value, and use the MapR format for topic (i.e. topic=/path/to/streams:topicname)


