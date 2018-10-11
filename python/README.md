# twitchchat

## Python packages to install

#####To use kafka from Python install this package
- sudo pip install kafka-python
#####To use json like format - here used for kafka ingestion
- sudo pip install msgpack
#####To use python-twitch-client
- sudo pip install python-twitch-client
- note you may need to install additional dependencies here.
I had to pip install the following on my macbook
    - six
    - requests
    - configparser
#####To use python cassandra
- sudo pip install cassandra-driver
    

## Kafka installation (local)
- Download kafka and unzip:
- Download from: https://www.apache.org/dyn/closer.cgi?path=/kafka/2.0.0/kafka_2.11-2.0.0.tgz
- Follow instructions: https://kafka.apache.org/quickstart

## Cassandra installation (local) version 3.11.3
Download from: http://www.apache.org/dyn/closer.lua/cassandra/3.11.3/apache-cassandra-3.11.3-bin.tar.gz

Follow instructions for Installing from binary tarball here: http://cassandra.apache.org/doc/latest/getting_started/installing.html

In cqlsh:
- create keyspace twitchspace;
- create keyspace twitchspace with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

For cassandra-spark connector:

Following instructions from: https://www.codementor.io/sheena/installing-cassandra-spark-linux-debian-ubuntu-14-du107vbhx

- git clone https://github.com/datastax/spark-cassandra-connector.git
- cd spark-cassandra-connector/
- git ch v2.3.2
- ./sbt/sbt assembly
- cp spark-cassandra-connector/target/full/scala-2.11/spark-cassandra-connector-assembly-2.3.2.jar ~
- spark-shell --jars ~/spark-cassandra-connector-assembly-2.3.2.jar --conf spark.cassandra.connection.host="10.0.0.7"

##Redis (local)
#####MacOS
- brew install redis (installed redis-4.0.11.high_sierra.bottle.tar.gz)
- brew services start redis
#####Ubuntu
- sudo pip install redis (Successfully installed redis-2.10.6)

#Dash
- pip install dash==0.27.0
- pip install dash-html-components==0.13.2
- pip install dash-core-components==0.30.2
