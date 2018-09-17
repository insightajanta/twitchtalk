# twitchchat

## Python packages to install
#####To use kafka from Python install this package
sudo pip install kafka-python
#####To use json like format - here used for kafka ingestion
sudo pip install msgpack
#####To use python-twitch-client
sudo pip install python-twitch-client
- note you may need to install additional dependencies here.
I had to pip install the following on my macbook
    - six
    - requests
    - configparser
#####To use python cassandra
sudo pip install cassandra-driver
    

## Kafka installation
download kafka and unzip:

download from: https://www.apache.org/dyn/closer.cgi?path=/kafka/2.0.0/kafka_2.11-2.0.0.tgz

follow instructions: https://kafka.apache.org/quickstart

## Cassandra installation (local)
download from: http://www.apache.org/dyn/closer.lua/cassandra/3.11.3/apache-cassandra-3.11.3-bin.tar.gz

Follow instructions for Installing from binary tarball here: http://cassandra.apache.org/doc/latest/getting_started/installing.html

In cqlsh:
- create keyspace testkeyspace;
- create table chatmessage(key int primary key, value text)
