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

## Cassandra installation (local) version 3.11.3
download from: http://www.apache.org/dyn/closer.lua/cassandra/3.11.3/apache-cassandra-3.11.3-bin.tar.gz

Follow instructions for Installing from binary tarball here: http://cassandra.apache.org/doc/latest/getting_started/installing.html

In cqlsh:
- create keyspace testkeyspace;
- create keyspace testkeyspace with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };
- create table livechannels(ts text primary key, value text)

##Redis
brew install redis (installed redis-4.0.11.high_sierra.bottle.tar.gz)

brew services start redis

sudo pip install redis (Successfully installed redis-2.10.6)

#Docker steps
##Cassandra
(full instructions: https://docs.docker.com/samples/library/cassandra/)

docker run --name cassandra -d cassandra:3.11.3

To connect using cqlsh, do the following

docker run -it --link cassandra:cassandra --rm cassandra cqlsh cassandra
