from kafka import producer
from kafka.errors import KafkaError
import msgpack
import PythonCassandraExample
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, BatchStatement
from cassandra.query import SimpleStatement

import json
import time

from twitch import TwitchClient


class LiveStreamProducer:

    def __init__(self, config):
        self.config = config
        self.producer = producer(bootstrap_servers=self.config.kafka_host + ":" + self.config.kafka_port,
                                 value_serializer=msgpack.dumps)
        self.client = TwitchClient(client_id=self.config.client_id)

    def get_top_live_channels(self):
        streams = self.client.streams.get_live_streams(limit=100)
        count = 0
        for stream in streams:
            if stream['viewers'] >= 10000:
                count = count + 1
                self.producer.send('livestreams', stream)
                self.producer.send('livechannels', {'channel': stream['channel']['name'], 'viewers': stream['viewers']})

        print "Total streams inserted: ", count

    def run(self):
        while True:
            self.producer.get_top_live_channels(self)
            time.sleep(60)
