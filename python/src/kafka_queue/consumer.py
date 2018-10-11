import msgpack
import redis
from kafka import KafkaConsumer

from src.datastore.cassandra_store import PythonCassandraExample


class ChatMessageConsumer:
    def __init__(self, config):
        self.config = config

        # live channel consumer
        self.chat_consumer = KafkaConsumer(bootstrap_servers=self.config['kafka_config'],
                                           value_deserializer=msgpack.loads, api_version=(0, 10, 1))
        self.chat_consumer.subscribe(['chatmessage'])
        self.redis = redis.Redis(host=self.config['redis_host'], port=6379)

        # setup cassandra below
        self.cass = PythonCassandraExample.PythonCassandraExample(self.config)
        self.cass.createsession()
        self.cass.setlogger()
        self.cass.session.set_keyspace(self.config['cassandra_keyspace'])

    def run(self):
        for msg in self.chat_consumer:
            # print ("in for loop")
            username = msg.value['username']
            message = msg.value['message']
            if self.redis.get(message):
                print "Found duplicate message: " + message
            value = self.redis.get(username)
            if value is None:
                self.redis.set(username, str(msg.timestamp) + ':' + str(1))
            else:
                words = value.split(':')
                oldtime = long(words[0])
                oldcount = int(words[1])
                if msg.timestamp - oldtime < self.config['time_chunk']:
                    if oldcount < self.config['max_frequency']:
                        self.redis.set(username, words[0] + ':' + str(oldcount + 1))
                    else:
                        print "found an offender: " + username
                else:
                    self.redis.set(username, str(msg.timestamp) + ':' + str(1))
