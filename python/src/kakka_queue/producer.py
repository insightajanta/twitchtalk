from kafka import KafkaProducer
import msgpack
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

import json
import time
import redis

from twitch import TwitchClient
from config import config
from requests.exceptions import RequestException


class LiveStreamProducer:
    def __init__(self, config):
        self.config = config
        self.producer = KafkaProducer(bootstrap_servers=self.config['kafka_config'],
                                      value_serializer=msgpack.dumps,
                                      api_version=(0, 10, 1))
        self.client = TwitchClient(client_id=self.config['client_id'])
        self.redis = redis.Redis(host=self.config['redis_host'], port=6379)

    def get_top_live_channels(self):
        print "About to get live streams"

        try:
            streams = self.client.streams.get_live_streams(limit=100)

            count = 0
            channel_list = []
            for stream in streams:
                # print "In for loop for: "
                # print stream
                if stream['viewers'] >= 10000:
                    count = count + 1
                    channel_list.append(stream['channel']['name'])
                    self.producer.send('livechannels_new', json.dumps(stream, default=str).encode('utf8'))
                    # self.producer.send('livechannels', {'channel': stream['channel']['name'], 'viewers': stream['viewers']})

            # store the current list in redis
            self.redis.delete('__channels')
            self.redis.sadd('__channels', *channel_list[:10])

            print "Total streams inserted: ", count, channel_list
        except RequestException:
            print "Got exception!"


    def run(self):
        while True:
            self.get_top_live_channels()
            time.sleep(120)

if __name__ == '__main__':
    LiveStreamProducer(config).get_top_live_channels()
