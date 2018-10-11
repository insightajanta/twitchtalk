import logging
from datetime import datetime

import redis
from src.config.config import config
from src.datastore.cassandra_store import PythonCassandraExample
from twitch import TwitchClient


# This class uses Twitch REST API (via python package TwitchClient) to get current top live channels
class LiveChannelProcessor(object):
    def __init__(self, config):
        self.config = config
        self.log = logging.getLogger()

        # setup cassandra below
        self.cass = PythonCassandraExample(self.config)
        self.cass.createsession()
        self.cass.setlogger()
        self.cass.session.set_keyspace(self.config['cassandra_keyspace'])
        self.client = TwitchClient(client_id=self.config['client_id'])
        self.redis = redis.Redis(host=self.config['redis_host'], port=6379)

    def get_top_live_channels(self):
        if config['debug']:
            print "About to get live streams"

        try:
            streams = self.client.streams.get_live_streams(limit=100)
            insert_sql = self.prepare_insert_sql()
            count = 0
            channel_list = []
            now = datetime.now()

            for stream in streams:
                if stream['viewers'] >= 10000:
                    count = count + 1
                    channel_list.append(stream['channel']['name'])
                    self.execute_insert(now, insert_sql, stream)

            # store the current list in redis for the chat processor to get it
            self.redis.delete('__channels')
            self.redis.sadd('__channels', *channel_list[:10])
            self.log.info("Total streams inserted: " + str(count) + str(channel_list))
        except: # yes, too broad, just keeping it here for now to know from the logs if an Exception is ever thrown
            self.log.erro("Got exception! Igonoring")

    def prepare_insert_sql(self):
        return self.cass.session.prepare("INSERT INTO live_channel ("
                                         "hours,"
                                         "minutes,"
                                         "ts,"
                                         "dt,"
                                         "broadcast_platform,"
                                         "created_at,"
                                         "game,"
                                         "status,"
                                         "updated_at,"
                                         "privacy_options_enabled,"
                                         "logo,"
                                         "partner,"
                                         "display_name,"
                                         "followers,"
                                         "broadcaster_language,"
                                         "private_video,"
                                         "description,"
                                         "views,"
                                         "channel,"
                                         "language,"
                                         "mature,"
                                         "average_fps,"
                                         "id,"
                                         "viewers) "
                                         "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

    def execute_insert(self, now, insert_sql, stream):
        vals = [now.hour,
                now.minute,
                now,
                now.strftime("%Y-%m-%d"),
                stream['broadcast_platform'],
                stream['created_at'],
                stream['game'],
                stream['channel']['status'],
                stream['channel']['updated_at'],
                stream['channel']['privacy_options_enabled'],
                stream['channel']['logo'],
                stream['channel']['partner'],
                stream['channel']['display_name'],
                stream['channel']['followers'],
                stream['channel']['broadcaster_language'],
                stream['channel']['private_video'],
                stream['channel']['description'],
                stream['channel']['views'],
                stream['channel']['name'],
                stream['channel']['language'],
                stream['channel']['mature'],
                stream['average_fps'],
                str(stream['id']),
                stream['viewers']]
        print "about to insert: " + str(insert_sql) + str(vals)
        self.cass.session.execute(insert_sql, vals)


if __name__ == "__main__":
    LiveChannelProcessor(config).get_top_live_channels()
