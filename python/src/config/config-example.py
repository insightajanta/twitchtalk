global config

# rename this file to config.py to run the code
config = {

    # details required to login to twitch IRC server
    'server': 'irc.twitch.tv',
    'port': 6667,
    'username': 'twitch_username',
    'oauth_password': 'oauth:',  # get this from http://twitchapps.com/tmi/
    'client_id': '',  # get this from twitch as well

    # channel to join
    'channels': ['#loltyler1'],

    # if set to true will display any data received
    'debug': True,

    # if set to true will log all messages from all channels
    # todo
    'log_messages': True,

    # maximum amount of bytes to receive from socket - 1024-4096 recommended
    'socket_buffer_size': 2048,

    # kafka config - if multiple kafka hosts, have them comma separated here
    'kafka_config': ['localhost:9092'],

    # Cassandra settings - if multiple cassandra hosts, have them comma separated here
    'cassandra_host': ['localhost'],

    'cassandra_keyspace': 'twitchspace',

    'webapp_host': 'localhost',
    'webapp_port': '8050',

    'redis_host': 'localhost',

    # Settings for app
    'max_frequency': 5,  # this for detecting spammers, 5 messages in 1 minute
    'time_chunk': 6000   # this for detecting spammers, 5 messages in 1 minute in millis
}

