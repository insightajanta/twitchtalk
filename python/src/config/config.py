global config

config = {

    # details required to login to twitch IRC server
    'server': 'irc.twitch.tv',
    'port': 6667,
    'username': 'newlovetwitch',
    'oauth_password': 'oauth:kps8tqsz83f787weeu1hda1gaigkin', # 'oauth:awvu6op6a7vve5pha0l75rxg95l2dk', # get this from http://twitchapps.com/tmi/
    'client_id': 'b7a2xg2ev371062n79zm10rnsg238q',#'r96iwgijx7bhoypndzy6cejofpmwo5',

    # channel to join
    'channels': ['#loltyler1'],
    #'channels': ['#asmongold', '#speedgaming2', '#sco'],

    # if set to true will display any data received
    'debug': True,

    # if set to true will log all messages from all channels
    # todo
    'log_messages': True,

    # maximum amount of bytes to receive from socket - 1024-4096 recommended
    'socket_buffer_size': 2048,

    # kafka config
    'kafka_config': ['localhost:9092'],

    'zk_config': 'localhost:2181',

    # Cassandra settings
    'cassandra_host': ['localhost'],

    'cassandra_keyspace': 'twitchspace',

    'redis_host': 'localhost',

    # Settings for app
    'max_frequency': 5,
    'time_chunk': 6000 # in millis
}

