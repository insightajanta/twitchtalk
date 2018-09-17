global config

config = {

    # details required to login to twitch IRC server
    'server': 'irc.twitch.tv',
    'port': 6667,
    'username': '<twitch-username>',
    'oauth_password': 'oauth:', # get this from http://twitchapps.com/tmi/

    # if set to true will display any data received
    'debug': False,

    # if set to true will log all messages from all channels
    # todo
    'log_messages': False,

    # maximum amount of bytes to receive from socket - 1024-4096 recommended
    'socket_buffer_size': 2048
}