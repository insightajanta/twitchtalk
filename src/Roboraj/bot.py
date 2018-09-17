"""
Simple IRC Bot for Twitch.tv

Developed by Aidan Thomson <aidraj0@gmail.com>
"""

import irc as irc_
import msgpack
from functions_general import *
from kafka import KafkaProducer
from kafka.errors import KafkaError


producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=msgpack.dumps)


class Roboraj:

    def __init__(self, config):
        self.config = config
        self.irc = irc_.irc(config)
        self.socket = self.irc.get_irc_socket_object()

    def run(self):
        irc = self.irc
        sock = self.socket
        config = self.config

        while True:
            data = sock.recv(config['socket_buffer_size']).rstrip()

            if len(data) == 0:
                pp('Connection was lost, reconnecting.')
                sock = self.irc.get_irc_socket_object()

            if config['debug']:
                print data

            # check for ping, reply with pong
            irc.check_for_ping(data)

            if irc.check_for_message(data):
                message_dict = irc.get_message(data)

                channel = message_dict['channel']
                message = message_dict['message']
                username = message_dict['username']

                producer.send('chatmessage', {' channel ': channel, ' username ': username, ' message ': message})

                ppi(channel, message, username)
