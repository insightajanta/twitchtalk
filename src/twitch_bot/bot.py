"""
Simple IRC Bot for Twitch.tv

Developed by Aidan Thomson <aidraj0@gmail.com>
"""

import irc as irc_
import msgpack
from functions_general import *
from kafka import KafkaProducer
from kafka.errors import KafkaError


class Roboraj:

    def __init__(self, config):
        self.config = config
        self.irc = irc_.irc(config)
        self.socket = self.irc.get_irc_socket_object()
        self.chat_topic = KafkaProducer(bootstrap_servers=config['kafka_config'],
                                        value_serializer=msgpack.dumps,
                                        api_version=(0, 10, 1))

    def run(self):
        t1 = time.time()-60
        t2 = time.time()
        while True:
            if t2 - t1 > 60:
                self.irc.leave_and_join()
                t1 = time.time()
            t2 = time.time()
            self.get_chat_message()

    def get_chat_message(self):
        irc = self.irc
        sock = self.socket
        config = self.config

        data = sock.recv(config['socket_buffer_size']).rstrip()

        if len(data) == 0:
            pp('Connection was lost, reconnecting.')
            self.irc.channels = set()
            self.socket = self.irc.get_irc_socket_object()

        if config['debug']:
            print data

        # check for ping, reply with pong
        irc.check_for_ping(data)

        if irc.check_for_message(data):
            message_dict = irc.get_message(data)

            channel = message_dict['channel']
            message = message_dict['message']
            username = message_dict['username']

            self.chat_topic.send('chatmessage', {'channel': channel, 'username': username, 'message': message})

            # ppi(channel, message, username)
