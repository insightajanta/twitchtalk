"""
Simple IRC Bot for Twitch.tv

Developed by Aidan Thomson <aidraj0@gmail.com>
Adapted by Ajanta
"""

import json
from kafka import KafkaProducer
import irc as irc_


class Roboraj:

    def __init__(self, config):
        self.config = config
        self.irc = irc_.irc(config)
        self.socket = self.irc.get_irc_socket_object()
        self.chat_topic = KafkaProducer(bootstrap_servers=config['kafka_config'],
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
            print('Connection was lost, reconnecting.')
            self.irc.channels = set()
            self.socket = self.irc.get_irc_socket_object()

        if config['debug']:
            print data

        # check for ping, reply with pong
        irc.check_for_ping(data)

        if irc.check_for_message(data):
            message_dict = irc.get_message(data)
            self.chat_topic.send('chatmessage_new', str.encode(json.dumps(message_dict)))
