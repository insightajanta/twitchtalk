import socket, re, time, sys, redis
from functions_general import *


class irc:

    def __init__(self, config):
        self.config = config
        self.channels = set()
        self.redis = redis.Redis(host='localhost', port=6379)

    def check_for_message(self, data):
        if re.match(
                r'^:[a-zA-Z0-9_]+\![a-zA-Z0-9_]+@[a-zA-Z0-9_]+(\.tmi\.twitch\.tv|\.testserver\.local) PRIVMSG #[a-zA-Z0-9_]+ :.+$',
                data):
            return True

    def check_is_command(self, message, valid_commands):
        for command in valid_commands:
            if command == message:
                return True

    def check_for_connected(self, data):
        if re.match(r'^:.+ 001 .+ :connected to TMI$', data):
            return True

    def check_for_ping(self, data):
        if data[:4] == "PING":
            self.sock.send('PONG')

    def get_message(self, data):
        return {
            'channel': re.findall(r'^:.+\![a-zA-Z0-9_]+@[a-zA-Z0-9_]+.+ PRIVMSG (.*?) :', data)[0],
            'username': re.findall(r'^:([a-zA-Z0-9_]+)\!', data)[0],
            'message': re.findall(r'PRIVMSG #[a-zA-Z0-9_]+ :(.+)', data)[0].decode('utf8')
        }

    def check_login_status(self, data):
        if re.match(r'^:(testserver\.local|tmi\.twitch\.tv) NOTICE \* :Login unsuccessful\r\n$', data):
            return False
        else:
            return True

    def send_message(self, channel, message):
        self.sock.send('PRIVMSG %s :%s\n' % (channel, message.encode('utf-8')))

    def get_irc_socket_object(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10)

        self.sock = sock

        try:
            sock.connect((self.config['server'], self.config['port']))
        except:
            pp('Cannot connect to server (%s:%s).' % (self.config['server'], self.config['port']), 'error')
            sys.exit()

        sock.settimeout(None)

        sock.send('USER %s\r\n' % self.config['username'])
        sock.send('PASS %s\r\n' % self.config['oauth_password'])
        sock.send('NICK %s\r\n' % self.config['username'])

        if self.check_login_status(sock.recv(1024)):
            pp('Login successful.')
        else:
            pp('Login unsuccessful. (hint: make sure your oauth token is set in self.config/self.config.py).', 'error')
            sys.exit()

        self.leave_and_join()

        return sock

    def channels_to_string(self, channel_list):
        hash_prepended = list(map(lambda x: '#'+x, channel_list))
        return ','.join(hash_prepended)

    def join_channels(self, channels):
        pp('Joining channels %s.' % channels)
        self.channels = channels
        self.sock.send('JOIN %s\r\n' % channels)
        pp('Joined channels.')

    def leave_channels(self, channels):
        pp('Leaving chanels %s,' % channels)
        self.sock.send('PART %s\r\n' % channels)
        pp('Left channels.')

    # added this method to take care of dynamically changing channel list
    def leave_and_join(self):
        new_channels = self.redis.smembers('__channels')
        # if channels is not set, then we just need to join
        if len(self.channels) == 0 and len(new_channels) > 0:
            print("about to join channels:" + str(len(new_channels)))
            print(list(new_channels))
            self.join_channels(self.channels_to_string(new_channels))
            self.channels = new_channels
        else:
            # remove old channels that are no longer popular and add new ones
            # some set magic
            common = self.channels & new_channels
            old_to_remove = self.channels - common
            new_to_add = new_channels - common
            if len(old_to_remove) > 0:
                print("about to leave channels:")
                print(old_to_remove)
                self.leave_channels(self.channels_to_string(old_to_remove))
                print("about to join channels:")
                print(new_to_add)
                self.join_channels(self.channels_to_string(new_to_add))
                self.channels = new_channels