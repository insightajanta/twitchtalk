#!/usr/bin/python -u

from sys import argv

from src.twitch_bot.bot import *
from src.config.config import *
from src.kafka import *

#live_channel_producer = LiveChannelProducer(config).
print 'About to read chat from twitch'
bot = Roboraj(config).run()

