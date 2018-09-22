#!/usr/bin/python -u

from sys import argv

from src.kafka.producer import LiveStreamProducer
from src.twitch_bot.bot import *
from src.config.config import *
from src.kafka import *

print 'About to start to get current live stream details'
LiveStreamProducer(config).run()

