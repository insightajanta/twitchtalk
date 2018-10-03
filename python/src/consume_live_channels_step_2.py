#!/usr/bin/python -u

from python.src.config.config import *
from python.src.kafka.consumer import LiveChannelConsumer

print 'About to start to consume chat'
LiveChannelConsumer(config).insert_data()

