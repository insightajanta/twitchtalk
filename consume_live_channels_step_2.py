#!/usr/bin/python -u

from src.config.config import *
from src.kafka.consumer import LiveChannelConsumer

print 'About to start to consume chat'
LiveChannelConsumer(config).insert_data()

