#!/usr/bin/python -u

from src.config.config import *
from src.kafka.producer import LiveStreamProducer

print 'About to start to get current live stream details'
LiveStreamProducer(config).run()

