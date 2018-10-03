#!/usr/bin/python -u

from python.src.config.config import *
from python.src.kafka.producer import LiveStreamProducer

print 'About to start to get current live stream details'
LiveStreamProducer(config).run()

