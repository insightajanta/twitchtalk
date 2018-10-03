#!/usr/bin/python -u

from python.src.config.config import *
from python.src.kafka.consumer import ChatMessageConsumer

print 'About to start to consume chat'
ChatMessageConsumer(config).run()

