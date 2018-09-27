#!/usr/bin/python -u

from src.config.config import *
from src.kafka.consumer import ChatMessageConsumer

print 'About to start to consume chat'
ChatMessageConsumer(config).run()

