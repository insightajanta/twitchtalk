#!/usr/bin/python -u

from src.config.config import *
from src.twitch_bot.bot import *

print 'About to read chat from twitch'
bot = Roboraj(config).run()

