#!/usr/bin/python -u

from config.config import config
from twitch_bot.bot import Roboraj

print 'About to read chat from twitch'
bot = Roboraj(config).run()

