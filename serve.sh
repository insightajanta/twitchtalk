#!/bin/bash

nohup python get_live_streams_step_1.py &> get_live_streams_step_1.log &
nohup python serve_step_2.py &> serve_step_2.log &
nohup python consume_live_channels_step_2.py &> consume_live_channels_step_2.log &
nohup python consume_chat_step_3.py &> consume_chat_step_3.log &
