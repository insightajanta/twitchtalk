#!/bin/bash

python get_live_streams_step_1.py &
python serve_step_2.py &
python consume_live_channels_step_2.py &
python consume_chat_step_3.py &
