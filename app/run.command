#!/bin/bash
cd "/Users/matthew.lucas/Documents/Activity Aggregator Update/app"
export PATH="/Users/matthew.lucas/.pyenv/shims:$PATH"
pip3 install -q -r requirements.txt 2>/dev/null
open "http://127.0.0.1:5050"
python3 app.py
