#!/bin/bash -ex

psql -U postgres  -f create_subreddit_list.sql
pushd /tmp
sudo sed -i '/^u\_/d' subreddit_list.csv
sudo mv subreddit_list.csv ~/dash_setup
popd
python3 create_subreddit_list.py
