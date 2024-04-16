#!/bin/bash
ps -ef | grep start_rpc_server | grep -v grep | awk '{print $2}' | xargs kill -9 1>/dev/null 2>&1
ps -ef | grep GraphServer | grep -v grep | awk '{print $2}' | xargs kill -9 1>/dev/null 2>&1
