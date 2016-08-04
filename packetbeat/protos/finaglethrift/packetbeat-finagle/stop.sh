#!/bin/bash

ps aux | grep 'packetbeat.yml' | grep -v 'grep' | awk '{print $2}' | xargs kill
