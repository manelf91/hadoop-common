#! /usr/bin/env/bash

command="ps aux | awk '/loadmgf/ {print \"sudo kill -9 \"\$2}' | bash"

parallel-ssh -h loaded_machines -l root $command
