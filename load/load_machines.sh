#! /usr/bin/env/bash

parallel-ssh -h loaded_machines "rm -r load; mkdir load"
parallel-scp -h loaded_machines -r .  ~/load
parallel-ssh -h loaded_machines "cd ~/load; bash loadmgf_main.sh"
