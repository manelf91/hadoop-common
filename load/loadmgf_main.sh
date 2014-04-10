#! /usr/bin/env/bash

for i in {1..2}
do
	screen -d -m ./loadmgf_disk.sh
done

for i in {1..4}
do
        screen -d -m ./loadmgf_proc.sh
done

