#! /usr/bin/env/bash

while read host_ip host_name
do
  simple_name=`echo $host_name | cut -d \. -f 1`
  scp $host_ip:~/hadoop-common/logs/hadoop-mgferreira-tasktracker-$simple_name.out tmp_out
  `cat tmp_out | tail -n 7 >> indexes`
done < names_ips

