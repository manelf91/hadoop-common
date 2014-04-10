#!/bin/bash

echo "loading for disk"
while true; do 
head -c 300000000 /dev/urandom | tee to_loadmgf_TT4| tee to_loadmgf_TT3| tee to_loadmgf_TT2 > to_loadmgf_TT
cat to_loadmgf* | grep "a" > /dev/null
rm -rf to_loadmgf_TT
rm -rf to_loadmgf_TT2
rm -rf to_loadmgf_TT3
rm -rf to_loadmgf_TT4
done
