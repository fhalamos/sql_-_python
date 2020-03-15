#!/bin/bash

while getopts w: option 
do 
 case "${option}" 
 in 
 w) WORKERS=${OPTARG};; 
 esac 
done 
 
echo "Running with # Workers: "$WORKERS

python run.py --sink &

for i in $(seq 1 1 $WORKERS)
do
    python run.py --worker &
done

python run.py --vent &