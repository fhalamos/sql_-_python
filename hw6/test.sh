#!/bin/bash          
if [ -n "$1" ]; 
then
  LOAD="--load_file $1"
  echo "Load file $1"
else
  LOAD=""
  echo "Using default load file"
fi

echo "removing sfbike.log"
rm sfbike.log

# Data Loading
echo "Load Bulk Pre"
python3 driver.py --index=pre --load bulk --skipanalyze $LOAD
echo  "Load 1 Pre"
python3 driver.py --index=pre --load 1 --skipanalyze $LOAD
echo  "Load 10 Pre"
python3 driver.py --index=pre --load 10 --skipanalyze $LOAD
echo  "Load 100 Pre"
python3 driver.py --index=pre --load 100 --skipanalyze $LOAD
echo  "Load 1000 Pre"
python3 driver.py --index=pre --load 1000 --skipanalyze $LOAD
# Test index post loading
echo "Load Bulk Post"
python3 driver.py --index=post --load bulk --skipanalyze $LOAD
echo  "Load 1 Post"
python3 driver.py --index=post --load 1 --skipanalyze $LOAD
echo  "Load 100 Post"
python3 driver.py --index=post --load 100 --skipanalyze $LOAD
# Test Queries with and without index
echo "Query Bulk No"
python3 driver.py --index=none --load bulk $LOAD
echo "Query Bulk Post"
python3 driver.py --index=post --load bulk $LOAD


