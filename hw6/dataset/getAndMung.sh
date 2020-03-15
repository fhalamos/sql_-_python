wget https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.csv
wget https://s3.amazonaws.com/fordgobike-data/201801-fordgobike-tripdata.csv.zip
wget https://s3.amazonaws.com/fordgobike-data/201802-fordgobike-tripdata.csv.zip
wget https://s3.amazonaws.com/fordgobike-data/201803-fordgobike-tripdata.csv.zip

unzip 201801-fordgobike-tripdata.csv.zip 
unzip 201802-fordgobike-tripdata.csv.zip
unzip 201803-fordgobike-tripdata.csv.zip
mv 201803_fordgobike_tripdata.csv 201803-fordgobike-tripdata.csv

cp 2017-fordgobike-tripdata.csv trip.csv
tail -n+2 201801-fordgobike-tripdata.csv | cut -d, -f16 --complement    >> trip.csv
tail -n+2 201802-fordgobike-tripdata.csv | cut -d, -f16 --complement  >> trip.csv
tail -n+2 201803-fordgobike-tripdata.csv | cut -d, -f16 --complement  >> trip.csv
