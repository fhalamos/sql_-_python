# Python to DB and Data Loading


In this assignment, you'll learn how interact with PostgeSQL 9.x via Python on an ubuntu/debian VM, and how to load and analyze data via a python interface/application to postgres. The data is from the Bay Area's bike share.

Full Datasets :
 - https://s3.amazonaws.com/fordgobike-data/2017-fordgobike-tripdata.csv
 - https://s3.amazonaws.com/fordgobike-data/201801-fordgobike-tripdata.csv.zip
 - https://s3.amazonaws.com/fordgobike-data/201802-fordgobike-tripdata.csv.zip
 - https://s3.amazonaws.com/fordgobike-data/201803-fordgobike-tripdata.csv.zip

One of the datasets is already in the dataset directory. Once you are confident that your code works, test with the first (full) dataset and report the results on that dataset.
The getAndMung.sh script should download all the files and put them together in a trip.csv

## Collaboration Policy

You can work with one other person on this assignment. However **both** partners are expected to understand all of the code and be able to run the code. We will sample students after the assignment to show us the running VM in their laptop. You should register as a team for this assignment on chisubmit.

## Project Description
 For this homework, you will complete a series of functions in the `SFBikeDBClient.py` file to connect to a database, create and drop tables, load data (both in bulk and transactionally), create indexes, and query the database.  In this assignment, the file `driver.py` will call the methods that you write in `SFBikeDBClient`. The `driver.py` file takes additional input parameters to restrict which methods are called. Methods will be invoked in the following order: `create_tables`, `add_indexes` (if `--index=pre`), `bulk_load_file` (for `--load=bulk`) or `load_record` (repeatedly called with `--load=N`, with committing transactions every `n` records), `add_indexes` (if `--index=pre`), and then repeatedly `query_db`. 

 Your goal is to run and to measure the performance of loading and querying data. 
 For loading, you will measure the impact of bulk loading as compared to transactional loading (committing every n records), and the impact of creating indexes before and after data loading for each type of data loading. For querying, you will measure the impact of indexes on queries.

## To Run
 - `python3 driver.py`
 - See help on options to running the program `python driver.py --help`
 - Parameters that might help with debugging: `debug`, `limit_ops`, and `limit_load`
 - Note if you want to add output in `SFBikeDBClient.py` code, please use `logger.debug` and set the debug parameter. You can find more information on `logger` [here](https://docs.python.org/3/howto/logging.html) 

 
## Tasks

### Step 0
 - Log into your VM
 - You will use a pre-installed psql instance. You should have a username and a database created already with your CNetID. Test connecting to the database by running `psql`. **You should be able to connect to the database from the local server with no password.**
 - Use python's `psycopg2` to interact with the database [http://initd.org/psycopg/docs/](http://initd.org/psycopg/docs/) 
 - Unzip the trip data file (do not check in the unzipped to git.) The file type should be ignored via .gitignore
 - Specify the database name and username as your CNETID in your SFBikeDBClient file. *do not use a password -- it should work*
 - *DEPRECATED* install, configure, and verify postgres(psql) of you want to run postgres on your own machine : see [psql.md](psql.md). This step is only listed in case you want to install a database on your own machine/server. Create the database and user using the prior instructions. 


### Step 1
 - Design a database to satisfy the data requirements primarily based on the function calls in the SFBikeDBClient (the full dataset has more than what we use). Look at both the function calls in SFBikeDBClient, as well as the schema provided in the "load data functions" sections of SFBikeDBClient, to help you. You do not need to modify driver.py 
 - Create the physical schema with a set of DDL statements in the `create_tables` call in the SFBikeDBClient.  *Do not optimize the schema at all outside of primary keys*, for bulk loading you might need to create a table that is not used by the query or indexing.
 - You should normalize this design by avoiding duplication of data (e.g. if there is a key for a logical object, do not repeat values for this key).
 - To do this task, you may have to add additional attributes to the constructor for the client class as well as code within other class methods. 


### Step 2
 - Implement the functions in `SFBikeDBClient.py`, including the `add_index` function and the loading functions. The instructions for each function are included as comments in the `SFBikeDBClient.py` file.  You will need to at least 2 indexes that improve query peformance.
 - Run the full database driver `$ python3 driver.py`

### Step 3
 - You should run and evaluate the performance of you system using the parameters used in the test.sh script. Note you can run this script with a specified load file (eg. myfile.csv) `sh test.sh myfile.csv` 
 - Copy the the sfbike.log to `hwfiles/out1.log` after your final run before submitting.
 - Regarding indexes, creating a primary key by default will create a B+ tree. You need to add two new indexes beyond primary keys that should improve the queries' performance/latency.
 - Briefly analyze the impact of indexes on queries and loading, and the impact of bulk loads and transactions on loading.



## Important Submission Note
You must make sure that any added files (`out1`, etc.) are added and committed in git. Submissions without these files will be graded as if you did not do this step!

### General Tips
The load function is not well-designed to map to an ideal DB. You may need to tease out what functions are updating and inserting which entities...

You typically need to create the database outside the program.

You should put ```drop table if exists [TABLENAME];``` statements in the start of your create table to clean up tables from an old setup (e.g., recreate a clean/fresh database).

If you do not understand the following Linux concepts, you may want to read up on them or come to office hours:
 - Linux users and permissions, and running `whoami` to see who i am logged in as
 - `ls`, `cd`, `mv`, `rm`, `-rf`, `mkdir`, `cp`
 - `su`   ( to switch users/ switch to root)
 - When you need to `sudo` and what `sudo` is
 - `CTRL D` and `CTRL C` to quit programs
 - How to write and quit in `vi` or `vim` (or how to install `nano`)
 - What an IP address and port are

It's generally a good idea to create a cursor for each query/function. Cursors are relatively lightweight, while connections are expensive and should be saved. See `psycopg` docs: [http://initd.org/psycopg/docs/faq.html#best-practices](http://initd.org/psycopg/docs/faq.html#best-practices)

Close connections when you know you are done or if your program will crash/halt.

When casting data types, you should wrap them in a `try, catch` block. If a bad string is passed, your program would crash with out this block.

Do not silently handle errors (eg do nothing in a catch block). At minimum log the error!

Multi-line SQL statements can be enclosed in `""" """`  (this is for long strings in Python).

You can disable operations for testing load operations only by passing  `--skipanalyze` to  `driver.py`. Run with --help to see all the phases you can skip.
 
Use best practices for interacting with a database via python (search online).
 
You will need to disable autocommit to get transactions to work explicitly, otherwise it is implicitly a transaction for each operation. 

Regarding transactions: Think about when you need to commit.  Note that the close connection is called after the data loading process, so if there are uncommitted inserts they should be committed at this point. If an update operation is expected by itself, you might need to commit.


### Suggested Plan for Getting Started
The homework is relatively modular and you can approach it from a variety of angles. I strongly suggest that implement a function or two at a time and verify using PSQL whenever possible. Here is one approach to get started
 - Implement open and close connection. This one is harder to verify by itself that it is working.
 - Implement create tables. Run `python3 driver.py --index none --skipload --skipanalyze`. Log into psql and verify that your tables are theyre. Rerun the code and ensure you tables are being dropped when you create tables.
 - Implement bulk load for a denormalized design (e.g. a flat table that mirrors the CSV file). Run `python3 driver.py --index none --load bulk --skipanalyze`. Log into psql and verify that records are there.
 - Implement add indexes. Run `python3 driver.py --index pre --load bulk --skipanalyze`. Log into psql and check that your tables have indexes by running \d [your_table_name] and see the indexes ae listed with the schema.
 - From here you could then extend your bulk load to split data into multiple tables, add record at a time loading (start with 1), or implement the analyze queries. 
