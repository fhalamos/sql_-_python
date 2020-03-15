# Homework 8 Data Engineering

## Introduction

A typical data engineering project employs many of the elements we've learned about in class: matching records from disparate data sources, storing them in a normalized schema, and doing so in as parallel a process as possible. Parallizing independent tasks across machines or across computer cores is an excellent way of reducing latency and increasing throughput of an application.

Your assignment will be to build off the previous assignment to allow for parallelization in your matching algorithm. Here the inspection files are broken into A and B, that have 80% and 20% of the original data. You will run the prior cleaning process for A file. For the B file you will match records to your 
cleaned business records and add the new inspection data.  There are 100,10,and 2 percent samples for both A and B. These files are in Sf-RestAB.zip. **Note that B files do not have a header**

We'll be using an asynchronous I/O library called [ZeroMQ](http://zguide.zeromq.org/py:all) to perform this parallelization, by placing messages on a queue, processing fuzzy matching in parallel, then wrapping up any remaining serial tasks. The architecture used for this assignment is shown below.

<span style="display:block;text-align:center">![alt-text](https://github.com/imatix/zguide/raw/master/images/fig19.png "Task Ventilator Architecture")</span>

It begins with a _Ventilator_, which will be tasked with reading in data, e.g. bulk importing the raw-input, and then reading in potentially matching tuples from the raw-input. The _Ventilator_ will then send potentially matching tuple data to several _Workers_ to calculate whether the records match. These _Workers_ will alert a _Sink_ when they complete work on a potential pair, so that we can benchmark the end-to-end process. Once the *Ventilator* process puts all tasks on the queue for the Workers, it also has the *Sink* send a `KILL` signal to shut down the *Workers* once they've finished.

Your job will be to take the work you built last assignment, and to use the notion of a logical "queue" to split out expensive work to multiple processes. We have provided a set of "Enterprise Integration Patterns" or EIPs to help you structure this assignment. In `eip.py`, you will find a `Ventilator`, `Worker`, and `Sink` class. You will be responsible for enqueuing potentially matching records via the `Ventilator`, and processing matches/non-matches via the `Worker`. The `Sink`, which measures end-to-end time, also needs to run the `join_trips` method you build last week.

## Part 0: What You Need From Last Week

You will be reusing a few files from last week's assignment:

* `connection.py`
* `rest_inspection.py`
* `schemas.sql`

  
Copy these files over to the `hw8` folder.

## Part 1: The Data Vent (10 points)

The Ventilator has two methods, `vent()` and `tearDown()`. You will be responsible for sending a list of messages in the form of JSON serializable objects (e.g. Python Dictionaries) via the `vent()` method to the `Worker` instances, running separately.

The reason we "serialize" to JSON before placing messages on the queue is that JSON acts as a "data-interchange format". In many systems, one side of a queue may be written in Python, where the other side may be written in a completely different language. Using an intermediate standard like JSON allows for flexibility in engineering solutions. You will add the following process to `run.py`.

What you do before sending data to `vent()` is up-to-you, but a common workflow will look like:

1. Call the code to run on the A dataset 
2. Query batches of potentially matching records from the raw table in a B file. This should essentially be a part of your `clean_dirty_inspection()` implementation from last week.
3. Serialize the queried tuples to JSON, filling in the helper function on WL38 of eip.py
4. Vent the batch of JSON data to the Workers  

## Part 2: The Parallel Worker (10 points)

Each Worker will receive individual JSON messages you encode, containing potentially matching records, e.g.:

```{json}
    {
        "record1_business_name": ...
        "record1_zipcode": ...
        ...
        "record2_business_name": ...
        "record2_zipcode": ...
        ...
    }
```

Receiving messages is done for you on *Line 88* of `eip.py`, in the Worker class's `work()` method. Your job will be to take the remaining work from last week's `clean_dirty_inspection()` method and have the `work()` method execute it. The goal here is to perform all of the expensive calculations/queries from your function last week. Note that you have access to the DB Client on each of the eip classes.

To reiterate, inside of the `work()` method, you will have something like the following workflow:

1. Perform the remaining functionality from `clean_dirty_inspection()`.

## Part 3: The Sink (10 points)

The *Sink* will keep track of how many potential pairs have been processed, and whether there exist Workers with remaining work to do. Once all workers have wrapped up, the *Sink* should call the remaining serial functions required from the previous homework. You will have something like the following workflow:

1. Call `join_trips()` (implemented last week)

## Part 4: Benchmarking (10 points)

We have provided you with a shell script, `run.sh`, with the following usage:

* sh run.sh -w [NUMBER OF WORKERS] 

This shell script will create separate Python processes for venting, working, and sinking messages,and will create as many workers as you provide on the CLI. 

The goal here is to benchmark the following numbers of workers:

w = {1, 2, 4}

Place your output from these runs in a file called `out.txt` in the `hwfiles/` folder.

## Part 5: Write-Up (10 points)

Please talk briefly about your findings. Below are a few questions to guide you.

* Did the number of workers affect total runtime of the data ingestion process?
* Did workers linearly improve your processing time. If not, why do you think you saw this behavior?
* Are there other elements of this project you think would be worth parallelizing? Explain why.