#DO NOT MODIFY THIS FILE
import SFBikeDBClient
from hdrh import histogram
import random
import sys, traceback
import argparse
import logging
import os.path
from datetime import datetime
from datetime import timedelta
from timeit import default_timer as timer

# logging
logger= logging.getLogger('sfbikedb')
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
fh = logging.FileHandler('sfbike.log')
logger.addHandler(ch)
logger.addHandler(fh)


# FYI These are the debug levels
# logger.debug('debug message')
# logger.info('info message')
# logger.warn('warn message')
# logger.error('error message')
# logger.critical('critical message')

LOAD = "LoadTiming"


# Main funcion. You should not need to modify this function at all
# It invokes various functions from the client based on argument paraments 
def run_sf_bikedb(args):
    logger.info("Staring SFBikeDB for file %s" % args.load_file)
    if args.debug:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug Mode")
    loadHists = {}

    # Create tables and optionally create indexes
    try:
        if args.load == 'bulk':
            db_client = SFBikeDBClient.Client(args.override, args.user_name)
        else:
            logger.warning('Creating client with load batch size set to %s' % args.load)
            db_client = SFBikeDBClient.Client(args.override, args.user_name, int(args.load))
        db_client.open_connection()
        db_client.create_tables()
        if args.index == 'pre':
            logger.warning('Adding indexes pre')
            db_client.add_indexes()
        db_client.close_connection()
    except:
        logger.error("Error: %s" % sys.exc_info()[0])
        traceback.print_exc()      
    
    # Load data (skip, bulk, or record at a time)
    if args.skipload:
        logger.warning("==>Skipping load phase")
    else:
        if not os.path.isfile(args.load_file):
            logger.error("Load file does not exist %s exiting" % (args.load_file))
            sys.exit()
        try:
            db_client.open_connection()
            if args.load == 'bulk':
                load_hist = bulk_load_initial_data(args.load_file, db_client)
            else:
                load_hist = load_initial_data(args.load_file, db_client, args.limit_load)

            logger.info("Load Times. Index = %s" % args.index)
            logger.info(get_stat_string('Loading Records', load_hist))
        except:
            logger.error("Error: %s" % sys.exc_info()[0])
            traceback.print_exc()
        finally:
            db_client.close_connection()

    # Optional add indexes after loading
    if args.index == 'post':
        db_client.open_connection()
        logger.warning('Adding indexes post')
        start = timer()
        db_client.add_indexes()
        end = timer()
        db_client.close_connection()
        logger.info("Post-load index creation time %4.2f(ms)" % ((end-start)*1000))

    # Analyze phase (queries)
    if args.skipanalyze:
        logger.warning("==> Skipping analyze phase")
    else:
        try:
            db_client.open_connection()
            analyze_hists = analyze_data(db_client, args)
            logger.info("Analyze Times. Index = %s" % args.index)
            for key in analyze_hists.keys():
                logger.info(get_stat_string(key, analyze_hists[key]))

        except:
            logger.error("Error: %s" % sys.exc_info()[0])
            traceback.print_exc()
        finally:
            db_client.close_connection()


def get_stat_string(key, hist):
    if hist.get_total_count() == 0:
        d = {50:0,95:0,99:0,100:0}
    else:
        d = hist.get_percentile_to_value_dict([50,95,99,100])

    return "%40s -  Latency Perecentiles(ms) - 50th:%4.2f, 95th:%4.2f, 99th:%4.2f, 100th:%4.2f - Count:%s" % (key, d[50],d[95],d[99],d[100], hist.get_total_count() )


def print_stats(op_hists, load_hists):
    logger.info("Load Operation Times")
    print(load_hists)
    for key in load_hists.keys():
        logger.info(get_stat_string(key, load_hists[key]))
    logger.info("Operation Times")
    for key in op_hists.keys():
        logger.info(get_stat_string(key, op_hists[key]))


# from http://stackoverflow.com/questions/2130016/splitting-a-list-of-arbitrary-size-into-only-roughly-n-equal-parts
def chunk_list(seq, num):
    out = []
    try:
        avg = len(seq) / float(num)
        last = 0.0
        while last < len(seq):
            out.append(seq[int(last):int(last + avg)])
            last += avg
    except:
        pass
    return out


def get_hist():
    return histogram.HdrHistogram(1,1000*60*60,2)


def bulk_load_initial_data(bike_file, db_client):
    logger.info("Bulk Loading Initial Data -- IGNORING LIMIT")
    loadc = get_hist()
    s = datetime.now()
    db_client.bulk_load_file(bike_file)
    e = datetime.now()
    time = e - s
    loadc.record_value(time.total_seconds() * 1000)
    return loadc


def load_initial_data(bike_file, db_client, limit):
    logger.info("Loading Initial File: %s Data Limit:%s" % (bike_file,limit))
    loadc = get_hist()

    count = 0
    for line in open(bike_file, 'r'):
        if limit is not None and count >= limit:
            logger.info(" --> Breaking load early")
            return loadc
        s = datetime.now()
        db_client.load_record(line)
        e = datetime.now()
        time = e - s
        loadc.record_value(time.total_seconds() * 1000)
        count += 1
    return loadc


# get 5 minutes to 3 days of time in the years inclusive
def gen_random_dates(min_year, max_year):
    year = random.randint(min_year, max_year)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour = random.randint(0, 23)
    minute = random.randint(0, 59)
    d1 = datetime(year, month, day, hour, minute)
    d2 = d1 + timedelta(seconds=random.randint(300, 259200))
    return d1, d2


def gen_op_list(number, min_year, max_year):
    ops = []
    for i in range(number):
        op = random.choice(SFBikeDBClient.TYPES)
        if op == SFBikeDBClient.TYPES[0] or op == SFBikeDBClient.TYPES[1]:
            d1, d2 = gen_random_dates(min_year, max_year)
            ops.append((op, d1, d2))
        elif op == SFBikeDBClient.TYPES[2]:
            d1 = random.randint(200,50000)
            d2 = d1 + random.randint(30,600)
            ops.append((op, d1, d2))
        elif op == SFBikeDBClient.TYPES[3]:
            ops.append((op, random.randint(1955,1999),None))
    return ops


def analyze_data(db_client, args):
    logger.info("Analyzing Data")
    ahists = {}
    ops = gen_op_list(args.limit_ops, args.min_year, args.max_year)
    for i in ops:
        s = datetime.now()
        db_client.query_db(i[0], i[1], i[2])
        e = datetime.now()
        time = e - s
        if i[0] not in ahists:
            ahists[i[0]] = get_hist()
        ahists[i[0]].record_value(time.total_seconds() * 1000)

    return ahists


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Do a simple lobbyist benchmark application. Phases in order are Load, Ops, Analyze')
    parser.add_argument('--processes', dest='procs', type=int, default=1,
                       help='The number of parallel processes to do read and write operations')

    parser.add_argument('--limit_ops', help='Limit the number of operations (use only for testing)', type=int, dest='limit_ops', default=100)
    parser.add_argument('--limit_load', help='Limit the number of records to load (use only for testing)', type=int, dest='limit_load', default=None)
    parser.add_argument('--index', help='Must be none, pre, or post. Create indexes before loading data (pre), after loading data (post), or no indexes (none)', choices=['none', 'pre', 'post'], dest='index', default='none')
    parser.add_argument('--load', help='Must be bulk, 1, 10, 100, or 1000. How to load records, either bulk (copy) or batching load statements into transactions (eg 10 records batched)', choices=['bulk', '1', '10', '100', '1000'], dest='load', default=1)
    parser.add_argument('--skipload', action='store_true', help='Skip the loading phase')
    parser.add_argument('--skipops', action='store_true', help='Skip the operation phase')
    parser.add_argument('--skipanalyze', action='store_true', help='Skip the analyze phase')
    parser.add_argument('--debug', action='store_true', help='Run in debug mode')
    parser.add_argument('--override', action='store_true', help='Override Client db connection parameters (for grading only)')
    parser.add_argument('--load_file', help='The csv file to load from',default='dataset/201803_fordgobike_tripdata.csv')
    parser.add_argument('--min_year', help='Min Year for Loaded Data', type=int, dest='min_year', default=2018)
    parser.add_argument('--max_year', help='Max Year for Loaded Data', type=int, dest='max_year', default=2018)
    parser.add_argument('--user', help='Specify a user name to overide the database name and user', dest='user_name', default=None)
    

    args = parser.parse_args()
    run_sf_bikedb(args)
