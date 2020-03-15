import sys, traceback
import argparse
import eip
import logging
import rest_inspection

# logging
logger = logging.getLogger('sfinspect')

def run(args):
    try:
        client = rest_inspection.Client()
        client.open_connection()

        if args.vent:
            ventilator = eip.Ventilator(client)

            # Load raw TSV data of A to DB
            # TODO <Your code goes here>

            # Query batches of B potentially matching records
            # TODO <Your code goes here>

            # Vent potential matches to workers
            # TODO <Your code goes here>
            
            # Tear down the Ventilator once all messages are sent
            # TODO <Your code goes here>

        elif args.worker:
            worker = eip.Worker(client)
            worker.work()
        elif args.sink:
            sink = eip.Sink(client)
            sink.sink()
    finally:
        client.close_connection()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run a ventilator, worker, or sync node')

    parser.add_argument('--vent', action='store_true', help='Run node as Ventilator')
    parser.add_argument('--worker', action='store_true', help='Run node as Worker')
    parser.add_argument('--sink', action='store_true', help='Run node as Sink')
    
    args = parser.parse_args()
    run(args)
