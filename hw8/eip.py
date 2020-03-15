# Task Enterprise Integration Patterns (Vent, Worker, Sink)
# Binds PUSH socket to tcp://localhost:5557
# Sends batch of tasks to workers via that socket
#
# Author: Lev Givon <lev(at)columbia(dot)edu>

import zmq
import time
import sys, traceback
import argparse

VENT_TO_WORKER = "5557"
TO_SINK = "5558"
SINK_TO_WORKER = "5559"

class Ventilator:

    def __init__(self, client):
        # DB Client
        self.client = client

        # Send messages to worker(s)
        self.context = zmq.Context()

        self.sender = self.context.socket(zmq.PUSH)
        self.sender.bind("tcp://*:" + VENT_TO_WORKER)

        # Send final record count to sink
        self.sink = self.context.socket(zmq.PUSH)
        self.sink.connect("tcp://localhost:" + TO_SINK)

        self.message_count = 0

        # A bug in ZMQ requires some sleep here
        # to allow the context to start up.
        time.sleep(2)

    def to_json(self, row):
        """Convert a returned row tuple into a JSON compatible dictionary."""
        # TODO <Your code here>
        pass

    def vent(self, messages):
        for message in messages:
            self.sender.send_json(message)
            self.message_count += 1

    def tearDown(self):
        self.sink.send_json({"final_count": self.message_count})
    
class Worker:

    def __init__(self, client):
        # DB Client
        self.client = client

        self.context = zmq.Context()

        # Socket to receive messages on
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect("tcp://localhost:" + VENT_TO_WORKER)

        # Socket to send messages to
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.connect("tcp://localhost:" + TO_SINK)

        # Socket for control input
        self.controller = self.context.socket(zmq.SUB)
        self.controller.connect("tcp://localhost:" + SINK_TO_WORKER)
        self.controller.setsockopt(zmq.SUBSCRIBE, b"")

        # Process messages from receiver and controller
        self.poller = zmq.Poller()
        self.poller.register(self.receiver, zmq.POLLIN)
        self.poller.register(self.controller, zmq.POLLIN)

    def work(self):
        # Process tasks until signaled to quit
        while True:
            socks = dict(self.poller.poll())

            # Quit signal received
            if zmq.POLLIN == socks.get(self.controller):
                break

            # Worker message received
            elif zmq.POLLIN == socks.get(self.receiver):
                message = self.receiver.recv_json()

                # Perform steps to clean record
                # TODO <Your code goes here>
                
                # Do not remove
                self.sender.send_json("match candidate processed.")
                
        self.receiver.close()
        self.sender.close()
        self.controller.close()
        self.context.term()

class Sink:

    def __init__(self, client):
        # DB Client
        self.client = client

        self.context = zmq.Context()

        # Socket to receive messages on
        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.bind("tcp://*:" + TO_SINK)

        # Socket for worker control
        self.controller = self.context.socket(zmq.PUB)
        self.controller.bind("tcp://*:" + SINK_TO_WORKER)

    def sink(self):
        sunk = 0
        sent = None
        vent_done = False
        t0 = None

        # Sink tasks until received a record for each ventilated record
        while True:
                if sent and sent == sunk:
                    break

                message = self.receiver.recv_json()
                if "final_count" in message:
                    sent = message["final_count"]
                    print(f"Vent sent {sent} messages (to sink).")
                else:
                    if sunk == 0:
                        t0 = time.time()
                    sunk += 1
 
        # Safe to shut down workers
        self.controller.send(b"")

        # Check Tables
        # TODO <Your code goes here>

        # Do not remove
        t1 = time.time()
        print(f"Time between first and last message arriving at sink: {t1-t0} seconds")