"""Example producer service."""
import json
import string
import sys
import uuid
import random
import datetime
import confluent_kafka
import zlib
import os
import logging
from pythonjsonlogger import jsonlogger
import resource
import gc
import tracemalloc
import psutil

def memory_usage_psutil():
    # return the memory usage in MB
    import psutil
    process = psutil.Process(os.getpid())
    mem = process.memory_percent()
    return mem

class Producer:
    """Produce messages to Kafka.
    Arguments:
        logger (``object`` of ``logging``): Logger so we can log stuff inside the producer.
        hosts (``list`` of ``str``): Should be a list of hosts. Using the format of: {host}:{port}.
        topic (``str``): The name of the topic to push items to
        broker_version (``int.int.int)`` of str, optional): Define the Kafka
            API version
            *default:* 0.8.2.2
    """

    def __init__(self, logger, hosts, topic, broker_version="0.8.2.2"):
        """Construct a Producer."""
        self.logger = logger
        self.topic = topic
        self.producer = confluent_kafka.Producer({"bootstrap.servers": ",".join(hosts),
                                                  "queued.min.messages": 1000,
                                                  "queue.buffering.max.messages": 1000000,
                                                  "queue.buffering.max.ms": 1000,
                                                  "api.version.request": False,
                                                  "compression.type": "snappy",
                                                  "broker.version.fallback": broker_version})

    def delivery_report(self, err, msg):
        """Callback for delivery reports."""
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
        
    def push(self, *messages):
        """Push items to Kafka."""
        self.producer.poll(timeout=0)  # Trigger any available delivery report callbacks from previous produce() calls
        for message in messages:
            encoded_message = message.encode()
            produced_message = self.producer.produce(self.topic, encoded_message, callback=self.delivery_report)
        

def generate_post(key):
    """Generate a post."""
    output = {"value": "kafka-test"*100,
              "key": key,
              "correlationId": str(uuid.uuid4())}
    return output
    
class TestProducerService:
    """Test Producer Service."""

    def __init__(self, run_time_minutes=10):
        """Initialize kafka producer"""
        self.topic = "test-topic"
        self.log_level = "INFO"
        self.run_time_start = None
        self.run_time_end = None
        self.run_time_minutes = run_time_minutes
        self.got_sigterm_signal = False
        self.logger = None
        self.producer = None
        self.tracker = None
        self.setup_logger()  # Always create the logger first.
        self.setup_runtime_end()
        self.setup_kafka_producer()
        self.logger.info("setup-done", extra={"topic": self.topic, "run_time_start": self.run_time_start, "run_time_end": self.run_time_end, "run_time_minutes": self.run_time_minutes, "log_level": self.log_level})
    
    def setup_runtime_end(self):
        """Set up the run time end."""
        self.run_time_start = datetime.datetime.utcnow()
        self.run_time_end = self.run_time_start + datetime.timedelta(minutes=self.run_time_minutes)
        self.logger.info("setup-run-time-end", extra={"run_time_start": self.run_time_start, "run_time_end": self.run_time_end, "run_time_minutes": self.run_time_minutes})
        
    def setup_logger(self):
        """Set up the logger."""
        self.logger = logging.getLogger('producer-service')
        self.logger.setLevel(self.log_level)
        formatter = jsonlogger.JsonFormatter("(levelname) (name) (message)", timestamp=True)
        log_handler = logging.StreamHandler(sys.stdout)
        log_handler.setFormatter(formatter)
        self.logger.addHandler(log_handler)
        self.logger.info("setup-logger", extra={"log_level": self.log_level})
        
    def setup_kafka_producer(self):
        """Set up the Kafka producer."""
        kafka_hosts = ["localhost:9092"]
        self.producer = Producer(self.logger, kafka_hosts, self.topic)
        self.logger.info("setup-kafka-producer", extra={"kafka_hosts": kafka_hosts, "topic": self.topic})
    
    def shutdown_gracefully(self):
        """Set graceful shutdown variable."""
        self.got_sigterm_signal = True
        self.logger.warning("shutting-down")
      
    def run(self):
        """Run the main loop for the service."""
        self.logger.info("starting-run")
        message_count = 0
        iteration = 0
        report_at_message_no = 10000
        messages_per_batch = 100
        while datetime.datetime.utcnow() < self.run_time_end:
            posts = [generate_post(x) for x in range(messages_per_batch)]
            self.producer.push(*[json.dumps(post) for post in posts])
            posts = None
            message_count += messages_per_batch
            iteration += 1
            if message_count % report_at_message_no == 0:
                gc.collect()
                self.logger.info("memory-usage", extra={"memory-usage-in-mb": memory_usage_psutil()})
                self.logger.info("produced-messages", extra={"messages_per_batch": messages_per_batch, 
                                                         "message_count": message_count,
                                                         "iteration": iteration,
                                                         "started_at": self.run_time_start.isoformat(),
                                                         "ends_at": self.run_time_end.isoformat()})
        self.producer.producer.flush()
        self.logger.info("run-finished", extra={"topic": self.topic, "run_time_start": self.run_time_start, "run_time_end": self.run_time_end, "run_time_minutes": self.run_time_minutes, "log_level": self.log_level})

def main():
    tracemalloc.start()
    tps = TestProducerService(run_time_minutes=1)
    tps.run()
    snapshot = tracemalloc.take_snapshot()
    top_stats = snapshot.statistics('lineno')
    print("[ Top 10 ]")
    for stat in top_stats[:10]:
        print(stat)
            
if __name__ == '__main__':
    main()
