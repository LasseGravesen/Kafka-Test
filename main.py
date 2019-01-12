"""Example producer service."""
import json
import string
import sys
import uuid
import random
import datetime
import pykafka
import zlib
import os
import logging
from pythonjsonlogger import jsonlogger
import signal
from pympler.tracker import SummaryTracker
import atexit

class Producer:
    """Produce messages to Kafka.
    Arguments:
        hosts (``list`` of ``str``): Should be a list of hosts. Using the format of: {host}:{port}.
        topic (``str``): The name of the topic to push items to
        broker_version (``int.int.int)`` of str, optional): Define the Kafka
            API version
            *default:* 0.8.2
    """

    def __init__(self, hosts, topic, broker_version="0.8.2", none_delete=False):
        """Construct a Producer."""
        self.topic = topic
        self.none_delete=none_delete
        self.client = pykafka.KafkaClient(hosts=','.join(hosts), socket_timeout_ms=10000, broker_version=broker_version)
        self.producer_topic = self.client.topics[self.topic]
        self.producer = self.producer_topic.get_producer(
            max_retries=3, linger_ms=3000, retry_backoff_ms=1000, use_rdkafka=True
        )
        atexit.register(lambda c: c.stop() if c._running else None, self.producer)

    def push(self, *messages):
        """Push items to Kafka."""

        for message in messages:
            encoded_message = message.encode()
            compressed_message = zlib.compress(encoded_message, 9)
            produced_message = self.producer.produce(compressed_message)
            if self.none_delete:
                encoded_message = None
                compressed_message = None
                produced_message = None

def generate_post(key):
    """Generate a post."""
    output = {"value": "kafka-test"*100,  # "".join(random.choice(string.ascii_lowercase) for i in range(10000)),
              "key": key,
              "correlationId": str(uuid.uuid4())}
    return output
    
class TestProducerService:
    """Test Producer Service."""

    def __init__(self, run_time_minutes=10, none_delete=False):
        """Initialize kafka producer"""
        self.topic = "test-topic"
        self.log_level = "INFO"
        self.run_time_start = None
        self.run_time_end = None
        self.run_time_minutes = run_time_minutes
        self.none_delete = none_delete
        self.got_sigterm_signal = False
        self.logger = None
        self.producer = None
        self.tracker = None
        self.setup_logger()  # Always create the logger first.
        self.setup_memory_tracking()
        self.setup_runtime_end()
        self.setup_kafka_producer()
        if self.logger:
            self.logger.info("setup-done", extra={"topic": self.topic, "run_time_start": self.run_time_start, "run_time_end": self.run_time_end, "run_time_minutes": self.run_time_minutes, "none_delete": self.none_delete, "log_level": self.log_level})
    
    def setup_memory_tracking(self):
        """Set up the memory tracker."""
        self.tracker = SummaryTracker()
        if self.logger:
            self.logger.info("setup-memory-tracker")
    
    def setup_runtime_end(self):
        """Set up the run time end."""
        self.run_time_start = datetime.datetime.utcnow()
        self.run_time_end = self.run_time_start + datetime.timedelta(minutes=self.run_time_minutes)
        if self.logger:
            self.logger.info("setup-run-time-end", extra={"run_time_start": self.run_time_start, "run_time_end": self.run_time_end, "run_time_minutes": self.run_time_minutes})
        
    def setup_logger(self):
        """Set up the logger."""
        self.logger = logging.getLogger('producer-service')
        self.logger.setLevel(self.log_level)
        formatter = jsonlogger.JsonFormatter("(levelname) (name) (message)", timestamp=True)
        log_handler = logging.StreamHandler(sys.stdout)
        log_handler.setFormatter(formatter)
        self.logger.addHandler(log_handler)
        
        #file_handler = logging.FileHandler(f"./logs/kafka-test-delete-{self.none_delete}.log")
        #file_handler.setFormatter(formatter)
        #self.logger.addHandler(file_handler)
        
        self.logger.info("setup-logger", extra={"log_level": self.log_level})
        
    def setup_kafka_producer(self):
        """Set up the Kafka producer."""
        kafka_hosts = ["localhost:9092"]
        self.producer = Producer(kafka_hosts, self.topic, none_delete=self.none_delete)
        if self.logger:
            self.logger.info("setup-kafka-producer", extra={"kafka_hosts": kafka_hosts, "topic": self.topic})
    
    def shutdown_gracefully(self):
        """Set graceful shutdown variable."""
        self.got_sigterm_signal = True
        if self.logger:
            self.logger.warning("shutting-down")
      
    def setup_signal(self):
        """Set up SIGTERM signal handler."""
        signal.signal(signal.SIGTERM, self.shutdown_gracefully)
        if self.logger:
            self.logger.info("setup-sigterm-handler")
      
    def run(self):
        """Run the main loop for the service."""
        if self.logger:
            self.logger.info("starting-run")
        message_count = 0
        iteration = 0
        report_at_message_no = 10000
        messages_per_batch = 100
        while datetime.datetime.utcnow() < self.run_time_end:
            if self.got_sigterm_signal:
                return
            posts = [generate_post(x) for x in range(messages_per_batch)]
            self.producer.push(*[json.dumps(post) for post in posts])
            posts = None
            message_count += messages_per_batch
            iteration += 1
            if message_count % report_at_message_no == 0:
                if self.logger:
                    self.logger.info("produced-messages", extra={"messages_per_batch": messages_per_batch, 
                                                             "message_count": message_count,
                                                             "iteration": iteration,
                                                             "started_at": self.run_time_start.isoformat(),
                                                             "ends_at": self.run_time_end.isoformat()})
        if self.logger:
            self.logger.info("run-finished", extra={"topic": self.topic, "run_time_start": self.run_time_start, "run_time_end": self.run_time_end, "run_time_minutes": self.run_time_minutes, "none_delete": self.none_delete, "log_level": self.log_level})
        self.tracker.print_diff()

def main():
    none_delete = True if os.getenv("NONE_DELETE", "True") == "True" else False
    tps = TestProducerService(run_time_minutes=1, none_delete=none_delete)
    tps.run()
            
if __name__ == '__main__':
    main()
