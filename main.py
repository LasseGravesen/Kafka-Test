"""Example producer service."""
import json
import string
import uuid
import random
import pykafka
import zlib
import logging

class Producer:
    """Produce messages to Kafka.
    Arguments:
        hosts (``list`` of ``str``): Should be a list of hosts. Using the format of: {host}:{port}.
        topic (``str``): The name of the topic to push items to
        broker_version (``int.int.int)`` of str, optional): Define the Kafka
            API version
            *default:* 0.8.2
    """

    def __init__(self, hosts, topic, broker_version="0.8.2"):
        """Construct a Producer."""
        self.topic = topic
        self.client = pykafka.KafkaClient(hosts=','.join(hosts), socket_timeout_ms=10000, broker_version=broker_version)
        self.producer_topic = self.client.topics[self.topic]
        self.producer = self.producer_topic.get_producer(
            max_retries=3, linger_ms=3000, retry_backoff_ms=1000, use_rdkafka=True
        )

    def push(self, *messages):
        """Push items to Kafka."""

        results = []
        for message in messages:
            compressed_message = zlib.compress(message.encode(), 9)
            results.append(self.producer.produce(compressed_message))

        return results

def generate_post(key):
    """Generate a post."""
    output = {"value": "".join(random.choice(string.ascii_lowercase) for i in range(10000)),
              "key": key,
              "correlationId": str(uuid.uuid4())}
    return output

FORMAT = '%(timestamp)s %(level)s %(message)s'
logging.basicConfig(format=FORMAT)    
    
class TestProducerService:
    """Test Producer Service."""

    def __init__(self,):
        """Initialize kafka producer"""
        self.topic = "test-topic"
        self.log = logging.getLogger('producer-service')
        self.kafka_hosts = ["kafka:9092"]
        self.producer = Producer(self.kafka_hosts, self.topic)        
        
    def run(self):
        """Run the main loop for the service."""
        while True:
            posts = [generate_post(x) for x in range(100)]
            self.producer.push(*[json.dumps(post) for post in posts])
            self.log.info("produced 100 messages")

def main():
    tps = TestProducerService()
    tps.run()
            
            
if __name__ == '__main__':
    main()
