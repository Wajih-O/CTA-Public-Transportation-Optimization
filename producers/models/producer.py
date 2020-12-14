"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)

def topic_exists(client, topic_name):
    """ A helper to check the existance of a givent topic """
    # topic_name in set(t.topic for t in iter(client.list_topics(timeout=5).topics.values()))
    return client.list_topics(timeout=5).topics.get(topic_name) is not None

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas


        # Configure the broker properties below. Make sure to reference the project README
        # and use the Host URL for Kafka and Schema Registry!

        self.broker_properties = {
            "bootstrap.servers": "localhost:9092",  # TODO (optional): load from a config file!
            "schema.registry.url": "http://localhost:8081"
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # TODO: Configure the AvroProducer
        # assumes that value_schema and key_schema passed as strings !
        self.producer = AvroProducer(self.broker_properties, default_key_schema=avro.loads(self.key_schema),
                            default_value_schema=avro.loads(self.value_schema))

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""
        client = AdminClient(self.broker_properties) # create a client
        if not topic_exists(client, self.topic_name):
            futures = client.create_topics([
                    NewTopic(self.topic_name, num_partitions=self.num_partitions,
                            replication_factor=self.num_replicas,
                            # config={"compression_type":"lz4"})
                            # TODO: cleanup.policy, delete.retentions.ms
                ])
            # Wait for each operation to finish.
            for topic, future in futures.items():
                try:
                    future.result()  # The result itself is None
                    logger.info("Topic {} created".format(topic))
                except Exception as e:
                    logger.infor("Failed to create topic {}: {}".format(topic, e))
        else:
            logger.info(f"topic {self.topic_name} already exists!")


    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        # TODO: Write cleanup code for the Producer h
        self.producer.flush() # write to kafka
        logger.info("producer close (clean pipeline)")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))