"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer, OFFSET_BEGINNING
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)


class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        # Configure the broker
        self.broker_properties = {
            "bootstrap.servers": ', '.join(map(lambda port: f'PLAINTEXT://localhost:{port}', range(9092, 9095))),
            "enable.auto.commit": True,
            "group.id": f"{topic_name_pattern}-CONSUMER"
        }

        # Create the Consumer, using the appropriate type.
        if is_avro is True:
            self.broker_properties["schema.registry.url"] = "http://localhost:8081"
            self.consumer = AvroConsumer(self.broker_properties)
        else:
            self.consumer = Consumer(self.broker_properties)

        # Configure the consumer and subscribe to the topics.
        if self.offset_earliest:
            self.consumer.subscribe([self.topic_name_pattern], on_assign=self.on_assign)
        else:
            self.consumer.subscribe([self.topic_name_pattern])

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        if self.offset_earliest:
            for partition in partitions:
                partition.offset = OFFSET_BEGINNING
        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = self._consume()
            await gen.sleep(self.sleep_secs)

    def _consume(self):
        """Polls for a message. Returns 1 if a message was received and processed, 0 otherwise"""
        # TODO : exceptions handling check
        message = self.consumer.poll(1)
        if message is None:
            return 0
        # Handle message
        try:
            self.message_handler(message)
            return 1
        except Exception as e:
            logger.error(e)
            return 0
        return 0


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
