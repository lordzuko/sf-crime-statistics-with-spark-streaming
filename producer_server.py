import json
import time
import logging

from confluent_kafka import Producer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic
from tqdm import tqdm

logger = logging.getLogger(__name__)

class ProducerServer:
    """
    Setup Basic Kafka Servers
    """
    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(self, conf):
        self.conf = conf
        self.producer = Producer({
            "bootstrap.servers": conf.get("bootstrap.servers")
        })
        self.client = AdminClient({
            "bootstrap.servers": conf.get("bootstrap.servers")
        })
        if conf.get("topic_name") not in ProducerServer.existing_topics:
            self.create_topic()
            ProducerServer.existing_topics.add(conf.get("topic_name"))

        
    def topic_exists(self, topic_name):
        """Checks if the given topic exists"""
        topics = self.client.list_topics(timeout=5)
        return topics.topics.get(topic_name) is not None

    def create_topic(self):
        """
        Create topic if it doesn't exists
        """
        exists = self.existing_topics(self.client)
        if exists:
            logger.info(f"topic already exists: {self.topic_name}")
        else:
            futures = self.client.create_topics([
                NewTopic(
                    topic=self.conf.get("topic_name"),
                    num_partitions=self.conf.get("num_partitions"),
                    replication_factor=self.conf.get("num_replicas")
                )
            ])

            for topic, future in futures.items():
                try:
                    future.result()
                    logger.info(f"topic created: {self.conf.get('topic_name')}")
                except Exception as e:
                    logger.error(f"failed to create topic {self.conf.get('topic_name')}: {e}")

    @staticmethod
    def serialize_json(json_data):
        """
        Take JSON dictionary object and convert that to string(serialize)
        :param json_data: JSON dictionary object 
        :return: JSON string
        """ 
        return json.dumps(json_data)
    
    def generetate_data(self):
        """
        Read JSON data and produce serialized rows to Kafa Topic
        """
        try:
            with open(self.conf.get('input_file')) as f:
                data = json.loads(f.read())
                logger.info(f"Reading {len(data)} lines from {self.conf.get('input_file')}")
                for idx, row in tqdm(enumerate(data), total=len(data), desc="Producer:> "):
                    message = self.serialize_json(row)
                    logger.info(f"Serialized Data: {message}")
                    self.producer.produce(
                        topic=self.conf.get("topic_name"),
                        value=message
                    )
            logger.info("Processing complete \n Cleaning Producer!")
            self.close()

        except KeyboardInterrupt as e:
            self.close()

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        try:
            if self.conf.get("topic_name"):
                self.producer.flush()
                logger.info("Producer Shutdown!!! ")
        except Exception as e:
            logger.error("producer close incomplete - skipping")