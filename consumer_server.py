import time
import logging
import logging.config
from configparser import ConfigParser
from confluent_kafka import Consumer
from confluent_kafka import OFFSET_BEGINNING

class ConsumerServer:
    """
    Defines the bases kafka consumer class
    """

    def __init__(self, conf):
        self.conf = conf
        self.consumer = Consumer({
            "bootstrap.servers": conf.get("producer", "bootstrap.servers"),
            "auto.offset.reset": conf.get("consumer","auto.offset.reset"),
            "group.id": conf.get("consumer","group.id")
        })
        self.consumer.subscribe([conf.get("consumer", "group.id")], on_assign=self.on_assign)

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        # If the topic is configured to use `offset_earliest` set the partition offset to
        # the beginning or earliest
        for partition in partitions:
            if self.conf.getboolean("consumer", "offset_earliest"):
                partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.conf.get("consumer","group.id"))
        consumer.assign(partitions)

    def run(self):
        """Asynchronously consumes data from kafka topic"""
        try:
            while True:
                # num_results = 1
                # while num_results > 0:
                self._consume()
                time.sleep(1.0)
            
        except KeyboardInterrupt as e:
            self.close()
            
    def _consume(self):
        """Polls for a message. Returns 1 if a message was received, 0 otherwise"""
        try:
            msg = self.consumer.poll(timeout=self.conf.getfloat("consumer","consume_timeout"))
            if msg:
                self.message_handler(msg)
                return 1
            else:
                logger.error(f"Some error in consumer {self.conf.get('consumer', 'group.id')}: {msg}")
                return 0
        except Exception as e:
            logger.error(f"Error in consumer {self.conf.get('consumer','group.id')}: {e}")
            return 0

    def message_handler(self, msg):
        if msg is None:
            logger.info("No message received")
        elif msg.error():
            logger.error(f"Consumer error: {msg.error()}")
        else:
            logger.info(f"Message: {msg.value().decode('utf-8')}")

    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
        logger.info(f"consumer {self.conf.get('consumer','group.id')} closed!!")
        
if __name__ == "__main__":
    logging.config.fileConfig("logging.ini")
    logger = logging.getLogger(__name__)
    config = ConfigParser()
    config.read("app.cfg")
    consumer_server = ConsumerServer(config)
    consumer_server.run()