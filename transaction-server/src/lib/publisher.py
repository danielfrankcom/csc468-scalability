import logging
import pika
import time

logger = logging.getLogger(__name__)

class Publisher(object):
    def __init__(self):
        credentials = pika.credentials.PlainCredentials('admin','admin')
        self.connection = None
        while True:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',credentials=credentials))
            except:
                logger.info("Cannot connect, trying again in 1 seconds...")
                time.sleep(1)
                continue
            else:
                logger.info("Publisher connection successful...")
                break
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='logs')
        logger.debug("Finished init of Publisher")

    async def publish_message(self,message):
        logger.info("Publishing message %s", message)
        self.channel.basic_publish(exchange='',
                            routing_key='logs',
                            body=message)
