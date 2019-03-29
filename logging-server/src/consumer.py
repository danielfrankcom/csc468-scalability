from lib.logging_DB import logging_DB
import logging
import json
import pika
import time
import threading

logger = logging.getLogger(__name__)

class Consumer(object):
    def __init__(self):
        credentials = pika.credentials.PlainCredentials('admin','admin')
        self.connection = None
        while True:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',credentials=credentials))
            except:
                logger.info("Cannot connect to RabbitMQ, trying again in 1 seconds...")
                time.sleep(1)
                continue
            else:
                logger.info("RabbitMQ Connection successful...")
                break
        self.db = None
        while True:
            try:
                self.db = logging_DB()
            except:
                logger.info("Cannot connect to DB, trying again in 1 seconds...")
                time.sleep(1)
                continue
            else:
                logger.info("Connected to DB!")
                break

        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='logs')
        self.channel.basic_consume(consumer_callback=self.callback, queue='logs', no_ack=True)
        consumer_thread = threading.Thread(target=self.consume)
        consumer_thread.start()
        logger.debug("Finished init of Consumer")

    def callback(self, ch, method, properties, body):
        logger.info("Consumed %s",body)
        j = json.loads(body)
        log_type = j["type"]
        data = j["data"]
        if log_type == "userCommand":
            self.db.userCommand(data)
        elif log_type == "quoteServer":
            self.db.quoteServer(data)
        elif log_type == "accountTransaction":
            self.db.accountTransaction(data)
        elif log_type == "systemEvent":
            self.db.systemEvent(data)
        elif log_type == "errorEvent":
            self.db.errorEvent(data)
        elif log_type == "debugEvent":
            self.db.debugEvent(data)
        else:
            logger.error("MESSAGE TYPE NOT FOUND %s",log_type)
    
    def consume(self):
        self.channel.start_consuming()  

consumer = Consumer()






