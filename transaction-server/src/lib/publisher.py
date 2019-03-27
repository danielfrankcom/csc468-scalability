import pika
import time

class Publisher(object):
    def __init__(self):
        credentials = pika.credentials.PlainCredentials('admin','admin')
        self.connection = None
        while True:
            try:
                self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq',credentials=credentials))
            except:
                print("Cannot connect, trying again in 1 seconds...")
                time.sleep(1)
                continue
            else:
                print("Connection successful...")
                break
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='logs')
        print("Finishing init. of Publisher")

    async def publish_message(self,message):
        print("Publishing message",message)
        self.channel.basic_publish(exchange='',
                            routing_key='logs',
                            body=message)
