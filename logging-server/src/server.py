import pika
import time

connection = None
channel = None

while True:
    try:
        cred = pika.credentials.PlainCredentials('admin','admin')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', credentials=cred))
        channel = connection.channel()
    except:
        time.sleep(1)
        continue
    else:
        break

channel.queue_declare(queue='logging')


def callback(ch, method, properties, body):
    print(" [x] Received %r" % body, flush=True)

channel.basic_consume(callback,
                      queue='logging',
                      no_ack=True)

print(' [*] Waiting for messages. To exit press CTRL+C', flush=True)
channel.start_consuming()