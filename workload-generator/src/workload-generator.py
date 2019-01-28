import sys
import os
import requests
import threading
import logging
import queue
from timeit import default_timer as timer
import time
import tornado.queues
from tornado import gen
from tornado.httpclient import AsyncHTTPClient
from tornado.ioloop import IOLoop


logging.basicConfig(level=logging.DEBUG, filename="/src/log", filemode="a+", format='%(asctime)s: %(threadName)s  %(message)s')

def log(info):
    #print(info)
    logging.info(info)

# NOTE You must have http:// at the start of the URL
URL = "http://nginx:80"

STATS = None

try:  
   STATS = int(os.environ["STATS"])
except KeyError: 
   print("Please set the environment variables.")
   sys.exit(1)

if len(sys.argv) != 2:
    print("Incorrect usage. Example: /{0} <workload file>".format(sys.argv[0]))
    exit()
path = sys.argv[1]
workload_file = None
try:
    workload_file = open(path, 'r')
except:
    print("Workload file not found: {0}".format(path))
    exit()

workload = workload_file.readlines()

urls = []
work_done = 0

for work in workload:
    arguments = work.split(" ")[1].split(",")
    command = arguments[0].lower()
    arguments = arguments[1:]
    url = "{0}/{1}".format(URL,command)
    for arg in arguments:
        url += "/{0}".format(arg)
    urls.append(url)

def timing_thread(total_work):
    start = timer()
    while True:
        elapsed = timer() - start
        tps = work_done/elapsed
        stats = "[STATS]: {0}/{1} @ {2} transactions per second".format(work_done,total_work,tps)
        print(stats)
        if work_done >= total_work:
            end = timer()
            finishing_debug = "[STATS]: Completed {0} in {1} seconds.".format(path,(end - start))
            log(finishing_debug)
            print(finishing_debug)
            break
        time.sleep(1)

if STATS:
    t = threading.Thread(target=timing_thread, args=(len(urls),))
    t.start()

NUM_WORKERS = 100
QUEUE_SIZE = 1000
q = tornado.queues.Queue(QUEUE_SIZE)
AsyncHTTPClient.configure(None, max_clients=NUM_WORKERS)
http_client = AsyncHTTPClient()

@gen.coroutine
def worker():
    global work_done
    while True:
        url = yield q.get()
        try:
            response = yield http_client.fetch(url)
            #print('got response from', url)
            work_done+=1
        except Exception as e:
            print('failed to fetch', url)
            print(e)
        finally:
            q.task_done()

@gen.coroutine
def main():
    for i in range(NUM_WORKERS):
        IOLoop.current().spawn_callback(worker)
    for url in urls:
        # When the queue fills up, stop here to wait instead
        # of reading more from the file.
        yield q.put(url)
    yield q.join()

IOLoop.current().run_sync(main)