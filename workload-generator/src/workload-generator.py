import sys
import os
import requests
import threading
import logging
import queue
from timeit import default_timer as timer
import time

logging.basicConfig(level=logging.DEBUG, filename="/src/log", filemode="a+", format='%(asctime)s: %(threadName)s  %(message)s')

def log(info):
    #print(info)
    logging.info(info)

# NOTE You must have http:// at the start of the URL
URL = "http://webserver:5000"
PRINT_WORK = True
PRINT_STATUS_CODE = True
PRINT_TEXT = True

STATS = None
NUM_THREADS = None



try:  
   NUM_THREADS = int(os.environ["NUM_THREADS"])
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

total_work_count = 0
work_done = {} # A list containing the amounts of work done by each worker

user_work = {}
q = queue.Queue()

for work in workload:
    arguments = work.split(" ")[1].split(",")
    command = arguments[0].lower()
    arguments = arguments[1:]
    url = "{0}/{1}".format(URL,command)
    for arg in arguments:
        url += "/{0}".format(arg)
    user = "NO_USER"
    if command != "dumplog":
        user = arguments[0]
    if user not in user_work:
        user_work[user] = []
    user_work[user].append(url)
    total_work_count+=1
    continue

def worker_thread():
    log("Starting worker thread: {0}".format(threading.current_thread().getName()))
    global work_done
    identifier = threading.get_ident()
    work_done[identifier] = 0
    while True:
        work = q.get()
        if work is None:
            break # Once all of the work has been finished
        # Do the work
        for url in work:
            if PRINT_WORK: log("{0}".format(url))
            r = requests.get(url)
            if(str(r.status_code)[0] != "2"):
                log("ATTENTION for URL: {0}".format(url))
            if PRINT_STATUS_CODE: log(r.status_code)
            if PRINT_TEXT: log(r.text+"\n")
            work_done[identifier] = work_done[identifier] + 1
        q.task_done()

def timing_thread(starting_count):
    start = timer()
    global work_done
    while True:
        elapsed = timer() - start
        work_done_total = sum(work_done.values())
        tps = work_done_total/elapsed
        stats = "[STATS]: {0}/{1} @ {2} transactions per second".format(work_done_total,starting_count,tps)
        print(stats)
        if work_done_total >= starting_count:
            end = timer()
            finishing_debug = "[STATS]: Completed {0} using {1} threads in {2} seconds.".format(path,NUM_THREADS,(end - start))
            log(finishing_debug)
            print(finishing_debug)
            break
        time.sleep(1)

threads = []
q = queue.Queue()
# Put each user's work into the queue
for _,work in user_work.items():
    q.put(work)
if STATS:
    t = threading.Thread(target=timing_thread, args=(total_work_count,))
    t.start()
    threads.append(t)

#TODO: Allow all threads to start and get a workload before the first request is sent.
for _ in range(NUM_THREADS):
    t = threading.Thread(target=worker_thread)
    t.start()
    threads.append(t)

q.join() # block until all tasks are complete

# Stop all of the workers
for _ in range(NUM_THREADS):
    q.put(None)
for t in threads:
    t.join()