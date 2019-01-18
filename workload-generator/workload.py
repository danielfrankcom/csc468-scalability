import sys
import requests
import threading
import logging

logging.basicConfig(level=logging.DEBUG, filename="log", filemode="a+", format='%(asctime)s: %(threadName)s  %(message)s')

def log(info):
    #print(info)
    logging.info(info)

# NOTE You must have http:// at the start of the URL
URL = "http://127.0.0.1:5000"
PRINT_WORK = True
PRINT_STATUS_CODE = True
PRINT_TEXT = True

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

user_work = {}
for work in workload:
    arguments = work.split(" ")[1].split(",")
    command = arguments[0].lower()
    arguments = arguments[1:]
    url = "{0}/{1}".format(URL,command)
    for arg in arguments:
        url += "/{0}".format(arg)
    # if the command is a user command, the user thread will do the work
    if command != "dumplog":
        user = arguments[0]
        if user not in user_work:
            user_work[user] = []
        user_work[user].append(url)
        continue
    if PRINT_WORK: log(url)
    r = requests.get(url)
    if(str(r.status_code)[0] != "2"):
        log("ATTENTION for URL: {0}".format(url))
    if PRINT_STATUS_CODE: log(r.status_code)
    if PRINT_TEXT: log(r.text)


def worker_thread(workload):
    log("Starting worker thread: {0}".format(threading.current_thread().getName()))
    index = 0
    for url in workload:
        if PRINT_WORK: log("{0} : {1}".format(index,url))
        r = requests.get(url)
        if(str(r.status_code)[0] != "2"):
            log("ATTENTION for URL: {0}".format(url))
        if PRINT_STATUS_CODE: log(r.status_code)
        if PRINT_TEXT: log(r.text+"\n")
        index+=1

threads = []
for worker,workload in user_work.items():
    t = threading.Thread(target=worker_thread, name=worker, args=(workload,))
    threads.append(t)
    t.start()







