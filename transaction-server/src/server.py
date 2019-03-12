import re, time
from threading import Thread

from flask import Flask, request, jsonify
from lib.commands import *
import psycopg2
from psycopg2 import pool

pattern = re.compile(r"^\[(\d+)\] ([A-Z_]+),([^ ]+) ?$")

"""
Provides a method to call with parameters.
"""
def parseCommand(raw, conn):

    match = re.findall(pattern, raw)

    if not match:
        print("No matching command found.")
        return

    transactionNum, command, arguments = match[0]
    transactionNum = int(transactionNum)
    arguments = arguments.split(",")

    #ADD Command
    if command == "ADD":
        try:
            user_id, amount = arguments
        except ValueError:
            print("Invalid Input. <ADD, USER_ID, AMOUNT>")
        else:    
            for i in range(10):
                add(transactionNum, user_id, amount, conn)

app = Flask(__name__)
app.run(threaded=True, host='0.0.0.0')

time.sleep(10) # hack for now to let database start up properly
pool = psycopg2.pool.ThreadedConnectionPool(10, 20, user="postgres", password="supersecure", host="postgres", port="5432", database="postgres")

@app.route('/', methods=['POST'])
def root():

    body = request.data.decode('utf-8')
    print(body, flush=True)
    body = "[1] ADD,usr1,100"

    #parseCommand(body, conn)

    threads = []
    for i in range(0, 10):
        conn = pool.getconn()
        t = Thread(target=parseCommand, args=(body, conn))
        # pool.putconn(conn) We don't need to put this back for the test scenario but we should later
        threads.append(t)

    for thread in threads:
        thread.start()

    response = jsonify(success=True)
    return response
