import re, time

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
            add(transactionNum, user_id, amount, conn)

app = Flask(__name__)

time.sleep(10) # hack for now to let database start up properly
pool = psycopg2.pool.ThreadedConnectionPool(10, 20, user="postgres", password="supersecure", host="postgres", port="5432", database="postgres")

@app.route('/', methods=['POST'])
def root():

    body = request.data.decode('utf-8')
    print(body, flush=True)

    conn = pool.getconn()
    parseCommand(body, conn)
    pool.putconn(conn)

    response = jsonify(success=True)
    return response
