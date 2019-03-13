import sys, os, re
from flask import Flask, request, jsonify
from lib.commands import *

from queue import Queue
from threading import Thread
import time
import psycopg2
from psycopg2 import pool

pattern = re.compile(r"^\[(\d+)\] ([A-Z_]+),([^ ]+) ?$")


"""
Provides a method to call with parameters.
"""
def parse(raw, cursor, conn):

    match = re.findall(pattern, raw)

    if not match:
        print("No matching command found.")
        return

    transactionNum, command, arguments = match[0]
    transactionNum = int(transactionNum)
    arguments = arguments.split(",")
    
    #QUOTE Command
    if command == "QUOTE":
        print("got in quote:", arguments)
        try:
            user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid Input. <QUOTE user_id stock_symbol>")
        else:    
            quote(transactionNum, user_id, stock_symbol)
    #ADD Command
    elif command == "ADD":
        try:
            user_id, amount = arguments
        except ValueError:
            print("Invalid Input. <ADD, USER_ID, AMOUNT>")
        else:    
            add(transactionNum, user_id, amount, cursor, conn)
    #BUY Command
    elif command == "BUY":
        try:
            user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid Input. <BUY USER_ID STOCK_SYMBOL AMOUNT>")
        else:    
            buy(transactionNum, user_id, stock_symbol, amount, cursor, conn)
    elif command == "COMMIT_BUY":
        try:
            [user_id] = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_BUY USER_ID>")
        else:    
            commit_buy(transactionNum, user_id, cursor, conn)
    elif command == "CANCEL_BUY":
        try:
            [user_id] = arguments
        except ValueError:
            print("Invalid Input. <CANCEL_BUY USER_ID>")
        else:    
            cancel_buy(transactionNum, user_id, cursor, conn)
    elif command == "SELL":
        try:
            user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid Input. <SELL USER_ID STOCK_SYMBOL AMOUNT>")
        else:    
            sell(transactionNum, user_id, stock_symbol, amount, cursor, conn)
    elif command == "COMMIT_SELL":
        try:
            [user_id] = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_SELL USER_ID>")
        else:    
            commit_sell(transactionNum, user_id, cursor, conn)
    elif command == "SET_BUY_AMOUNT":
        try:
            user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid input.  <SET_BUY_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_buy_amount(transactionNum, user_id, stock_symbol, amount, cursor, conn)
    elif command == "CANCEL_SET_BUY":
        try:
            user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid input.  <CANCEL_SET_BUY USER_ID STOCK_SYMBOL>")
        else:
            cancel_set_buy(transactionNum, user_id, stock_symbol, cursor, conn)
    elif command == "SET_BUY_TRIGGER":
        try:
            user_id, symbol, amount = arguments
        except ValueError:
            print("Invalid input. <SET_BUY_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_buy_trigger(transactionNum, user_id, symbol, amount, cursor, conn)
    elif command == "SET_SELL_AMOUNT":
        try:
            user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid input.  <SET_SELL_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_sell_amount(transactionNum, user_id, stock_symbol, amount, cursor, conn)
    elif command == "CANCEL_SET_SELL":
        try:
            user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid input.  <CANCEL_SET_SELL USER_ID STOCK_SYMBOL>")
        else:
            cancel_set_sell(transactionNum, user_id, stock_symbol, cursor, conn)
    elif command == "CANCEL_SELL":
        try:
            [user_id] = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_SELL USER_ID>")
        else:    
            cancel_sell(transactionNum, user_id, cursor, conn) 
    elif command == "SET_SELL_TRIGGER":
        try:
            user_id, symbol, amount = arguments
        except ValueError:
            print("Invalid input. <SET_SELL_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_sell_trigger(transactionNum, user_id, symbol, amount, cursor, conn)
    elif command == "DUMPLOG":
        try:
            [location] = arguments
        except ValueError:
            try:
                user_id, filename = arguments
            except ValueError: 
                print("Invalid Input. Functionality: <DUMPLOG FILENAME> or <DUMPLOG USERNAME>")
            else:
                filename = os.path.basename(location)
                path = os.path.join("/out/", filename)
                dumplog_user(transactionNum, user_id, path)
        else:
            filename = os.path.basename(location)
            path = os.path.join("/out/", filename)
            dumplog(transactionNum, path)
    elif command == "DISPLAY_SUMMARY":
        try:
            [user_id] = arguments
        except ValueError:
            print("Invalid input. <DISPLAY_SUMMARY USER_ID>")
        else:
            display_summary(transactionNum, user_id)
    else:
        print(arguments, " Invalid Command")

app = Flask(__name__)

WORKERS=80

time.sleep(10) # hack - fix me
pool = psycopg2.pool.ThreadedConnectionPool(10, WORKERS, user="postgres", password="supersecure", host="postgres", port="5432", database="postgres")

transactions = Queue()

def process():
    
    while True:
        transaction = transactions.get()
        print("Received: " + transaction)
        conn = pool.getconn()

        parse(transaction, conn.cursor(), conn)
        print("Processed!")
        pool.putconn(conn)

for i in range(WORKERS):
    t = Thread(target=process)
    t.start()

@app.route('/', methods=['POST'])
def root():

    body = request.data.decode('utf-8')
    print(body, flush=True)

    transactions.put(body)

    response = jsonify(success=True)
    return response
