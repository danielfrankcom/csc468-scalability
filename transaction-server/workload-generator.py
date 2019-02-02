import sys
import os
import requests
import threading
import logging
import queue
from timeit import default_timer as timer
import time
import commands






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

cursor, conn = commands.initdb()

for work in workload:
    arguments = work.split(" ")[1].split(",")
    command = arguments[0]

#    if command == "add":
#        commands.add(arguments[0], arguments[1], cursor, conn)
#    elif command == 
    #QUOTE Command
    if command == "QUOTE":
        print("got in quote:", arguments)
        try:
            command, user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid Input. <QUOTE user_id stock_symbol>")
        else:    
            commands.quote(user_id, stock_symbol)
    #ADD Command
    elif command == "ADD":
        try:
            command, user_id, amount = arguments
        except ValueError:
            print("Invalid Input. <ADD, USER_ID, AMOUNT>")
        else:    
            commands.add(user_id, amount, cursor, conn)
    #BUY Command
    elif command == "BUY":
        try:
            command, user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid Input. <BUY USER_ID STOCK_SYMBOL AMOUNT>")
        else:    
            commands.buy(user_id, stock_symbol, amount, cursor, conn)
    elif command == "COMMIT_BUY":
        try:
            command, user_id = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_BUY USER_ID>")
        else:    
            commands.commit_buy(user_id, cursor, conn)
    elif command == "CANCEL_BUY":
        try:
            command, user_id = arguments
        except ValueError:
            print("Invalid Input. <CANCEL_BUY USER_ID>")
        else:    
            commands.cancel_buy(user_id, cursor, conn)
    elif command == "SELL":
        try:
            command, user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid Input. <SELL USER_ID STOCK_SYMBOL AMOUNT>")
        else:    
            commands.sell(user_id, stock_symbol, amount, cursor, conn)
    elif command == "COMMIT_SELL":
        try:
            command, user_id = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_SELL USER_ID>")
        else:    
            commands.commit_sell(user_id, cursor, conn)
    elif command == "SET_BUY_AMOUNT":
        try:
            command, user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid input.  <SET_BUY_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            commands.set_buy_amount(user_id, stock_symbol, amount, cursor, conn)
    elif command == "CANCEL_SET_BUY":
        try:
            command, user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid input.  <CANCEL_SET_BUY USER_ID STOCK_SYMBOL>")
        else:
            commands.cancel_set_buy(user_id, stock_symbol, cursor, conn)
    elif command == "SET_BUY_TRIGGER":
        try:
            command, user_id, symbol, amount = arguments
        except ValueError:
            print("Invalid input. <SET_BUY_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            commands.set_buy_trigger(user_id, symbol, amount, cursor, conn)
    elif command == "SET_SELL_AMOUNT":
        try:
            command, user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid input.  <SET_SELL_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            commands.set_sell_amount(user_id, stock_symbol, amount, cursor, conn)
    elif command == "CANCEL_SET_SELL":
        try:
            command, user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid input.  <CANCEL_SET_SELL USER_ID STOCK_SYMBOL>")
        else:
            commands.cancel_set_sell(user_id, stock_symbol, cursor, conn)
    elif command == "SET_SELL_TRIGGER":
        try:
            command, user_id, symbol, amount = arguments
        except ValueError:
            print("Invalid input. <SET_SELL_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            commands.set_sell_trigger(user_id, symbol, amount, cursor, conn)

    elif command == "quit":
        break
    else:
        print(arguments, " Invalid Command")

commands.closedb(cursor)
