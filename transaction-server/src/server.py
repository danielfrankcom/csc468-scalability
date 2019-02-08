import sys, os
from flask import Flask, request, jsonify
from lib.commands import *

"""
Provides a method to call with parameters.
"""
def parseCommand(raw):
    arguments = raw.split(",")
    command = arguments[0]
    command = command.upper()

    #QUOTE Command
    if command == "QUOTE":
        print("got in quote:", arguments)
        try:
            command, user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid Input. <QUOTE user_id stock_symbol>")
        else:    
            quote(user_id, stock_symbol)
    #ADD Command
    elif command == "ADD":
        try:
            command, user_id, amount = arguments
        except ValueError:
            print("Invalid Input. <ADD, USER_ID, AMOUNT>")
        else:    
            add(user_id, amount, cursor, conn)
    #BUY Command
    elif command == "BUY":
        try:
            command, user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid Input. <BUY USER_ID STOCK_SYMBOL AMOUNT>")
        else:    
            buy(user_id, stock_symbol, amount, cursor, conn)
    elif command == "COMMIT_BUY":
        try:
            command, user_id = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_BUY USER_ID>")
        else:    
            commit_buy(user_id, cursor, conn)
    elif command == "CANCEL_BUY":
        try:
            command, user_id = arguments
        except ValueError:
            print("Invalid Input. <CANCEL_BUY USER_ID>")
        else:    
            cancel_buy(user_id, cursor, conn)
    elif command == "SELL":
        try:
            command, user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid Input. <SELL USER_ID STOCK_SYMBOL AMOUNT>")
        else:    
            sell(user_id, stock_symbol, amount, cursor, conn)
    elif command == "COMMIT_SELL":
        try:
            command, user_id = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_SELL USER_ID>")
        else:    
            commit_sell(user_id, cursor, conn)
    elif command == "SET_BUY_AMOUNT":
        try:
            command, user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid input.  <SET_BUY_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_buy_amount(user_id, stock_symbol, amount, cursor, conn)
    elif command == "CANCEL_SET_BUY":
        try:
            command, user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid input.  <CANCEL_SET_BUY USER_ID STOCK_SYMBOL>")
        else:
            cancel_set_buy(user_id, stock_symbol, cursor, conn)
    elif command == "SET_BUY_TRIGGER":
        try:
            command, user_id, symbol, amount = arguments
        except ValueError:
            print("Invalid input. <SET_BUY_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_buy_trigger(user_id, symbol, amount, cursor, conn)
    elif command == "SET_SELL_AMOUNT":
        try:
            command, user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid input.  <SET_SELL_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_sell_amount(user_id, stock_symbol, amount, cursor, conn)
    elif command == "CANCEL_SET_SELL":
        try:
            command, user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid input.  <CANCEL_SET_SELL USER_ID STOCK_SYMBOL>")
        else:
            cancel_set_sell(user_id, stock_symbol, cursor, conn)
    elif command == "CANCEL_SELL":
        try:
            command, user_id = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_SELL USER_ID>")
        else:    
            cancel_sell(user_id, cursor, conn) 
    elif command == "SET_SELL_TRIGGER":
        try:
            command, user_id, symbol, amount = arguments
        except ValueError:
            print("Invalid input. <SET_SELL_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_sell_trigger(user_id, symbol, amount, cursor, conn)
    elif command == "DUMPLOG":
        try:
            command, location = arguments
        except ValueError:
            print("Invalid input. <SET_SELL_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            filename = os.path.basename(location)
            path = os.path.join("/out/", filename)
            dumplog(path)

    else:
        print(arguments, " Invalid Command")


app = Flask(__name__)
cursor, conn = initdb()

@app.route('/', methods=['POST'])
def root():

    body = request.data.decode('utf-8')
    print(body, flush=True)
    parseCommand(body)

    response = jsonify(success=True)
    return response
