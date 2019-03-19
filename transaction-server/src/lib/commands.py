import psycopg2
import socket, sys
import time, threading, datetime
import random # used to gen random numbers in get_quote()
from lib.xml_writer import *
from itertools import count

QUOTE_LIFESPAN = 60.0 # period of time a quote is valid for (will be 60.0 for deployed software)
accounts = []
cached_quotes = {}

XMLTree = LogBuilder("/out/testLOG")

def add(transaction_num, user_id, amount, cursor, conn):
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "ADD",
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    XMLTree.append(command)
    
    function =  "INSERT INTO users (username, balance) " \
                "VALUES ('{username}', {amount}) " \
                "ON CONFLICT (username) DO UPDATE " \
                "SET balance = (users.balance + {amount}) " \
                "WHERE users.username = '{username}';".format(username=user_id, amount=amount)
    
    cursor.execute(function)
    conn.commit()
        
    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "action": "add", 
        "username": user_id,
        "funds": float(amount)
    }
    transaction.updateAll(**attributes)
    XMLTree.append(transaction)

# Contact the actual quote server
def contact_server(query):

        # Create the socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect the socket
        #s.connect(('quoteserve.seng.uvic.ca', 4444))
        s.connect(('192.168.1.249', 6000))

        # Send the user's query
        s.send(query.encode())

        # Read and print up to 1k of data.
        data = s.recv(1024).decode().split("\n")[0]

        # close the connection, and the socket
        s.close()

        return data

# Contact a fake server
def fake_server(query):
        return "20.01,BAD,usernamehere,1549827515,crytoKEY=123=o"

# get_quote() is used to directly acquire a quote from the quote server (eventually)
# for now, this is a placeholder function, and returns a random value between
# 1.0 and 10.0. 
def get_quote(user_id, stock_symbol):

        request = "{symbol},{user}\n".format(symbol=stock_symbol, user=user_id)

        data = contact_server(request)
        #data = fake_server(request)

        price, symbol, username, timestamp, cryptokey = data.split(",")
        return float(price), int(timestamp), cryptokey, username

# quote() is called when a client requests a quote.  It will return a valid price for the
# stock as requested, but this value will either come from cached_quotes or from q hit
# to the quote server directly.  In the case of the latter, the new quote will be put
# in the cached_quotes dictionary
def quote(transaction_num, user_id, stock_symbol):
    # if not stock_symbol in cached_quotes.keys() or ((time.time() - cached_quotes[stock_symbol][1]) > QUOTE_LIFESPAN):
    # get quote from server
    new_price, time_of_quote, cryptokey, quote_user = get_quote(user_id, stock_symbol)

    quote = QuoteServer()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "price": new_price, 
        "username": quote_user,
        "stockSymbol": stock_symbol,
        "quoteServerTime": time_of_quote,
        "cryptokey": cryptokey
    }
    quote.updateAll(**attributes)
    XMLTree.append(quote)

    # cached_quotes[stock_symbol] = (new_price, time_of_quote)
    return new_price, stock_symbol, user_id, time_of_quote, cryptokey

""" 
        need to send data to log file including:
        - "quoteServer" of type "QuoteServerType"
        - "timestampe" of type "unixTimeLimits"
        - "server" of type "xsd:string"
        - "transactionNum" of type "xsd:positiveInteger"
        - "price" of type "xsd:decimal"
        - "stockSymbol" of type "stockSymbolType"
        - "username" of type "xsd:string"
        - "quoteServerTime" of type "xsd:integer"
        - "cryptokey" of type "xsd:string"
        
"""
# Helper function - used to cancel buy orders after they timeout
def buy_timeout(user_id, stock_symbol, dollar_amount, cursor, conn):
    cursor.execute('SELECT * FROM reserved WHERE    '
        'type = %s AND                              '
        'username = %s AND                          '
        'stock_symbol = %s AND                      '
        'amount = %s;                               ',
        ('buy', user_id, stock_symbol, dollar_amount))
    
    
    try:
        result = cursor.fetchone()
    except:
        conn.rollback()
        return

    if result is None: #order has already been manually confirmed or cancelled
        print("Timer for order is up, but the order is already gone")
        return
    else: # the reservation still exists, so delete it and refund the cash back to user's account
        reservationid = result[0]
        reserved_cash = result[6]
        cursor.execute('DELETE FROM reserved WHERE reservationid = %s;', (reservationid,))    
        cursor.execute('SELECT balance FROM users where username = %s;', (user_id,))
        try:
            result = cursor.fetchall()
        except:
            conn.rollback()
            return
        if result is None:
            print("Error - user does not exist!")
            return
        existing_balance = result[0][0]
        new_balance = existing_balance + reserved_cash
        cursor.execute('UPDATE users SET balance = %s WHERE username = %s;', (str(new_balance), (user_id)))
        conn.commit()        
        print('buy order timout - the following buy order is now cancelled: ', 
            user_id, stock_symbol, dollar_amount)

def buy(transaction_num, user_id, stock_symbol, amount, cursor, conn):
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "BUY",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    XMLTree.append(command)
    
    price, stock_symbol, user_id, time_of_quote, cryptokey = quote(transaction_num, user_id, stock_symbol)
    # Debugging print - feel free to delete this (Dusty)
    print("in BUY.  List of data from quote:", 
        "Price:", price,
        "stock_symbol:", stock_symbol,
        "user_id:", user_id,
        "time_of_quote:", time_of_quote,
        "cryptokey:", cryptokey)

    balance_update =    "UPDATE users " \
                        "SET balance = balance - '{amount}' " \
                        "WHERE username = '{username}' " \
                        "AND balance >= {amount};".format(username=user_id, amount=amount)

    reserved_update =   "INSERT INTO reserved " \
                        "(type, username, stock_symbol, stock_quantity, price, amount, timestamp) " \
                        "VALUES " \
                        "('{command}', '{username}', '{stock_symbol}', {stock_quantity}, {price}, {amount}, {timestamp});".format(command='buy', username=user_id, stock_symbol=stock_symbol, stock_quantity=int(float(amount)/price), price=price, amount=amount, timestamp=round(time.time()))

    cursor.execute("BEGIN")
    try:
        print("in try block of BUY:", balance_update)
        cursor.execute(balance_update)
        cursor.execute(reserved_update)
        conn.commit()
    except Exception as e:
        print("in except block of BUY:", e)
        conn.rollback()
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "stockSymbol": stock_symbol,
            "funds": float(amount),
            "errorMessage": "Insufficient Funds" 
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        return


    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "action": "remove", 
        "username": user_id,
        "funds": float(amount)
    }
    transaction.updateAll(**attributes)
    XMLTree.append(transaction)

    #threading.Timer(QUOTE_LIFESPAN, buy_timeout, args=(user_id, stock_symbol, amount, cursor, conn)).start()


def commit_buy(transaction_num, user_id, cursor, conn):
    cursor.execute('SELECT * FROM reserved WHERE type = %s AND username = %s AND timestamp > %s;', ('buy', user_id, round(time.time(), 5)-60))
    conn.commit()

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "COMMIT_BUY",
        "username": user_id
    }
    command.updateAll(**attributes)
    XMLTree.append(command)
    
    
    # NO BUY TO COMMIT
    try:
        if cursor.fetchall() == []:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": transaction_num,
                "command": "BUY",
                "username": user_id,
                "errorMessage": "No BUY to commit" 
            }
            error.updateAll(**attributes)
            XMLTree.append(error)
        # BUY TO COMMIT
        else:
        
            cursor.execute('SELECT reservationid, stock_symbol, stock_quantity, amount, price FROM reserved WHERE type = %s AND username = %s AND timestamp = (SELECT MAX(timestamp) FROM reserved WHERE type = %s AND username = %s);', ('buy', user_id, 'buy', user_id))
            conn.commit()

            try:
                elements = cursor.fetchone()
                reservationid = elements[0]
                stock_symbol = elements[1]
                stock_quantity = elements[2]
                amount = elements[3]
                price = elements[4]
            except:
                conn.rollback()
                return
            
            
            function =  "INSERT INTO stocks (username, stock_symbol, stock_quantity) " \
                        "VALUES ('{username}', '{symbol}', {quantity}) " \
                        "ON CONFLICT (username, stock_symbol) DO UPDATE " \
                        "SET stock_quantity = (stocks.stock_quantity + {quantity}) " \
                        "WHERE stocks.username = '{username}' and stocks.stock_symbol = '{symbol}';".format(username=user_id, symbol=stock_symbol, quantity=stock_quantity)
            
            cursor.execute(function, (user_id, amount))
            
            cursor.execute('UPDATE users SET balance = balance + %s WHERE username = %s', (amount-(price*stock_quantity), user_id))    
            conn.commit
            

            transaction = AccountTransaction()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": transaction_num,
                "action": "add", 
                "username": user_id,
                "funds": float(amount-(price*stock_quantity))
            }
            transaction.updateAll(**attributes)
            XMLTree.append(transaction)

            cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
            conn.commit()        
        return
    except:
        conn.rollback()
        pass

def cancel_buy(transaction_num, user_id, cursor, conn):
    cursor.execute('SELECT reservationid, amount FROM reserved WHERE type = %s AND username = %s AND timestamp = (SELECT MAX(timestamp) FROM reserved WHERE type = %s AND username = %s);', ('buy', user_id, 'buy', user_id))
    conn.commit()

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "CANCEL_BUY",
        "username": user_id
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    # NOTE: Below is a bandaid, try/except block wrapped around the fetchone() command
    # TODO: implement proper logic, an error is logged or a succesful cance_buy is logged
    #       (as well as a transaction logged, depositing the funds back into user account)
    #   - Dusty

    elements = None
    try:
        elements = cursor.fetchone()
    except:
        conn.rollback()
        return

    if elements is None:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "errorMessage": "No BUY to cancel" 
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        
        return

    # if it runs this far, there must be something there
    reservationid = elements[0]
    amount = elements[1]
    cursor.execute('UPDATE users SET balance = balance + %s where username = %s', (amount, user_id))
    conn.commit()

    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "action": "add", 
        "username": user_id,
        "funds": float(amount)
    }
    transaction.updateAll(**attributes)
    XMLTree.append(transaction)

    cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
    conn.commit()
    return 

def sell(transaction_num, user_id, stock_symbol, amount, cursor, conn):
    cursor.execute('SELECT username FROM users;')
    conn.commit()
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "SELL",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    price, stock_symbol, user_id, time_of_quote, cryptokey = quote(transaction_num, user_id, stock_symbol)

    try:
        test = cursor.fetchall()
    except:
        conn.rollback()
        return

    for i in test:
        # USER EXISTS
        if i[0] == user_id:
            cursor.execute("SELECT stock_quantity FROM stocks WHERE username = %s AND stock_symbol = %s", (user_id, stock_symbol))
            conn.commit()
            try:
                stock_quantity = cursor.fetchall()
            except:
                conn.rollback()
                return
            stocks_to_sell = int(float(amount)/price)
            if stock_quantity != []:
                # User owns enough of the stock to sell the specified amount
                if stock_quantity[0][0] >= stocks_to_sell and stocks_to_sell != 0:
                    cursor.execute("UPDATE stocks SET stock_quantity = stock_quantity - %s WHERE username = %s AND stock_symbol = %s", (stocks_to_sell, user_id, stock_symbol,))
                    conn.commit()
                    cursor.execute("INSERT INTO reserved (type, username, stock_symbol, stock_quantity, price, amount, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s);", ('sell', user_id, stock_symbol, stocks_to_sell, price, amount, round(time.time(), 5),))
                    conn.commit() 

                    # create timer, when timer finishes have it cancel the buy
                    #threading.Timer(QUOTE_LIFESPAN, buy_timeout, args=(user_id, stock_symbol, amount, cursor, conn)).start()
                else:
                    error = ErrorEvent()
                    attributes = {
                        "timestamp": int(time.time() * 1000), 
                        "server": "DDJK",
                        "transactionNum": transaction_num,
                        "command": "BUY",
                        "username": user_id,
                        "errorMessage": "User either does not own enough of the stock requested, or the stock is worth more than the price requested to sell" 
                    }
                    error.updateAll(**attributes)
                    XMLTree.append(error)
                return
            else:
                error = ErrorEvent()
                attributes = {
                    "timestamp": int(time.time() * 1000), 
                    "server": "DDJK",
                    "transactionNum": transaction_num,
                    "command": "BUY",
                    "username": user_id,
                    "errorMessage": "No stock of this type to sell" 
                }
                error.updateAll(**attributes)
                XMLTree.append(error)
                return
    # USER DOESN"T EXIST
    error = ErrorEvent()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "BUY",
        "username": user_id,
        "errorMessage": "User does not exist" 
    }
    error.updateAll(**attributes)
    XMLTree.append(error)
    return

def commit_sell(transaction_num, user_id, cursor, conn):
    cursor.execute('SELECT * FROM reserved WHERE type = %s AND username = %s AND timestamp > %s;', ('sell', user_id, round(time.time(), 5)-60))
    conn.commit()

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "COMMIT_SELL",
        "username": user_id
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    # NO SELL TO COMMIT
    try:
        if cursor.fetchall() == []:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": transaction_num,
                "command": "BUY",
                "username": user_id,
                "errorMessage": "No SELL to commit" 
            }
            error.updateAll(**attributes)
            XMLTree.append(error)
        # SELL TO COMMIT
        else:
            cursor.execute( 'SELECT reservationid, stock_symbol, stock_quantity, amount, price      '
                            'FROM reserved                                                          '
                            'WHERE type = %s                                                        '
                            'AND username = %s                                                      '
                            'AND timestamp = (SELECT MAX(timestamp)                                 '
                            '                 FROM reserved                                         '
                            '                 WHERE type = %s                                       '
                            '                 AND username = %s);                                   '
                            , ('sell', user_id, 'sell', user_id))
            conn.commit()

            try:
                elements = cursor.fetchone()
            except:
                conn.rollback()
                return
            reservationid = elements[0]
            stock_symbol = elements[1]
            stock_quantity = elements[2]
            amount = elements[3]
            price = elements[4]

            cursor.execute('UPDATE users SET balance = balance + %s where username = %s', (stock_quantity*price, user_id))
            conn.commit()

            transaction = AccountTransaction()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": transaction_num,
                "action": "add", 
                "username": user_id,
                "funds": float(stock_quantity*price)
            }
            transaction.updateAll(**attributes)
            XMLTree.append(transaction)

            cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
            conn.commit()        
        return
    except:
        conn.rollback()
        pass

def cancel_sell(transaction_num, user_id, cursor, conn):
    cursor.execute('SELECT reservationid, stock_symbol, stock_quantity FROM reserved WHERE type = %s AND username = %s AND timestamp = (SELECT MAX(timestamp) FROM reserved WHERE type = %s AND username = %s);', ('sell', user_id, 'sell', user_id))
    conn.commit()

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "CANCEL_SELL",
        "username": user_id
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    try:
        elements = cursor.fetchone()
    except:
        conn.rollback()
        return
    if elements is None:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "errorMessage": "No SELL to cancel" 
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        
        return
    reservationid = elements[0]
    stock_symbol = elements[1]
    stock_quantity = elements[2]
    cursor.execute("UPDATE stocks SET stock_quantity = stock_quantity + %s WHERE username = %s AND stock_symbol = %s", (stock_quantity, user_id, stock_symbol))
    conn.commit()
    cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
    conn.commit()
    return 

# set_buy_amount allows a user to set a dollar amount of stock to buy.  This must be followed
# by set_buy_trigger() before the trigger goes 'live'. 
def set_buy_amount(transaction_num, user_id, stock_symbol, amount, cursor, conn):
    amount = float(amount)

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "SET_BUY_AMOUNT",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": amount
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    # Does SET_BUY order exist for this user/stock combo?
    cursor.execute( 'SELECT transaction_amount  '
                    'FROM triggers              '
                    'WHERE username = %s        '
                    'AND stock_symbol = %s      '
                    'AND type = %s;             '
                    ,(user_id, stock_symbol, 'buy')) 
    try:
        existing_setbuy_amount = cursor.fetchone() # this is a tuple containing 1 string or None
    except:
        conn.rollback()
        return
    setbuy_exists = None # placeholder value, will become True/False
    difference = 0
    if existing_setbuy_amount is None:
        setbuy_exists = False
        difference = amount
    else:
        setbuy_exists = True
        difference = amount - float(existing_setbuy_amount[0]) #convert tuple containing string into float
    # confirm that the user has the appropriate funds in their account
    cursor.execute( 'SELECT balance         '
                    'FROM users             '
                    'WHERE username = %s    '
                    ,(user_id,))
    try:
        balance = float(cursor.fetchone()[0])
    except:
        conn.rollback()
        return
    print('balance of ', user_id, ': ', balance, "and type: ", type(balance))
    if balance < difference:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "SET_BUY_AMOUNT",
            "username": user_id,
            "errorMessage": "Insufficient Funds" 
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        return
    else:   # balance > difference, so create the SET_BUY order
        print("balance is sufficient")
        # adjust member's account balance
        cursor.execute(     'UPDATE users SET balance = balance - %s        '
                            'WHERE username = %s                            '
                            ,(difference, user_id))

        transaction = AccountTransaction()
        if difference > 0: # money is to be removed from user account 
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": transaction_num,
                "action": "remove", 
                "username": user_id,
                "funds": float(difference)
            }
        else : # difference < 0, therefore money is being refunded back into user account
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": transaction_num,
                "action": "add", 
                "username": user_id,
                "funds": float(-difference) # difference is negative, so the log shows add of a positive
            }
        transaction.updateAll(**attributes)
        XMLTree.append(transaction)        
        
        # if the order existed already, update it with the new BUY_AMOUNT, else create new record
        if setbuy_exists:
            cursor.execute( 'UPDATE triggers                                '
                            'SET transaction_amount = %s,                   '
                            '    transaction_number = %s                    '
                            'WHERE username = %s                            '
                            'AND stock_symbol = %s                          '
                            'AND type = %s;                                 '
                            ,(amount, transaction_num, user_id, stock_symbol, 'buy'))
        else: # setbuy_exists = False
            cursor.execute( ' INSERT INTO triggers          '
                            ' (username,                    '
                            ' stock_symbol,                 '
                            ' type,                         '
                            ' transaction_amount,           '
                            ' transaction_number)           ' 
                            'VALUES (%s, %s, %s, %s, %s);   '
                            ,(user_id, stock_symbol, 'buy', amount, transaction_num))
        conn.commit()
    return

def cancel_set_buy(transaction_num, user_id, stock_symbol, cursor, conn):
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "CANCEL_SET_BUY",
        "username": user_id,
        "stockSymbol": stock_symbol
    }
    command.updateAll(**attributes)
    XMLTree.append(command)
    
    cursor.execute( 'SELECT transaction_amount from triggers    '
                    'WHERE username = %s                        '
                    'AND stock_symbol = %s                      ' 
                    'AND type = %s;                             '
                    ,(user_id, stock_symbol, 'buy'))
    try:
        result = cursor.fetchone()
    except:
        conn.rollback()
        return
    if result is None:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "errorMessage": "SET_BUY does not exist, no action taken"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)

        print("SET_BUY does not exist, no action taken")
        return
    else:
        print("SET_BUY being cancelled...")
        cursor.execute( 'DELETE FROM triggers   '
                        'WHERE username = %s    '
                        'AND stock_symbol = %s  '
                        'AND type = %s;         '
                        ,(user_id, stock_symbol, 'buy'))
        amount_to_refund = float(result[0])
        print("refund size:", amount_to_refund)
        cursor.execute( 'UPDATE users SET balance = balance + %s    '
                        'WHERE username = %s                        '
                        ,(amount_to_refund, user_id))
        conn.commit()

        transaction = AccountTransaction()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "action": "add", 
            "username": user_id,
            "funds": float(amount_to_refund)
        }
        transaction.updateAll(**attributes)
        XMLTree.append(transaction)

    return 

def set_buy_trigger(transaction_num, user_id, stock_symbol, amount, cursor, conn):
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "SET_BUY_TRIGGER",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    XMLTree.append(command)
    
    cursor.execute( 'SELECT transaction_amount FROM triggers        '
                    'WHERE username = %s    '
                    'AND stock_symbol = %s  '
                    'AND type = %s;         '
                    ,(user_id, stock_symbol, 'buy'))
    try:
        result = cursor.fetchone()
    except:
        conn.rollback()
        return
    if result is None:

        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "errorMessage": "SET_BUY does not exist, no action taken"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        return
    else:
        cursor.execute( ' UPDATE triggers               '
                        ' SET trigger_amount = %s,      '
                        '     transaction_number = %s  '
                        ' WHERE username = %s           '
                        ' AND stock_symbol = %s         '
                        ' AND type = %s;                '
                        ,(amount, transaction_num, user_id, stock_symbol, 'buy'))
        conn.commit()
    return 

#TODO: if set_sell of this stock already exists, account for that stock when
#      determining whether user owns enough stock to create set_sell order
#   Also, it is now apparent this logic needs to change.  amount is a dollar amount, this has 
#   been written under the assumption that it is a number of stock
def set_sell_amount(transaction_num, user_id, stock_symbol, amount, cursor, conn):
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "SET_SELL_AMOUNT",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    # verify amount is an integer value
    try:
        amount = int(amount)
    except ValueError:
        print('invalid amount of stock, must input integer values to sell')
        return

    # check if user owns enough stock to carry out this transaction
    cursor.execute( 'SELECT stock_quantity from stocks      '
                    'WHERE username = %s                    '
                    'AND stock_symbol = %s;                 '
                    ,(user_id, stock_symbol))
    try:
        shares_owned = cursor.fetchone()
    except:
        conn.rollback()
        return
    if shares_owned is None:
        
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "errorMessage": "User owns no shares of this type"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        return
    elif shares_owned[0] < amount:
        
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "errorMessage": "User dooes not own enough shares of this type"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)       
        return
    else:   # user owns sufficient shares to proceed, so remove them from user account and create order
        cursor.execute( ' UPDATE stocks                 '
                        ' SET stock_quantity = %s      '
                        ' WHERE username = %s           '
                        ' AND stock_symbol = %s         '
                        ,(amount, user_id, stock_symbol))
        # Does SET_SELL order exist for this user/stock combo?  If yes, modify record, else create new one
        cursor.execute( 'SELECT transaction_amount     '
                        'FROM triggers              '
                        'WHERE username = %s        '
                        'AND stock_symbol = %s      '
                        'AND type = %s;             '
                        ,(user_id, stock_symbol, 'sell')) 
        try:
            result = cursor.fetchone() # this is a tuple containing 1 string or None
        except:
            conn.rollback()
            return
        if result is None:
            cursor.execute( ' INSERT INTO triggers                                                      '
                            ' (username, stock_symbol, type, transaction_amount, transaction_number)    ' 
                            ' VALUES (%s, %s, %s, %s, %s);                                              '
                            ,(user_id, stock_symbol, 'sell', amount, transaction_num))
        else: #modify existing record
            cursor.execute( ' UPDATE triggers SET amount = amount + %s, transaction_number = %s '
                            ' WHERE username = %s                                               '
                            ' AND stock_symbol = %s                                             '
                            ' AND type = %s                                                     '
                            ,(amount, transaction_num, user_id, stock_symbol, 'sell'))
        conn.commit()
    return

def set_sell_trigger(transaction_num, user_id, stock_symbol, amount, cursor, conn):
    
    try:
        command = UserCommand()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "SET_SELL_TRIGGER",
            "username": user_id,
            "stockSymbol": stock_symbol,
            "funds": float(amount)
        }
        command.updateAll(**attributes)
        XMLTree.append(command)
    except:
        return
    
    cursor.execute( 'SELECT transaction_amount from triggers        '
                    'WHERE username = %s    '
                    'AND stock_symbol = %s  '
                    'AND type = %s;         '
                    ,(user_id, stock_symbol, 'sell'))
    try:
        result = cursor.fetchone()
    except:
        conn.rollback()
        return
    if result is None:

        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "errorMessage": "SET_SELL does not exist"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        return
    else:
        cursor.execute( ' UPDATE triggers SET trigger_amount = %s,  '
                        '        transaction_number = %s            '  
                        ' WHERE username = %s                       '
                        ' AND stock_symbol = %s                     '
                        ' AND type = %s;                            '
                        ,(amount, transaction_num, user_id, stock_symbol, 'sell'))
        conn.commit()
    return 

def cancel_set_sell(transaction_num, user_id, stock_symbol, cursor, conn):
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "CANCEL_SET_SELL",
        "username": user_id,
        "stockSymbol": stock_symbol
    }
    command.updateAll(**attributes)
    XMLTree.append(command)
    
    cursor.execute( 'SELECT transaction_amount FROM triggers   '
                    'WHERE username = %s                    '
                    'AND stock_symbol = %s;                '
                    ,(user_id, stock_symbol))
    try:
        result = cursor.fetchall()
    except:
        conn.rollback()
        return
    if len(result) == 0:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "errorMessage": "Order does not exist"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        return
    else:
        stock_amount_to_refund = result[0][0]
        print("order exists, will cancel it")
        cursor.execute('DELETE FROM triggers   '
                        'WHERE username = %s    '
                        'AND stock_symbol = %s  '
                        'AND type = %s;         '
                        ,(user_id, stock_symbol, 'sell'))
        # refund stocks to user
        cursor.execute( 'UPDATE stocks SET stock_quantity = stock_quantity + %s '
                        'WHERE username = %s                                    '
                        'AND stock_symbol = %s;                                 '
                        ,(stock_amount_to_refund, user_id, stock_symbol))
    return 

# this method is called by an extra thread.  Every QUOTE_LIFESPAN period of time it goes
# through the triggers table.  For any row that posesses a trigger_value, a quote is
# obtained for that stock and if appropriate the buy/sell is triggered
def trigger_maintainer(cursor, conn):
    cursor.execute('SELECT * FROM triggers WHERE trigger_amount IS NOT NULL;')
    try:
        results = cursor.fetchall() # NOTE: this will not scale - we may have HUGE numbers of rows later
                                    # I've done this now though to avoid having stuff on cursor's buffer
    except:
        conn.rollback()
        return
    print("running trigger_maintainer")
    for row in results:
        print("row from 'triggers' as it is read:", row)
        user_id = row[0]
        stock_symbol = row[1]
        buy_or_sell = row[2]
        trigger_amount = row[3]
        transaction_amount = row[4]
        transaction_num= row[5]
        current_price = quote(transaction_num, user_id, stock_symbol)[0]

        # Debugging data
        print("row details: user_id:", user_id, 
            "stock_symbol:", stock_symbol, 
            "buy_or_sell:", buy_or_sell, 
            "trigger_amount:", trigger_amount, 
            "transaction_amount:", transaction_amount, 
            "transaction_number: ", transaction_num, 
            "current_price:", current_price)

        if buy_or_sell == 'buy' and current_price <= trigger_amount: # trigger the buy
            num_stocks_to_buy = int(transaction_amount/current_price)
            leftover_cash = transaction_amount - (current_price * num_stocks_to_buy)

            # if user already had this stock, then update amount
            cursor.execute( 'SELECT stock_quantity from stocks  '
                            'WHERE username = %s                '
                            'AND stock_symbol = %s              '
                            ,(user_id, stock_symbol))
            try:
                result = cursor.fetchone()
            except:
                conn.rollback()
                return

            if result is None: # the user had none of this stock previously
                cursor.execute( 'INSERT INTO stocks (username, stock_symbol, stock_quantity)    ' 
                                'VALUES (%s, %s, %s);                                       '
                                ,(user_id, stock_symbol, num_stocks_to_buy))
            else: # the user already had this type of stock

                # purchase the stocks
                cursor.execute( 'UPDATE stocks                              '
                                'SET stock_quantity = stock_quantity + %s   '
                                'WHERE username = %s                        '
                                'AND stock_symbol = %s;                     '
                                ,(num_stocks_to_buy, user_id, stock_symbol))

            # credit user account leftover cash
            cursor.execute( 'UPDATE users SET balance = balance + %s        '
                            'WHERE username = %s                            '
                            ,(leftover_cash, user_id))


            transaction = AccountTransaction()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": transaction_num,
                "action": "add", 
                "username": user_id,
                "funds": float(leftover_cash)
            }
            transaction.updateAll(**attributes)
            XMLTree.append(transaction)

            # remove the trigger
            cursor.execute( 'DELETE FROM triggers   '
                            'WHERE username = %s     '
                            'AND stock_symbol = %s  '
                            'AND type = %s;         '
                            ,(user_id, stock_symbol, buy_or_sell))
            conn.commit()

        # NOTE: this method is still using the incorrect logic - it assums amount == num of stock
        # When making the C++ version, correct the logic to assume amount == dollar amount
        elif buy_or_sell == 'sell' and current_price >= trigger_amount: # trigger the sell
            cash_from_sale = current_price * transaction_amount
            # credit user account from sale
            cursor.execute( 'UPDATE users SET balance = balance + %s    '
                            'WHERE username = %s;                       '
                            ,(cash_from_sale, user_id))

            transaction = AccountTransaction()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": transaction_num,
                "action": "add", 
                "username": user_id,
                "funds": float(cash_from_sale)
            }
            transaction.updateAll(**attributes)
            XMLTree.append(transaction)


            # remove the trigger
            cursor.execute( 'DELETE FROM triggers   '
                            'WHERE username = %s     '
                            'AND stock_symbol = %s  '
                            'AND type = %s;         '
                            ,(user_id, stock_symbol, buy_or_sell))

    # recurse, but using another thread.  I'm not sure, but I believe this avoids busy-waiting 
    # even on the new thread.  This needs more looking into to be sure if it's optimal
    #threading.Timer(QUOTE_LIFESPAN, trigger_maintainer, args=(cursor, conn)).start()

def dumplog(transaction_num, filename):
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "DUMPLOG"
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    # todo: verify if this can be removed - Daniel
    #time.sleep(30) # hack - fix me
    #XMLTree.write(filename)

def dumplog_user(transaction_num, user_id, filename):
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "DUMPLOG",
        "username": user_id
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    # This basically won't work at all atm - Daniel
    #time.sleep(30) # hack - fix me
    #XMLTree.writeFiltered(filename, user_id)


def display_summary(transaction_num, user_id):
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "DISPLAY_SUMMARY",
        "username": user_id
    }
    command.updateAll(**attributes)
    XMLTree.append(command)


def main():
    cursor, conn = initdb()

    # THIS HAS BEEN MOVED TO initdb()
    #start the trigger maintainer thread
#    threading.Timer(QUOTE_LIFESPAN, trigger_maintainer, args=(cursor, conn)).start()

    while True:
        var = input("Enter a command: ")
        command = var.split(' ', 1)[0]
        #ADD Command
        if command == "ADD":
            try:
                command, user_id, amount = var.split()
            except ValueError:
                print("Invalid Input. <ADD, USER_ID, AMOUNT>")
            else:    
                add(user_id, amount, cursor, conn)
        #BUY Command
        elif command == "BUY":
            try:
                command, user_id, stock_symbol, amount = var.split()
            except ValueError:
                print("Invalid Input. <BUY USER_ID STOCK_SYMBOL AMOUNT>")
            else:    
                buy(user_id, stock_symbol, amount, cursor, conn)
        elif command == "COMMIT_BUY":
            try:
                command, user_id = var.split()
            except ValueError:
                print("Invalid Input. <COMMIT_BUY USER_ID>")
            else:    
                commit_buy(user_id, cursor, conn)
        elif command == "CANCEL_BUY":
            try:
                command, user_id = var.split()
            except ValueError:
                print("Invalid Input. <CANCEL_BUY USER_ID>")
            else:    
                cancel_buy(user_id, cursor, conn)
        elif command == "SELL":
            try:
                command, user_id, stock_symbol, amount = var.split()
            except ValueError:
                print("Invalid Input. <SELL USER_ID STOCK_SYMBOL AMOUNT>")
            else:    
                sell(user_id, stock_symbol, amount, cursor, conn)
        elif command == "COMMIT_SELL":
            try:
                command, user_id = var.split()
            except ValueError:
                print("Invalid Input. <COMMIT_SELL USER_ID>")
            else:    
                commit_sell(user_id, cursor, conn)
        elif command == "SET_BUY_AMOUNT":
            try:
                command, user_id, stock_symbol, amount = var.split()
            except ValueError:
                print("Invalid input.  <SET_BUY_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
            else:
                set_buy_amount(user_id, stock_symbol, amount, cursor, conn)
        elif command == "CANCEL_SET_BUY":
            try:
                command, user_id, stock_symbol = var.split()
            except ValueError:
                print("Invalid input.  <CANCEL_SET_BUY USER_ID STOCK_SYMBOL>")
            else:
                cancel_set_buy(user_id, stock_symbol, cursor, conn)
        elif command == "SET_BUY_TRIGGER":
            try:
                command, user_id, symbol, amount = var.split()
            except ValueError:
                print("Invalid input. <SET_BUY_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
            else:
                set_buy_trigger(user_id, symbol, amount, cursor, conn)
        elif command == "SET_SELL_AMOUNT":
            try:
                command, user_id, stock_symbol, amount = var.split()
            except ValueError:
                print("Invalid input.  <SET_SELL_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
            else:
                set_sell_amount(user_id, stock_symbol, amount, cursor, conn)
        elif command == "CANCEL_SET_SELL":
            try:
                command, user_id, stock_symbol = var.split()
            except ValueError:
                print("Invalid input.  <CANCEL_SET_SELL USER_ID STOCK_SYMBOL>")
            else:
                cancel_set_sell(user_id, stock_symbol, cursor, conn)
        elif command == "SET_SELL_TRIGGER":
            try:
                command, user_id, symbol, amount = var.split()
            except ValueError:
                print("Invalid input. <SET_SELL_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
            else:
                set_sell_trigger(user_id, symbol, amount, cursor, conn)
        elif command == "CANCEL_SELL":
            try:
                command, user_id = var.split()
            except ValueError:
                print("Invalid Input. <COMMIT_SELL USER_ID>")
            else:    
                cancel_sell(user_id, cursor, conn)        
        elif command == "DUMPLOG":
            try:
                command, user_id, filename = var.split()
            except ValueError:
                try:
                    command, filename = var.split()
                except ValueError:
                    print("Invalid Input. Functionality: <DUMPLOG FILENAME> or <DUMPLOG USERNAME FILENAME")
                else: 
                    dumplog(filename)
            else:    
                dumplog_user(user_id, filename)   
        elif command == "quit":
            break
        else:
            print("Invalid Command")

        print("USERS TABLE")
        cursor.execute("SELECT * FROM USERS;")
        conn.commit()
        print(cursor.fetchall())

        print("RESERVED TABLE")
        cursor.execute("SELECT * FROM RESERVED;")
        conn.commit()
        print(cursor.fetchall())

        print("STOCKS TABLE")
        cursor.execute("SELECT * FROM STOCKS;")
        conn.commit()
        print(cursor.fetchall())


    
    closedb(cursor)

if __name__ == '__main__':
    main()
