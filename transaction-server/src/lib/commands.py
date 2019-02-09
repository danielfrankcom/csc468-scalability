import psycopg2
import socket, sys
import time, threading, datetime
import random # used to gen random numbers in get_quote()
from lib.xml_writer import *
from itertools import count

QUOTE_LIFESPAN = 60.0 # period of time a quote is valid for (will be 60.0 for deployed software)
accounts = []
cached_quotes = {}

XMLTree = LogBuilder()
transaction_number = count(1)

def initdb():
    conn = None
    try:
        # Setting connection params:
        psql_db = 'postgres'
        psql_user = 'postgres'
        psql_password = 'supersecure'
        psql_server = 'postgres'
        psql_port = 5432
        
        conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

        cursor = conn.cursor()

        #start the trigger maintainer thread
        threading.Timer(QUOTE_LIFESPAN, trigger_maintainer, args=(cursor, conn)).start()

        return cursor, conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def closedb(cursor):
    cursor.close()

def add(user_id, amount, cursor, conn):
    cursor.execute('SELECT username FROM users;')
    conn.commit()

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
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
    
    cursor.execute(function, (user_id, amount))
    conn.commit()
        
    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "action": "add", 
        "username": user_id,
        "funds": float(amount)
    }
    transaction.updateAll(**attributes)
    XMLTree.append(transaction)

# get_quote() is used to directly acquire a quote from the quote server (eventually)
# for now, this is a placeholder function, and returns a random value between
# 1.0 and 10.0. 
def get_quote(user_id, stock_symbol):

        # Create the socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        # Connect the socket
        s.connect(('quoteserve.seng.uvic.ca', 4444))

        # Send the user's query
        request = "{symbol},{user}\n".format(symbol=stock_symbol, user=user_id)
        s.send(request.encode())

        # Read and print up to 1k of data.
        data = s.recv(1024).decode().split("\n")[0]

        # close the connection, and the socket
        s.close()

        price, symbol, username, timestamp, cryptokey = data.split(",")
        return float(price), int(timestamp), cryptokey        

# quote() is called when a client requests a quote.  It will return a valid price for the
# stock as requested, but this value will either come from cached_quotes or from q hit
# to the quote server directly.  In the case of the latter, the new quote will be put
# in the cached_quotes dictionary
def quote(user_id, stock_symbol):
    # if not stock_symbol in cached_quotes.keys() or ((time.time() - cached_quotes[stock_symbol][1]) > QUOTE_LIFESPAN):
    # get quote from server
    new_price, time_of_quote, cryptokey = get_quote(user_id, stock_symbol)

    quote = QuoteServer()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "price": new_price, 
        "username": user_id,
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
    return
    print("\n\n\n" + str(repr(stock_symbol)) + "    " + str(type(stock_symbol)) + "\n\n\n")
    cursor.execute('SELECT * FROM reserved WHERE    '
        'type = %s AND                              '
        'username = %s AND                          '
        'stock_symbol = %s AND                      '
        'amount = %s;                               ',
        ('buy', user_id, unicode(str(stock_symbol), "utf-8"), dollar_amount))
    result = cursor.fetchone()
    if result is None: #order has already been manually confirmed or cancelled
        print("Timer for order is up, but the order is already gone")
        return
    else: # the reservation still exists, so delete it and refund the cash back to user's account
        reservationid = result[0]
        reserved_cash = result[6]
        cursor.execute('DELETE FROM reserved WHERE reservationid = %s;', (reservationid,))    
        cursor.execute('SELECT balance FROM users where username = %s;', (user_id,))
        result = cursor.fetchall()
        if result is None:
            print("Error - user does not exist!")
            return
        existing_balance = result[0][0]
        new_balance = existing_balance + reserved_cash
        cursor.execute('UPDATE users SET balance = %s WHERE username = %s;', (str(new_balance), (user_id)))
        conn.commit()        
        print('buy order timout - the following buy order is now cancelled: ', 
            user_id, stock_symbol, dollar_amount)

def buy(user_id, stock_symbol, amount, cursor, conn):
    cursor.execute('SELECT username FROM users;')
    conn.commit()
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "command": "BUY",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    XMLTree.append(command)
    
    price, stock_symbol, user_id, time_of_quote, cryptokey = quote(user_id, stock_symbol)

    result = None
    try:
        result = cursor.fetchall()
    except:
        result = []
    for i in result:
        if i[0] == user_id:
            # USER EXISTS
            cursor.execute("SELECT balance FROM users WHERE username = %s", (user_id,))
            conn.commit()
            balance = cursor.fetchone()
            if balance[0] >= float(amount):
                # CAN AFFORD THE STOCK
                cursor.execute("UPDATE users SET balance = balance - %s WHERE username = %s;", (float(amount), user_id))
                conn.commit()

                transaction = AccountTransaction()
                attributes = {
                    "timestamp": int(time.time() * 1000), 
                    "server": "DDJK",
                    "transactionNum": next(transaction_number),
                    "action": "remove", 
                    "username": user_id,
                    "funds": float(amount)
                }
                transaction.updateAll(**attributes)
                XMLTree.append(transaction)

                cursor.execute("INSERT INTO reserved (type, username, stock_symbol, stock_quantity, price, amount, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s);", ('buy', user_id, stock_symbol, int(float(amount)/price), price, amount, round(time.time(), 5),))
                conn.commit() 

                # create timer, when timer finishes have it cancel the buy
                threading.Timer(QUOTE_LIFESPAN, buy_timeout, args=(user_id, stock_symbol, amount, cursor, conn)).start()
            else:
                
                error = ErrorEvent()
                attributes = {
                    "timestamp": int(time.time() * 1000), 
                    "server": "DDJK",
                    "transactionNum": next(transaction_number),
                    "command": "BUY",
                    "username": user_id,
                    "stockSymbol": stock_symbol,
                    "funds": float(amount),
                    "errorMessage": "Insufficient Funds" 
                }
                error.updateAll(**attributes)
                XMLTree.append(error)
            return
    # USER DOESN"T EXIST
    error = ErrorEvent()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "command": "BUY",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount),
        "errorMessage": "User does not exist" 
    }
    error.updateAll(**attributes)
    XMLTree.append(error)
    return

def commit_buy(user_id, cursor, conn):
    cursor.execute('SELECT * FROM reserved WHERE type = %s AND username = %s AND timestamp > %s;', ('buy', user_id, round(time.time(), 5)-60))
    conn.commit()

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "command": "COMMIT_BUY",
        "username": user_id
    }
    command.updateAll(**attributes)
    XMLTree.append(command)
    
    
    # NO BUY TO COMMIT
    if cursor.fetchall() == []:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
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

        elements = cursor.fetchone()
        reservationid = elements[0]
        stock_symbol = elements[1]
        stock_quantity = elements[2]
        amount = elements[3]
        price = elements[4]
        
        
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
            "transactionNum": next(transaction_number),
            "action": "add", 
            "username": user_id,
            "funds": float(amount-(price*stock_quantity))
        }
        transaction.updateAll(**attributes)
        XMLTree.append(transaction)

        cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
        conn.commit()        
    return

def cancel_buy(user_id, cursor, conn):
    cursor.execute('SELECT reservationid, amount FROM reserved WHERE type = %s AND username = %s AND timestamp = (SELECT MAX(timestamp) FROM reserved WHERE type = %s AND username = %s);', ('buy', user_id, 'buy', user_id))
    conn.commit()

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "command": "CANCEL_BUY",
        "username": user_id
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    result = cursor.fetchall()
    print("\n\n\n\n" + str(len(result)) + "\n\n\n\n")
    if len(result) == 0:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
            "command": "BUY",
            "username": user_id,
            "errorMessage": "No BUY to cancel" 
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        
        return

    elements = result[0]
    reservationid = elements[0]
    amount = elements[1]
    cursor.execute('UPDATE users SET balance = balance + %s where username = %s', (amount, user_id))
    conn.commit()

    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "action": "add", 
        "username": user_id,
        "funds": float(amount)
    }
    transaction.updateAll(**attributes)
    XMLTree.append(transaction)

    cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
    conn.commit()
    return 

def sell(user_id, stock_symbol, amount, cursor, conn):
    cursor.execute('SELECT username FROM users;')
    conn.commit()
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "command": "SELL",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    price, stock_symbol, user_id, time_of_quote, cryptokey = quote(user_id, stock_symbol)

    result = None
    try:
        result = cursor.fetchall()
    except:
        result = []
    for i in result:
        # USER EXISTS
        if i[0] == user_id:
            cursor.execute("SELECT stock_quantity FROM stocks WHERE username = %s AND stock_symbol = %s", (user_id, stock_symbol))
            conn.commit()
            stock_quantity = cursor.fetchall()
            stocks_to_sell = int(float(amount)/price)
            if stock_quantity != []:
                # User owns enough of the stock to sell the specified amount
                if stock_quantity[0][0] >= stocks_to_sell and stocks_to_sell != 0:
                    cursor.execute("UPDATE stocks SET stock_quantity = stock_quantity - %s WHERE username = %s AND stock_symbol = %s", (stocks_to_sell, user_id, stock_symbol,))
                    conn.commit()
                    cursor.execute("INSERT INTO reserved (type, username, stock_symbol, stock_quantity, price, amount, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s);", ('sell', user_id, stock_symbol, stocks_to_sell, price, amount, round(time.time(), 5),))
                    conn.commit() 

                    # create timer, when timer finishes have it cancel the buy
                    threading.Timer(QUOTE_LIFESPAN, buy_timeout, args=(user_id, stock_symbol, amount, cursor, conn)).start()
                else:
                    error = ErrorEvent()
                    attributes = {
                        "timestamp": int(time.time() * 1000), 
                        "server": "DDJK",
                        "transactionNum": next(transaction_number),
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
                    "transactionNum": next(transaction_number),
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
        "transactionNum": next(transaction_number),
        "command": "BUY",
        "username": user_id,
        "errorMessage": "User does not exist" 
    }
    error.updateAll(**attributes)
    XMLTree.append(error)
    return

def commit_sell(user_id, cursor, conn):
    cursor.execute('SELECT * FROM reserved WHERE type = %s AND username = %s AND timestamp > %s;', ('sell', user_id, round(time.time(), 5)-60))
    conn.commit()

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "command": "COMMIT_SELL",
        "username": user_id
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    # NO SELL TO COMMIT
    if cursor.fetchall() == []:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
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

        elements = cursor.fetchone()
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
            "transactionNum": next(transaction_number),
            "action": "add", 
            "username": user_id,
            "funds": float(stock_quantity*price)
        }
        transaction.updateAll(**attributes)
        XMLTree.append(transaction)

        cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
        conn.commit()        
    return

def cancel_sell(user_id, cursor, conn):
    cursor.execute('SELECT reservationid, stock_symbol, stock_quantity FROM reserved WHERE type = %s AND username = %s AND timestamp = (SELECT MAX(timestamp) FROM reserved WHERE type = %s AND username = %s);', ('sell', user_id, 'sell', user_id))
    conn.commit()

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "command": "CANCEL_SELL",
        "username": user_id
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

    result = cursor.fetchall()
    print("\n\n\n\n" + str(len(result)) + "\n\n\n\n")
    if len(result) == 0:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
            "command": "BUY",
            "username": user_id,
            "errorMessage": "No SELL to cancel" 
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        
        return
    elements = result[0]
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
def set_buy_amount(user_id, stock_symbol, amount, cursor, conn):
    amount = float(amount)

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
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
                    , (user_id, stock_symbol, 'buy')) 
    existing_setbuy_amount = cursor.fetchone() # this is a tuple containing 1 string or None
    setbuy_exists = None # placeholder value, will become True/False
    difference = 0
    if existing_setbuy_amount is None:
        setbuy_exists = False
        difference = amount
    else:
        setbuy_exists = True
        difference = amount - float(existing_setbuy_amount[0]) #convert tuple containing string into float
    # confirm that the user has the appropriate funds in their account
    cursor.execute('SELECT balance from users where username = %s', (user_id,))
    balance = float(cursor.fetchone()[0])
    print('balance of ', user_id, ': ', balance, "and type: ", type(balance))
    if balance < difference:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
            "command": "BUY",
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
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
            "action": "remove", 
            "username": user_id,
            "funds": float(difference)
        }
        transaction.updateAll(**attributes)
        XMLTree.append(transaction)        
        
        # if the order existed already, update it with the new BUY_AMOUNT, else create new record
        if setbuy_exists:
            cursor.execute( 'UPDATE triggers SET transaction_amount = %s    '
                            'WHERE username = %s                            '
                            'AND stock_symbol = %s                          '
                            'AND type = %s;                                 '
                            ,(amount, user_id, stock_symbol, 'buy'))
        else: # setbuy_exists = False
            cursor.execute( 'INSERT INTO triggers (username, stock_symbol, type, transaction_amount)    ' 
                            'VALUES (%s, %s, %s, %s);                                                   '
                            ,(user_id, stock_symbol, 'buy', amount))
        conn.commit()
    return

def cancel_set_buy(user_id, stock_symbol, cursor, conn):
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
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
    result = cursor.fetchall()
    print("\n\n\n\n" + str(len(result)) + "\n\n\n\n")
    if len(result) == 0:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
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
        amount_to_refund = float(result[0][0])
        print("refund size:", amount_to_refund)
        cursor.execute( 'UPDATE users SET balance = balance + %s    '
                        'WHERE username = %s                        '
                        ,(amount_to_refund, user_id))
        conn.commit()

        transaction = AccountTransaction()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
            "action": "add", 
            "username": user_id,
            "funds": float(amount_to_refund)
        }
        transaction.updateAll(**attributes)
        XMLTree.append(transaction)

    return 

def set_buy_trigger(user_id, stock_symbol, amount, cursor, conn):
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "command": "SET_BUY_TRIGGER",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    XMLTree.append(command)
    
    cursor.execute( 'SELECT transaction_amount from triggers        '
                    'WHERE username = %s    '
                    'AND stock_symbol = %s  '
                    'AND type = %s;         '
                    ,(user_id, stock_symbol, 'buy'))
    result = cursor.fetchone()
    if result is None:

        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
            "command": "BUY",
            "username": user_id,
            "errorMessage": "SET_BUY does not exist, no action taken"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        return
    else:
        cursor.execute( 'UPDATE triggers SET trigger_amount = %s    '
                        'WHERE username = %s                        '
                        'AND stock_symbol = %s                      '
                        'AND type = %s;                             '
                        ,(amount, user_id, stock_symbol, 'buy'))
        conn.commit()
    return 

#TODO: if set_sell of this stock already exists, account for that stock when
#      determining whether user owns enough stock to create set_sell order
def set_sell_amount(user_id, stock_symbol, amount, cursor, conn):
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
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
    shares_owned = cursor.fetchone()
    if shares_owned is None:
        
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
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
            "transactionNum": next(transaction_number),
            "command": "BUY",
            "username": user_id,
            "errorMessage": "User dooes not own enough shares of this type"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)       
        return
    else:   # user owns sufficient shares to proceed, so remove them from user account and create order
        cursor.execute( 'UPDATE stocks SET stock_quantity = stock_quantity - %s '
                        'WHERE username = %s                                    '
                        'AND stock_symbol = %s                                  '
                        ,(amount, user_id, stock_symbol))
        # Does SET_SELL order exist for this user/stock combo?  If yes, modify record, else create new one
        cursor.execute( 'SELECT transaction_amount     '
                        'FROM triggers              '
                        'WHERE username = %s        '
                        'AND stock_symbol = %s      '
                        'AND type = %s;             '
                        ,(user_id, stock_symbol, 'sell')) 
        result = cursor.fetchone() # this is a tuple containing 1 string or None
        if result is None:
            cursor.execute( 'INSERT INTO triggers (username, stock_symbol, type, transaction_amount)   ' 
                            'VALUES (%s, %s, %s, %s);                                               '
                            ,(user_id, stock_symbol, 'sell', amount))
        else: #modify existing record
            cursor.execute( 'UPDATE triggers SET amount = amount + %s   '
                            'WHERE username = %s                        '
                            'AND stock_symbol = %s                      '
                            'AND type = %s                              '
                            ,(amount, user_id, stock_symbol, 'sell'))
        conn.commit()
    return

def set_sell_trigger(user_id, stock_symbol, amount, cursor, conn):
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
        "command": "SET_SELL_TRIGGER",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    XMLTree.append(command)
    
    cursor.execute( 'SELECT transaction_amount from triggers        '
                    'WHERE username = %s    '
                    'AND stock_symbol = %s  '
                    'AND type = %s;         '
                    ,(user_id, stock_symbol, 'sell'))
    result = cursor.fetchone()
    if result is None:

        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
            "command": "BUY",
            "username": user_id,
            "errorMessage": "SET_SELL does not exist"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        return
    else:
        cursor.execute( 'UPDATE triggers SET trigger_amount = %s    '
                        'WHERE username = %s                        '
                        'AND stock_symbol = %s                      '
                        'AND type = %s;                             '
                        ,(amount, user_id, stock_symbol, 'sell'))
        conn.commit()
    return 

def cancel_set_sell(user_id, stock_symbol, cursor, conn):
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": next(transaction_number),
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
    result = cursor.fetchall()
    print("\n\n\n\n" + str(len(result)) + "\n\n\n\n")
    if len(result) == 0:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": next(transaction_number),
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
    results = cursor.fetchall() # NOTE: this will not scale - we may have HUGE numbers of rows later
                                # I've done this now though to avoid having stuff on cursor's buffer
    print("running trigger_maintainer")
    for row in results:
        print(row)
        user_id = row[0]
        stock_symbol = row[1]
        buy_or_sell = row[2]
        trigger_amount = row[3]
        transaction_amount = row[4]
        current_price = quote(user_id, stock_symbol)[0]
        print("row details: user_id:", user_id, "stock_symbol:", stock_symbol, "buy_or_sell:", buy_or_sell, "trigger_amount:", trigger_amount, "transaction_amount:", transaction_amount, "current_price:", current_price)
        if buy_or_sell == 'buy' and current_price <= trigger_amount: # trigger the buy
            num_stocks_to_buy = int(transaction_amount/current_price)
            leftover_cash = transaction_amount - (current_price * num_stocks_to_buy)

            # if user already had this stock, then update amount
            cursor.execute( 'SELECT stock_quantity from stocks  '
                            'WHERE username = %s                '
                            'AND stock_symbol = %s              '
                            ,(user_id, stock_symbol))
            result = cursor.fetchone()

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
                "transactionNum": next(transaction_number),
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

        #TODO: implement the elif below
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
                "transactionNum": next(transaction_number),
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
    threading.Timer(QUOTE_LIFESPAN, trigger_maintainer, args=(cursor, conn)).start()

def dumplog(filename):
    XMLTree.write(filename)

def dumplog_user(user_id, filename):
    return 0

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
