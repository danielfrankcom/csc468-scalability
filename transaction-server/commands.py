import psycopg2
import time, threading
import random # used to gen random numbers in get_quote()

QUOTE_LIFESPAN = 10.0 # period of time a quote is valid for (will be 60.0 for deployed software)
accounts = []
cached_quotes = {}

def initdb():
    conn = None
    try:
        # Setting connection params:
        psql_user = 'postgres'
        psql_db = 'postgres'
        psql_password = 'gg'
        psql_server = 'localhost'
        psql_port = 5432
        
        print('Connecting...')
        conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

        cursor = conn.cursor()
        
        # Empty out all tables in the database
        cursor.execute( 'DROP TABLE IF EXISTS stocks;   '
                        'DROP TABLE IF EXISTS reserved; '
                        'DROP TABLE IF EXISTS triggers; '
                        'DROP TABLE IF EXISTS users;    ')
        conn.commit()

        # Recreate all tables in the database
        cursor.execute( 'CREATE TABLE users                               '
                        '(username VARCHAR(20) NOT NULL PRIMARY KEY,      '
                        'balance FLOAT NOT NULL);                        ')
        conn.commit()

        cursor.execute( 'CREATE TABLE stocks                                 '
                        '(username VARCHAR(20) references users(username),   '
                        'stock_symbol VARCHAR(3) NOT NULL,                   '
                        'stock_quantity INT NOT NULL,                        '
                        'PRIMARY KEY (username, stock_symbol));              ')
        conn.commit()

        cursor.execute( 'CREATE TABLE reserved                                      '
                        '(reservationid SERIAL PRIMARY KEY,                         '
                        'type VARCHAR(5) NOT NULL,                                  '
                        'username VARCHAR(20) references users(username),           '
                        'stock_symbol VARCHAR(3) NOT NULL,                          '
                        'stock_quantity INT NOT NULL,                               '
                        'price FLOAT NOT NULL,                                      '
                        'amount FLOAT NOT NULL,                                     '
                        'timestamp FLOAT NOT NULL);                                 ')      

        cursor.execute( 'CREATE TABLE triggers                                      '
                        '(username VARCHAR(20) NOT NULL references users(username)  '
                        'ON DELETE CASCADE ON UPDATE CASCADE,                       '
                        'stock_symbol VARCHAR(3) NOT NULL,                          '
                        'trigger_amount FLOAT,                                      '
                        'purchase_amount FLOAT NOT NULL,                            '
                        'PRIMARY KEY (username, stock_symbol));                     ')
        conn.commit()
        return cursor, conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def closedb(cursor):
    cursor.close()

def add(user_id, amount, cursor, conn):
    cursor.execute('SELECT username FROM users;')
    conn.commit()

    if cursor.fetchall() == []:
        cursor.execute('INSERT INTO users VALUES (%s, %s)', (user_id, amount))
        conn.commit()
        return
    else:
        cursor.execute('SELECT username FROM users;')
        conn.commit()
        for i in cursor.fetchall():
            if i[0] == user_id:
                cursor.execute('UPDATE users SET balance = balance + %s where username = %s;', (amount, user_id))
                conn.commit()
                return
         
        cursor.execute('INSERT INTO users VALUES (%s, %s)', (user_id, amount))
        conn.commit()
        return

# get_quote() is used to directly acquire a quote from the quote server (eventually)
# for now, this is a placeholder function, and returns a random value between
# 1.0 and 10.0. 
def get_quote(user_id, stock_symbol):
        time_of_quote = round(time.time(), 5)
        print("Getting quote from quote server for", stock_symbol, "at time: ", time_of_quote)
        new_price = round(random.uniform(1.0, 10.0), 2)
        return new_price, time_of_quote        

# quote() is called when a client requests a quote.  It will return a valid price for the
# stock as requested, but this value will either come from cached_quotes or from q hit
# to the quote server directly.  In the case of the latter, the new quote will be put
# in the cached_quotes dictionary
def quote(user_id, stock_symbol):
    cryptokey = '123450ABCDE' # placeholder, to be used until legacy quote servers are working
    # if not stock_symbol in cached_quotes.keys() or ((time.time() - cached_quotes[stock_symbol][1]) > QUOTE_LIFESPAN):
        # get quote from server
    new_price, time_of_quote = get_quote(user_id, stock_symbol)
    # cached_quotes[stock_symbol] = (new_price, time_of_quote)
    return new_price, stock_symbol, user_id, time_of_quote, cryptokey
#        return "1,ABC,Jaime,1234567,1234567890"
    # else: #the cached price is valid, return that
        # price = cached_quotes[stock_symbol][0] 
        # time_of_quote = cached_quotes[stock_symbol][1] 
        # return price, stock_symbol, user_id, time_of_quote, cryptokey 
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
    result = cursor.fetchone()
    if result is None: #order has already been manually confirmed or cancelled
        print("Timer for order is up, but the order is already gone")
        return
    else: # the reservation still exists, so delete it and refund the cash back to user's account
        reservationid = result[0]
        reserved_cash = result[6]
        cursor.execute('DELETE FROM reserved WHERE reservationid = %s;', (str(reservationid,)))    
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
    
    price, stock_symbol, user_id, time_of_quote, cryptokey = quote(user_id, stock_symbol)

    for i in cursor.fetchall():
        if i[0] == user_id:
            # USER EXISTS
            cursor.execute("SELECT balance FROM users WHERE username = %s", (user_id,))
            conn.commit()
            balance = cursor.fetchone()
            if balance[0] >= float(amount):
                # CAN AFFORD THE STOCK
                cursor.execute("UPDATE users SET balance = balance - %s WHERE username = %s;", (float(amount), user_id))
                conn.commit()
                cursor.execute("INSERT INTO reserved (type, username, stock_symbol, stock_quantity, price, amount, timestamp) VALUES (%s, %s, %s, %s, %s, %s, %s);", ('buy', user_id, stock_symbol, int(float(amount)/price), price, amount, round(time.time(), 5),))
                conn.commit() 

                # create timer, when timer finishes have it cancel the buy
                threading.Timer(QUOTE_LIFESPAN, buy_timeout, args=(user_id, stock_symbol, amount, cursor, conn)).start()
            else:
                print("Insufficient Funds")
            return
    # USER DOESN"T EXIST
    print("User does not exist")
    return

def commit_buy(user_id, cursor, conn):
    cursor.execute('SELECT * FROM reserved WHERE type = %s AND username = %s AND timestamp > %s;', ('buy', user_id, round(time.time(), 5)-60))
    conn.commit()

    # NO BUY TO COMMIT
    if cursor.fetchall() == []:
        print("No buy to commit")
    # BUY TO COMMIT
    else:
        print("GOT HERE GOT HERE GOT HERE")
        cursor.execute('SELECT reservationid, stock_symbol, stock_quantity, amount, price FROM reserved WHERE type = %s AND username = %s AND timestamp = (SELECT MAX(timestamp) FROM reserved WHERE type = %s AND username = %s);', ('buy', user_id, 'buy', user_id))
        conn.commit()

        elements = cursor.fetchone()
        reservationid = elements[0]
        stock_symbol = elements[1]
        stock_quantity = elements[2]
        amount = elements[3]
        price = elements[4]
        
        # See if the user already owns any of this stock
        cursor.execute('SELECT * FROM stocks WHERE username = %s and stock_symbol = %s', (user_id, stock_symbol,))
        conn.commit()
        
        # The user doesn't own any of this stock yet
        if cursor.fetchall() == []:
            cursor.execute('INSERT INTO stocks (username, stock_symbol, stock_quantity) VALUES (%s, %s, %s);', (user_id, stock_symbol, stock_quantity))
            conn.commit()
        # The user already owns some of this stock
        else:
            cursor.execute('UPDATE stocks SET stock_quantity = stock_quantity + %s WHERE username = %s and stock_symbol = %s;', (stock_quantity, user_id, stock_symbol))
            conn.commit()
        
        print("adding ", amount-(price*stock_quantity), " back to the account")
        cursor.execute('UPDATE users SET balance = balance + %s WHERE username = %s', (amount-(price*stock_quantity), user_id))    
        conn.commit()       

        cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
        conn.commit()        
    return

def cancel_buy(user_id, cursor, conn):
    cursor.execute('SELECT reservationid, amount FROM reserved WHERE type = %s AND username = %s AND timestamp = (SELECT MAX(timestamp) FROM reserved WHERE type = %s AND username = %s);', ('buy', user_id, 'buy', user_id))
    conn.commit()
    elements = cursor.fetchone()
    if elements is None:    # no orders exist for this user
        return
    reservationid = elements[0]
    amount = elements[1]
    cursor.execute('UPDATE users SET balance = balance + %s where username = %s', (amount, user_id))
    conn.commit()
    cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
    conn.commit()
    return 

def sell(user_id, stock_symbol, amount, cursor, conn):
    cursor.execute('SELECT username FROM users;')
    conn.commit()
    
    price, stock_symbol, user_id, time_of_quote, cryptokey = quote(user_id, stock_symbol)

    for i in cursor.fetchall():
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
                    print("User either does not own enough of the stock requested, or the stock is worth more than the price requested to sell")
                return
            else:
                print("No stock of this type to sell")
                return
    # USER DOESN"T EXIST
    print("User does not exist")
    return

def commit_sell(user_id, cursor, conn):
    cursor.execute('SELECT * FROM reserved WHERE type = %s AND username = %s AND timestamp > %s;', ('sell', user_id, round(time.time(), 5)-60))
    conn.commit()

    # NO SELL TO COMMIT
    if cursor.fetchall() == []:
        print("No sell to commit")
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
        cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
        conn.commit()        
    return

def cancel_sell(user_id):
    cursor.execute('SELECT reservationid, stock_quantity FROM reserved WHERE type = %s AND username = %s AND timestamp = (SELECT MAX(timestamp) FROM reserved WHERE type = %s AND username = %s);', ('sell', user_id, 'sell', user_id))
    conn.commit()
    elements = cursor.fetchone()
    if elements is None:    # no orders exist for this user
        return
    reservationid = elements[0]
    stock_quantity = elements[1]
    cursor.execute("UPDATE stocks SET stock_quantity = stock_quantity + %s WHERE username = %s AND stock_symbol = %s", (stock_quantity, user_id, stock_symbol))
    conn.commit()
    cursor.execute('DELETE FROM reserved WHERE reservationid = %s', (reservationid,))    
    conn.commit()
    return 

# set_buy_amount allows a user to set a dollar amount of stock to buy.  This must be followed
# by set_buy_trigger() before the trigger goes 'live'. 
def set_buy_amount(user_id, stock_symbol, amount, cursor, conn):
    amount = float(amount)
   
    # Does SET_BUY order exist for this user/stock combo?
    cursor.execute( 'SELECT purchase_amount     '
                    'FROM triggers              '
                    'WHERE username = %s        '
                    'AND stock_symbol = %s;     '
                    , (user_id, stock_symbol)) 
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
        print("insufficient funds, request denied")
        return
    else:   # balance > difference, so create the SET_BUY order
        print("balance is sufficient")
        # adjust member's account balance
        cursor.execute(     'UPDATE users SET balance = balance - %s        '
                            'WHERE username = %s                            '
                            ,(difference, user_id))
        
        # if the order existed already, update it with the new BUY_AMOUNT, else create new record
        if setbuy_exists:
            cursor.execute( 'UPDATE triggers SET purchase_amount = %s   '
                            'WHERE username = %s                        '
                            'AND stock_symbol = %s;                     '
                            ,(amount, user_id, stock_symbol))
        else: # setbuy_exists = False
            cursor.execute( 'INSERT INTO triggers (username, stock_symbol, purchase_amount) values (%s, %s, %s);', (user_id, stock_symbol, amount))
        conn.commit()
    return

""" PREVIOUS VERSION - DOESN'T ACCOUNT FOR EXISTING ORDER WHEN COMPARING TO ACCOUNT BALANCE
    LEAVE THIS ONE AROUND UNTIL THE ABOVE VERSION HAS BEEN TESTED FURTHER (JAN 30TH)
    # the logic of this next set of nested if statements is as follows:
    # - ensure the user has enough money in their account to reserve the amount needed for the SET_BUY
    # - if not enough money, cancel the request
    # - If they have enough, remove the money from their account 
    # - and determine whether there is already a SET_BUY order for this user and this stock
    # - If there is, then modify the SET_BUY amount to the amount provided and refund/debit the user account as required
    # - If not, create a new record in triggers
    if balance > amount: # note: '>= is not appropriate with floats, since 'equal to' is meaningless due to rounding errors.
        print("balance is sufficient")
        print("amount:", amount, "balance:", balance)
        # remove amount from user's account
        
        # check to see if this stock has a SET_BUY order for this user
        cursor.execute('SELECT purchase_amount FROM triggers WHERE username = %s AND stock_symbol = %s;', (user_id, stock_symbol))
        result = cursor.fetchone()
        if result is None: # no duplicate SET_BUY order exists
            cursor.execute('UPDATE users SET balance = balance - %s where username = %s', (amount, user_id))
            cursor.execute('INSERT INTO triggers (username, stock_symbol, purchase_amount) values (%s, %s, %s);', (user_id, stock_symbol, amount))
            conn.commit()
        else: # duplicate record exists, so modify the SET_BUY amount and correct the user's account balance
            prev_amount = float(result[0])
            difference = prev_amount - amount
            cursor.execute('UPDATE users SET balance = balance + %s where username = %s', (difference, user_id))
            cursor.execute('UPDATE triggers SET purchase_amount = %s where username = %s and stock_symbol = %s;', (amount, user_id, stock_symbol))
            conn.commit()
    else: # balance < amount, so deny the request
        print("funds not available")
        print("amount:", amount, "balance:", balance)
        return

    return 0
"""

def cancel_set_buy(user_id, stock_symbol, cursor, conn):
    cursor.execute( 'SELECT purchase_amount from triggers        '
                    'WHERE username = %s    '
                    'AND stock_symbol = %s; '
                    ,(user_id, stock_symbol))
    result = cursor.fetchone()
    if result is None:
        print("SET_BUY does not exist, no action taken")
        return
    else:
        print("SET_BUY being cancelled...")
        cursor.execute( 'DELETE FROM triggers   '
                        'WHERE username = %s    '
                        'AND stock_symbol = %s; '
                        , (user_id, stock_symbol))
        amount_to_refund = float(result[0])
        print("refund size:", amount_to_refund)
        cursor.execute( 'UPDATE users SET balance = balance + %s    '
                        'WHERE username = %s                        '
                        ,(amount_to_refund, user_id))
        conn.commit()
    return 

def set_buy_trigger(user_id, stock_symbol, amount):
    return 0

def set_sell_amount(user_id, stock_symbol, amount):
    return 0

def set_sell_trigger(user_id, stock_symbol, amount):
    return 0

def cancel_set_sell(user_id, stock_symbol):
    return 0

def dumplog(user_id, filename):
    return 0

def display_summary(user_id):
    return 0

def main():
    cursor, conn = initdb()
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
                print("Invalid Input.  <SET_BUY_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
            else:
                set_buy_amount(user_id, stock_symbol, amount, cursor, conn)
        elif command == "CANCEL_SET_BUY":
            try:
                command, user_id, stock_symbol = var.split()
            except ValueError:
                print("invalid Input.  <CANCEL_SET_BUY USER_ID STOCK_SYMBOL>")
            else:
                cancel_set_buy(user_id, stock_symbol, cursor, conn)
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

