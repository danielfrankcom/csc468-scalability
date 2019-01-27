import psycopg2
import time   
import random # used to gen random numbers in get_quote()

QUOTE_LIFESPAN = 10.0 # period of time a quote is valid for (will be 60.0 for deployed software)
accounts = []
cached_quotes = {}

def initdb():
    conn = None
    try:
        # Setting connection params:
        psql_user = 'databaseuser'
        psql_db = 'postgres'
        psql_password = ''
        psql_server = 'localhost'
        psql_port = 5432
        
        print('Connecting...')
        conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

        cursor = conn.cursor()
        
        # Empty out all tables in the database
        cursor.execute( 'DROP TABLE IF EXISTS stocks;   '
                        'DROP TABLE IF EXISTS reserved; '
                        'DROP TABLE IF EXISTS users;    '
                        'DROP TABLE IF EXISTS triggers; ')
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
                        '(reservationid SERIAL PRIMARY KEY,    '
                        'username VARCHAR(20) references users(username),           '
                        'stock_symbol VARCHAR(3) NOT NULL,                          '
                        'stock_quantity INT NOT NULL,                               '
                        'amount FLOAT NOT NULL,                                     '
                        'timestamp TIMESTAMP NOT NULL);                              ')                        
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
    if not stock_symbol in cached_quotes.keys() or ((time.time() - cached_quotes[stock_symbol][1]) > QUOTE_LIFESPAN):
        # get quote from server
        new_price, time_of_quote = get_quote(user_id, stock_symbol)
        cached_quotes[stock_symbol] = (new_price, time_of_quote)
        return new_price, stock_symbol, user_id, time_of_quote, cryptokey
#        return "1,ABC,Jaime,1234567,1234567890"
    else: #the cached price is valid, return that
        price = cached_quotes[stock_symbol][0] 
        time_of_quote = cached_quotes[stock_symbol][1] 
        return price, stock_symbol, user_id, time_of_quote, cryptokey 
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
                cursor.execute("UPDATE users SET balance = balance - %s WHERE username = %s;", (int(amount), user_id))
                conn.commit()
                cursor.execute("INSERT INTO reserved (username, stock_symbol, stock_quantity, amount, timestamp) VALUES (%s, %s, %s, %s, now());", (user_id, stock_symbol, int(float(amount)/price), amount,))
                conn.commit() 
            else:
                print("Insufficient Funds")
            return
    # USER DOESN"T EXIST
    print("User does not exist")
    return 0

def commit_buy(user_id):
    return 0

def cancel_buy(user_id):
    return 0

def sell(user_id, stock_symbol, amount):
    return 0

def commit_sell(user_id):
    return 0

def cancel_sell(user_id):
    return 0

def set_buy_amount(user_id, stock_symbol, amount):
    return 0

def cancel_set_buy(user_id, stock_symbol):
    return 0

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
        if command == "add":
            try:
                command, user_id, amount = var.split()
            except ValueError:
                print("Invalid Input. <ADD, USER_ID, AMOUNT>")
            else:    
                add(user_id, amount, cursor, conn)
        #BUY Command
        elif command == "buy":
            try:
                command, user_id, stock_symbol, amount = var.split()
            except ValueError:
                print("Invalid Input. <ADD, USER_ID, STOCK_SYMBOL, AMOUNT>")
            else:    
                buy(user_id, stock_symbol, amount, cursor, conn)
        elif command == "quit":
            break
    closedb(cursor)

if __name__ == '__main__':
    main()

