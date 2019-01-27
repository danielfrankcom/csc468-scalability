import psycopg2

accounts = []

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

        cursor.execute( 'CREATE TABLE stocks                                               '
                        '(username VARCHAR(20) references users(username),                 '
                        'stock_symbol VARCHAR(3) NOT NULL,                                 '
                        'stock_quantity INT NOT NULL,                                      '
                        'PRIMARY KEY (username, stock_symbol));                            ')
        conn.commit()

        cursor.execute( 'CREATE TABLE reserved                                 '
                        '(username VARCHAR(20) references users(username),     '
                        'stock_symbol VARCHAR(3) NOT NULL,                     '
                        'amount FLOAT NOT NULL,                         '
                        'timestamp TIMESTAMP NOT NULL,                         '
                        'PRIMARY KEY (username, stock_symbol));                ')                        
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
        

def quote(user_id, stock_symbol):
    return "1,ABC,Jaime,1234567,1234567890"

def buy(user_id, stock_symbol, amount, cursor, conn):
    cursor.execute('SELECT username FROM users;')
    conn.commit()

    for i in cursor.fetchall():
        if i[0] == user_id:
            # USER EXISTS
            cursor.execute("SELECT balance FROM users WHERE username = %s", (user_id,))
            conn.commit()
            balance = cursor.fetchone()
            if balance[0] >= float(amount):
                # CAN AFFORD THE STOCK
                cursor.execute("SELECT username FROM reserved WHERE username = %s and stock_symbol = %s", (user_id, stock_symbol))
                conn.commit()

                if cursor.fetchall() == []:
                    cursor.execute("UPDATE users SET balance = balance - %s WHERE username = %s;", (int(amount), user_id))
                    conn.commit()
                    cursor.execute("INSERT INTO reserved VALUES (%s, %s, %s, %s);", (user_id, stock_symbol, amount, '2011-05-16 15:36:38',))
                    conn.commit() 
                else:
                    cursor.execute("UPDATE users SET balance = balance - %s WHERE username = %s;", (int(amount), user_id))
                    conn.commit()
                    cursor.execute("UPDATE reserved SET amount = amount + %s where username = %s;", (int(amount), user_id,))
                    conn.commit() 
                    cursor.execute("select * from users")
                    conn.commit()
                    print(cursor.fetchall())
                    cursor.execute("select * from reserved")
                    conn.commit()
                    print(cursor.fetchall())

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

