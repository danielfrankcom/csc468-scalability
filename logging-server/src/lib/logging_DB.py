import psycopg2

class logging_DB(object):
    def __init__(self):
        """ Connect to the PostgreSQL database server """
        self.conn = None
        try:
            # connect to the PostgreSQL server
            print('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(host="logging-db",database="postgres",user="postgres",password="supersecure")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)

    def disconnect(self):
        self.conn.close()

    def userCommand(self,data):
        timestamp = data["timestamp"]
        server = data["server"]
        transaction_num = data["transaction_num"]
        command = data["command"]
        username = stock_symbol = funds = None
        if "username" in data:
            username = data["username"]
        if "stock_symbol" in data:
            stock_symbol = data["stock_symbol"]
        if "funds" in data:
            funds = data["funds"]
        
        cur = self.conn.cursor()
        sql = f"""INSERT INTO usercommands (timestamp, server, transaction_num, command, username, stock_symbol, funds)
                    VALUES (%s,%s,%s,%s,%s,%s,%s) """
        cur.execute(sql, (timestamp,server,transaction_num,command,username,stock_symbol,funds))
        self.conn.commit()
        cur.close()
        
    def quoteServer(self,data):
        timestamp = data["timestamp"]
        server = data["server"]
        transaction_num = data["transaction_num"]
        price = data["price"]
        stock_symbol = data["stock_symbol"]
        username = data["username"]
        quote_server_time = data["quote_server_time"]
        crypto_key = data["crypto_key"]

        cur = self.conn.cursor()
        sql = f"""INSERT INTO quoteservers (timestamp, server, transaction_num, price, stock_symbol, username, quote_server_time, crypto_key)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s) """
        cur.execute(sql, (timestamp,server,transaction_num,price,stock_symbol,username,quote_server_time,crypto_key))
        self.conn.commit()
        cur.close()

    def accountTransaction(self,data):
        timestamp = data["timestamp"]
        server = data["server"]
        transaction_num = data["transaction_num"]
        action = data["action"]
        username = data["username"]
        funds = None
        if "funds" in data:
            funds = data["funds"]

        cur = self.conn.cursor()
        sql = f"""INSERT INTO accounttransactions (timestamp, server, transaction_num, action, username, funds)
                    VALUES (%s,%s,%s,%s,%s,%s) """
        cur.execute(sql, (timestamp,server,transaction_num,action,username,funds))
        self.conn.commit()
        cur.close()

    def systemEvent(self,data):
        timestamp = data["timestamp"]
        server = data["server"]
        transaction_num = data["transaction_num"]
        command = data["command"]
        username = stock_symbol = funds = None
        if "username" in data:
            username = data["username"]
        if "stock_symbol" in data:
            stock_symbol = data["stock_symbol"]
        if "funds" in data:
            funds = data["funds"]

        cur = self.conn.cursor()
        sql = f"""INSERT INTO systemevents (timestamp, server, transaction_num, command, username, stock_symbol, funds)
                    VALUES (%s,%s,%s,%s,%s,%s,%s) """
        cur.execute(sql, (timestamp,server,transaction_num,command,username,stock_symbol,funds))
        self.conn.commit()
        cur.close()
        
    def errorEvent(self,data):
        timestamp = data["timestamp"]
        server = data["server"]
        transaction_num = data["transaction_num"]
        command = data["command"]
        username = stock_symbol = funds = error_message = None
        if "username" in data:
            username = data["username"]
        if "stock_symbol" in data:
            stock_symbol = data["stock_symbol"]
        if "funds" in data:
            funds = data["funds"]
        if "error_message" in data:
            error_message = data["error_message"]

        cur = self.conn.cursor()
        sql = f"""INSERT INTO errorevents (timestamp, server, transaction_num, command, username, stock_symbol, funds, error_message)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s) """
        cur.execute(sql, (timestamp,server,transaction_num,command,username,stock_symbol,funds,error_message))
        self.conn.commit()
        cur.close()

    def debugEvent(self,data):
        timestamp = data["timestamp"]
        server = data["server"]
        transaction_num = data["transaction_num"]
        command = data["command"]
        username = stock_symbol = funds = debug_message = None
        if "username" in data:
            username = data["username"]
        if "stock_symbol" in data:
            stock_symbol = data["stock_symbol"]
        if "funds" in data:
            funds = data["funds"]
        if "debug_message" in data:
            debug_message = data["debug_message"]

        cur = self.conn.cursor()
        sql = f"""INSERT INTO debugevents (timestamp, server, transaction_num, command, username, stock_symbol, funds, debug_message)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s) """
        cur.execute(sql, (timestamp,server,transaction_num,command,username,stock_symbol,funds,debug_message))
        self.conn.commit()
        cur.close()
        