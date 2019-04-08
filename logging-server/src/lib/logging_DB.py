import psycopg2
import logging
from lib.xml_writer import *

logger = logging.getLogger(__name__)

class logging_DB(object):
    def __init__(self):
        """ Connect to the PostgreSQL database server """
        self.conn = None
        try:
            # connect to the PostgreSQL server
            logger.info('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(host="logging-db",database="postgres",user="postgres",password="supersecure")
        except (Exception, psycopg2.DatabaseError) as error:
            logger.error(error)

    def disconnect(self):
        self.conn.close()

    def userCommand(self,data):
        timestamp = data["timestamp"]
        server = data["server"]
        transaction_num = data["transaction_num"]
        command = data["command"]
        username = stock_symbol = filename = funds = None
        if "username" in data:
            username = data["username"]
        if "stock_symbol" in data:
            stock_symbol = data["stock_symbol"]
        if "filename" in data:
            filename = data["filename"]
        if "funds" in data:
            funds = data["funds"]
        cur = self.conn.cursor()
        sql = f"""INSERT INTO usercommands (timestamp, server, transaction_num, command, username, stock_symbol, filename, funds)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s) """
        cur.execute(sql, (timestamp,server,transaction_num,command,username,stock_symbol,filename,funds))
        self.conn.commit()
        cur.close()
        # check if the command is a DUMPLOG
        if command == "DUMPLOG":
            self.dumplog(filename,username)
        
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
        username = stock_symbol = filename = funds = None
        if "username" in data:
            username = data["username"]
        if "stock_symbol" in data:
            stock_symbol = data["stock_symbol"]
        if "filename" in data:
            filename = data["filename"]
        if "funds" in data:
            funds = data["funds"]

        cur = self.conn.cursor()
        sql = f"""INSERT INTO systemevents (timestamp, server, transaction_num, command, username, stock_symbol, filename, funds)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s) """
        cur.execute(sql, (timestamp,server,transaction_num,command,username,stock_symbol,filename,funds))
        self.conn.commit()
        cur.close()
        
    def errorEvent(self,data):
        timestamp = data["timestamp"]
        server = data["server"]
        transaction_num = data["transaction_num"]
        command = data["command"]
        username = stock_symbol = filename = funds = error_message = None
        if "username" in data:
            username = data["username"]
        if "stock_symbol" in data:
            stock_symbol = data["stock_symbol"]
        if "filename" in data:
            filename = data["filename"]
        if "funds" in data:
            funds = data["funds"]
        if "error_message" in data:
            error_message = data["error_message"]

        cur = self.conn.cursor()
        sql = f"""INSERT INTO errorevents (timestamp, server, transaction_num, command, username, stock_symbol, filename, funds, error_message)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) """
        cur.execute(sql, (timestamp,server,transaction_num,command,username,stock_symbol,filename,funds,error_message))
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
        if "filename" in data:
            filename = data["filename"]
        if "funds" in data:
            funds = data["funds"]
        if "debug_message" in data:
            debug_message = data["debug_message"]

        cur = self.conn.cursor()
        sql = f"""INSERT INTO debugevents (timestamp, server, transaction_num, command, username, stock_symbol, filename, funds, debug_message)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) """
        cur.execute(sql, (timestamp,server,transaction_num,command,username,stock_symbol,filename,funds,debug_message))
        self.conn.commit()
        cur.close()
    
    def dumplog(self,filename,username=None):
        cur = self.conn.cursor()
        usercommands = quotes = accounttransactions = []
        if username is None:
            cur.execute("""SELECT * FROM usercommands""")
            usercommands = cur.fetchall()
            cur.execute("""SELECT * FROM accounttransactions""")
            accounttransactions = cur.fetchall()
            cur.execute("""SELECT * FROM quoteservers""")
            quotes = cur.fetchall()
            cur.close()
        else:
            cur.execute("""SELECT * FROM usercommands WHERE username = %s """, (username,))
            usercommands = cur.fetchall()
            cur.execute("""SELECT * FROM accounttransactions WHERE username = %s""", (username,))
            accounttransactions = cur.fetchall()
            cur.execute("""SELECT * FROM quoteservers WHERE username = %s""", (username,))
            quotes = cur.fetchall()
            cur.close()
        combined = usercommands + accounttransactions + quotes
        sorted_combined = sorted(combined, key=lambda x: x[0])
        log_path = str("/out/"+filename)
        builder = LogBuilder()
        for row in sorted_combined:
            if len(row) == 8: # user command or quote
                if isinstance(row[6],int):
                    columns = "timestamp server transactionNum price stockSymbol username quoteServerTime cryptokey".split(" ")
                    event = QuoteServer()
                    for idx,col in enumerate(row):
                        if col is not None:
                            event.update(columns[idx],col)
                    builder.store(event)
                else:   
                    columns = "timestamp server transactionNum command username stockSymbol filename funds".split(" ")
                    event = UserCommand()
                    for idx,col in enumerate(row):
                        if col is not None:
                            event.update(columns[idx],col)
                    builder.store(event)
            elif len(row) == 6: # account transaction
                columns = "timestamp server transactionNum action username funds".split(" ")
                event = AccountTransaction()
                for idx,col in enumerate(row):
                    if col is not None:
                        event.update(columns[idx],col)
                builder.store(event)
        builder.write(log_path)

            
        
