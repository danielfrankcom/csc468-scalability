import lib.commands as commands
from lib.xml_writer import *

from quart import Quart, request, jsonify
import asyncio
import uvloop
import asyncpg
from concurrent.futures import ProcessPoolExecutor

import logging
import json
import os, re, time, traceback


DB = DB_USER = DB_HOST = "postgres"
DB_PASSWORD = "supersecure"
DB_PORT = 5432

PROCESSORS = [
        (commands.quote, re.compile(r"^\[(\d+)\] QUOTE,([^ ]{10}),([A-Z]{1,3}) ?$")),
        (commands.add, re.compile(r"^\[(\d+)\] ADD,([^ ]{10}),(\d+\.\d{2}) ?$")),
        (commands.buy, re.compile(r"^\[(\d+)\] BUY,([^ ]{10}),([A-Z]{1,3}),(\d+\.\d{2}) ?$")),
        (commands.commit_buy, re.compile(r"^\[(\d+)\] COMMIT_BUY,([^ ]{10}) ?$")),
        (commands.cancel_buy, re.compile(r"^\[(\d+)\] CANCEL_BUY,([^ ]{10}) ?$")),
        (commands.sell, re.compile(r"^\[(\d+)\] SELL,([^ ]{10}),([A-Z]{1,3}),(\d+\.\d{2}) ?$")),
        (commands.commit_sell, re.compile(r"^\[(\d+)\] COMMIT_SELL,([^ ]{10}) ?$")),
        (commands.cancel_sell, re.compile(r"^\[(\d+)\] CANCEL_SELL,([^ ]{10}) ?$"))
]

ERROR_PATTERN = re.compile(r"^\[(\d+)\] ([A-Z_]+),([^ ,]+)")



"""
Provides a method to call with parameters.
"""
def parse(raw, conn):

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
            quote(transactionNum, user_id, stock_symbol, XMLTree)
    #ADD Command
    elif command == "ADD":
        try:
            user_id, amount = arguments
        except ValueError:
            print("Invalid Input. <ADD, USER_ID, AMOUNT>")
        else:    
            add(transactionNum, user_id, amount, conn, XMLTree)
    #BUY Command
    elif command == "BUY":
        try:
            user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid Input. <BUY USER_ID STOCK_SYMBOL AMOUNT>")
        else:    
            buy(transactionNum, user_id, stock_symbol, amount, conn, XMLTree)
    elif command == "COMMIT_BUY":
        try:
            [user_id] = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_BUY USER_ID>")
        else:    
            commit_buy(transactionNum, user_id, conn, XMLTree)
    elif command == "CANCEL_BUY":
        try:
            [user_id] = arguments
        except ValueError:
            print("Invalid Input. <CANCEL_BUY USER_ID>")
        else:    
            cancel_buy(transactionNum, user_id, conn, XMLTree)
    elif command == "SELL":
        try:
            user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid Input. <SELL USER_ID STOCK_SYMBOL AMOUNT>")
        else:    
            sell(transactionNum, user_id, stock_symbol, amount, conn, XMLTree)
    elif command == "COMMIT_SELL":
        try:
            [user_id] = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_SELL USER_ID>")
        else:    
            commit_sell(transactionNum, user_id, conn, XMLTree)
    elif command == "SET_BUY_AMOUNT":
        try:
            user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid input.  <SET_BUY_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_buy_amount(transactionNum, user_id, stock_symbol, amount, conn, XMLTree)
    elif command == "CANCEL_SET_BUY":
        try:
            user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid input.  <CANCEL_SET_BUY USER_ID STOCK_SYMBOL>")
        else:
            cancel_set_buy(transactionNum, user_id, stock_symbol, conn, XMLTree)
    elif command == "SET_BUY_TRIGGER":
        try:
            user_id, symbol, amount = arguments
        except ValueError:
            print("Invalid input. <SET_BUY_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_buy_trigger(transactionNum, user_id, symbol, amount, conn, XMLTree)
    elif command == "SET_SELL_AMOUNT":
        try:
            user_id, stock_symbol, amount = arguments
        except ValueError:
            print("Invalid input.  <SET_SELL_AMOUNT USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_sell_amount(transactionNum, user_id, stock_symbol, amount, conn, XMLTree)
    elif command == "CANCEL_SET_SELL":
        try:
            user_id, stock_symbol = arguments
        except ValueError:
            print("Invalid input.  <CANCEL_SET_SELL USER_ID STOCK_SYMBOL>")
        else:
            cancel_set_sell(transactionNum, user_id, stock_symbol, conn, XMLTree)
    elif command == "CANCEL_SELL":
        try:
            [user_id] = arguments
        except ValueError:
            print("Invalid Input. <COMMIT_SELL USER_ID>")
        else:    
            cancel_sell(transactionNum, user_id, conn, XMLTree)
    elif command == "SET_SELL_TRIGGER":
        try:
            user_id, symbol, amount = arguments
        except ValueError:
            print("Invalid input. <SET_SELL_TRIGGER USER_ID STOCK_SYMBOL AMOUNT>")
        else:
            set_sell_trigger(transactionNum, user_id, symbol, amount, conn, XMLTree)
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
                dumplog_user(transactionNum, user_id, path, XMLTree)
        else:
            filename = os.path.basename(location)
            path = os.path.join("/out/", filename)
            dumplog(transactionNum, path, XMLTree)
    elif command == "DISPLAY_SUMMARY":
        try:
            [user_id] = arguments
        except ValueError:
            print("Invalid input. <DISPLAY_SUMMARY USER_ID>")
        else:
            display_summary(transactionNum, user_id, XMLTree)
    else:
        print(arguments, " Invalid Command")

class Processor:

    # todo: move to database
    xml_tree = LogBuilder("/out/testLOG")

    def __init__(self):
        self.users = dict()
        logger.info("Processor object created.")

        # todo: will start timer thread here

    def _check_transaction(self, transaction):
        for function, pattern in PROCESSORS:
            match = re.match(pattern, transaction)
            if not match:
                logger.debug("Pattern %s for transaction %s failed.", pattern, transaction)
                continue

            groups = match.groups()
            logger.info("Pattern %s matched %s.", pattern, groups)
            return (function, groups)

    async def register_transaction(self, transaction):
        result = self._check_transaction(transaction)
        if not result:
            logger.error("Transaction %s did not match any pattern.", transaction)
            return False

        function, groups = result

        # todo: dumplog has no user
        username = groups[1]
        logger.debug("Username %s found for %s.", username, transaction)

        # If a queue exists for this user then add the transaction
        # to the queue. If one does not, create it and start an
        # async worker to process the queue.
        queue = None
        if username in self.users:
            queue = self.users[username]
            logger.info("Added user %s to existing queue.", username)
        else:
            queue = asyncio.Queue()
            self.users[username] = queue
            asyncio.create_task(self._handle_user(queue))
            logger.info("Created new queue (%s) for user %s.", id(queue), username)

        # There is an implicit race condition here that may occur if
        # 2 requests for the same user attempt to create a queue at
        # the same time. This would result in 2 queues, and 2 async
        # workers, only 1 of which would receive the remainder of
        # the requests. In practice this is not a problem, however
        # it may be possible that the first 2 requests for a user
        # are not in the correct order, providing this rare race
        # condition rears its head.

        # Set up the processing function for running asynchronously.
        work = lambda settings: function(*groups, **settings)
        await queue.put((work, transaction))
        logger.debug("Transaction %s added to queue.", transaction)

        return True

    def _log_error(self, transaction):

        try:
            match = re.match(ERROR_PATTERN, transaction)
            if not match:
                # Incapable of finding even the most basic info
                # in the transaction, so throw it away.
                logger.error("Unable to find enough info to log %s.", transaction)
                return

            transaction_num, command, user_id = match.groups()
            transaction_num = int(transaction_num)
            logger.debug("Error information for %s: %s, %s, %s.",
                    transaction, transaction_num, command, user_id)

            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000),
                "server": "DDJK",
                "transactionNum": transaction_num,
                "username": user_id,
                "command": command,
                "errorMessage": "Improperly formed command"
            }
            error.updateAll(**attributes)
            self.xml_tree.append(error)

        except:
            logger.exception("Error logging failed for %s.", transaction)

    async def _handle_user(self, queue):
        # Each async worker has their own database connection,
        # as their own actions are synchronous with respect to
        # transactions by the same user.
        conn = await asyncpg.connect(
                database=DB,
                user=DB_USER,
                password=DB_PASSWORD,
                host=DB_HOST,
                port=DB_PORT
        )
        logger.info("Connection opened to DB for queue (%s).", id(queue))

        # In theory creating a new async worker for each user
        # is not great, as they will never go away. In practice
        # this is fine for the scope of the project, as they
        # are not scheduled if they are not active, and our
        # limitation becomes the memory that they allocate in
        # a real system. With the amount of users that we are
        # dealing with, this won't be an issue.

        arguments = {
                "conn": conn,
                "xml_tree": self.xml_tree
        }

        while True:
            work_item, transaction = await queue.get()
            logger.info("Work retreived for transaction %s.", transaction)

            try:
                await work_item(arguments)
                logger.info("Work item completed for transaction %s.", transaction)
            except:
                # We log the error (in xml) and continue to limp along, hoping the
                # issue doesn't occur again. If it does, there's not much we can do.
                logger.exception("Work item failed for transaction %s.", transaction)
                self._log_error(transaction)
                

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

#executor = ThreadPoolExecutor(1000)
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

processor = Processor()
app = Quart(__name__)

@app.route('/', methods=['POST'])
async def root():

    body = await request.data
    transaction = body.decode()
    logger.info("Request received with body %s.", transaction)

    # Queue up the transaction for processing by an async worker.
    result = await processor.register_transaction(transaction)
    logger.info("Request stored with result %s.", result)

    response = jsonify(success=True)
    return response
