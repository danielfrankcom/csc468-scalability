import lib.commands as commands
from lib.xml_writer import *

from quart import Quart, request, jsonify
import asyncpg
import asyncio
import uvloop

import traceback
import logging
import socket
import time
import os
import re


DB = DB_USER = DB_HOST = "postgres"
DB_PASSWORD = "supersecure"
DB_PORT = 5432

CONN_MIN = 100
CONN_MAX = 1000

PROCESSORS = [
        (commands.quote, re.compile(r"^\[(\d+)\] QUOTE,([^ ]{10}),([A-Z]{1,3}) ?$")),
        (commands.add, re.compile(r"^\[(\d+)\] ADD,([^ ]{10}),(\d+\.\d{2}) ?$")),
        (commands.buy, re.compile(r"^\[(\d+)\] BUY,([^ ]{10}),([A-Z]{1,3}),(\d+\.\d{2}) ?$")),
        (commands.commit_buy, re.compile(r"^\[(\d+)\] COMMIT_BUY,([^ ]{10}) ?$")),
        (commands.cancel_buy, re.compile(r"^\[(\d+)\] CANCEL_BUY,([^ ]{10}) ?$")),
        (commands.sell, re.compile(r"^\[(\d+)\] SELL,([^ ]{10}),([A-Z]{1,3}),(\d+\.\d{2}) ?$")),
        (commands.commit_sell, re.compile(r"^\[(\d+)\] COMMIT_SELL,([^ ]{10}) ?$")),
        (commands.cancel_sell, re.compile(r"^\[(\d+)\] CANCEL_SELL,([^ ]{10}) ?$")),
        (commands.set_buy_amount, re.compile(r"^\[(\d+)\] SET_BUY_AMOUNT,([^ ]{10}),([A-Z]{1,3}),(\d+\.\d{2}) ?$")),
        (commands.cancel_set_buy, re.compile(r"^\[(\d+)\] CANCEL_SET_BUY,([^ ]{10}),([A-Z]{1,3}) ?$")),
        (commands.set_buy_trigger, re.compile(r"^\[(\d+)\] SET_BUY_TRIGGER,([^ ]{10}),([A-Z]{1,3}),(\d+\.\d{2}) ?$")),
        (commands.set_sell_amount, re.compile(r"^\[(\d+)\] SET_SELL_AMOUNT,([^ ]{10}),([A-Z]{1,3}),(\d+\.\d{2}) ?$")),
        #(commands.cancel_set_buy, re.compile(r"^\[(\d+)\] CANCEL_SET_BUY,([^ ]{10}),([A-Z]{1,3}) ?$")),
        (commands.set_sell_trigger, re.compile(r"^\[(\d+)\] SET_SELL_TRIGGER,([^ ]{10}),([A-Z]{1,3}),(\d+\.\d{2}) ?$"))
]

ERROR_PATTERN = re.compile(r"^\[(\d+)\] ([A-Z_]+),([^ ,]+)")

class Processor:

    # todo: move to database
    xml_tree = LogBuilder("/out/testLOG")

    def _db_available(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex((DB_HOST, DB_PORT)) == 0

    def __init__(self, loop):
        self.users = dict()
        logger.info("Processor object created.")

        # It is possible that the postgres container has started
        # but is not ready for connections. We poll until it is
        # ready to ensure that we can create the pool below.
        while not self._db_available():
            time.sleep(1)

        # Even after the above, sometimes the DB is in a secondary
        # 'not connectable' state, so execute connection in such a
        # way that it can be repeated.

        success = False
        while not success:
            try:
                self.pool = loop.run_until_complete(
                        asyncpg.create_pool(
                            min_size=CONN_MIN,
                            max_size=CONN_MAX,
                            database=DB,
                            user=DB_USER,
                            password=DB_PASSWORD,
                            host=DB_HOST,
                            port=DB_PORT
                    )
                )
                success = True
            except:
                # Database is still in a non-connectable state.
                continue

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
                "errorMessage": "Error while processing command"
            }
            error.updateAll(**attributes)
            self.xml_tree.append(error)

        except:
            logger.exception("Error logging failed for %s.", transaction)

    async def _handle_user(self, queue):
        # In theory creating a new async worker for each user
        # is not great, as they will never go away. In practice
        # this is fine for the scope of the project, as they
        # are not scheduled if they are not active, and our
        # limitation becomes the memory that they allocate in
        # a real system. With the amount of users that we are
        # dealing with, this won't be an issue.

        while True:
            work_item, transaction = await queue.get()
            logger.info("Work retreived for transaction %s.", transaction)

            async with self.pool.acquire() as conn:
                arguments = {
                        "conn": conn,
                        "xml_tree": self.xml_tree
                }

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

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

loop = asyncio.get_event_loop()
processor = Processor(loop)

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

app.run(host="0.0.0.0", port="5000", loop=loop)
