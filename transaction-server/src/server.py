import lib.commands as commands
from lib.publisher import Publisher

from quart import Quart, request, jsonify
import asyncpg
import asyncio
import asyncio
import uvloop

import logging
import socket
import time
import os
import re
import json


DB = DB_USER = DB_HOST = "postgres"
DB_PASSWORD = "supersecure"
DB_PORT = 5432

CONN_MIN = 100
CONN_MAX = 1000


R_START = r"^"
R_TRANS_NUM = r"^\[(\d+)\] "
R_END = r" ?$"
def build_regex(*args):
    center = ",".join(args)
    return re.compile(R_START + R_TRANS_NUM + center + R_END)

R_STOCK = r"([A-Z]{1,3})"
R_PRICE = r"(\d+\.\d{2})"
R_USERNAME = r"([^ ]{10})"
R_FILENAME = r"([\w\-. /]+)"

PROCESSORS = {
        "QUOTE": (commands.quote, build_regex("QUOTE", R_USERNAME, R_STOCK)),
        "ADD": (commands.add, build_regex("ADD", R_USERNAME, R_PRICE)),
        "BUY": (commands.buy, build_regex("BUY", R_USERNAME, R_STOCK, R_PRICE)),
        "COMMIT_BUY": (commands.commit_buy, build_regex("COMMIT_BUY", R_USERNAME)),
        "CANCEL_BUY": (commands.cancel_buy, build_regex("CANCEL_BUY", R_USERNAME)),
        "SELL": (commands.sell, build_regex("SELL", R_USERNAME, R_STOCK, R_PRICE)),
        "COMMIT_SELL": (commands.commit_sell, build_regex("COMMIT_SELL", R_USERNAME)),
        "CANCEL_SELL": (commands.cancel_sell, build_regex("CANCEL_SELL", R_USERNAME)),
        "SET_BUY_AMOUNT": (commands.set_buy_amount, build_regex("SET_BUY_AMOUNT", R_USERNAME, R_STOCK, R_PRICE)),
        "CANCEL_SET_BUY": (commands.cancel_set_buy, build_regex("CANCEL_SET_BUY", R_USERNAME, R_STOCK)),
        "SET_BUY_TRIGGER": (commands.set_buy_trigger, build_regex("SET_BUY_TRIGGER", R_USERNAME, R_STOCK, R_PRICE)),
        "SET_SELL_AMOUNT": (commands.set_sell_amount, build_regex("SET_SELL_AMOUNT", R_USERNAME, R_STOCK, R_PRICE)),
        "CANCEL_SET_SELL": (commands.cancel_set_sell, build_regex("CANCEL_SET_SELL", R_USERNAME, R_STOCK)),
        "SET_SELL_TRIGGER": (commands.set_sell_trigger, build_regex("SET_SELL_TRIGGER", R_USERNAME, R_STOCK, R_PRICE)),
        "DUMPLOG": (commands.dumplog_user, build_regex("DUMPLOG", R_USERNAME, R_FILENAME)),
        "DISPLAY_SUMMARY": (commands.display_summary, build_regex("DISPLAY_SUMMARY", R_USERNAME))
}

DUMPLOG_PATTERN = build_regex("DUMPLOG", R_FILENAME)

COMMAND_TYPE_PATTERN = re.compile(r"^\[\d+\] ([A-Z_]+)")
ERROR_PATTERN = re.compile(r"^\[(\d+)\] ([A-Z_]+),([^ ,]+)")

class Processor:

    def _db_available(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex((DB_HOST, DB_PORT)) == 0

    def __init__(self, loop):
        logger.info("Processor object being created.")

        self.users = dict()


        self.publisher = Publisher()

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

        loop = asyncio.get_event_loop()
        loop.create_task(commands.reservation_timeout_handler(loop, self.pool))
        loop.create_task(commands.trigger_maintainer(self.pool, self.publisher))

    async def _handle_dumplog(self, transaction, *args):
        settings = {
                "publisher": self.publisher
        }

        try:
            await commands.dumplog(*args, **settings)
            logger.info("Work item completed for transaction %s.", transaction)
        except:
            logger.exception("Work item failed for transaction %s.", transaction)
            self._log_error(transaction)

    async def register_transaction(self, transaction, callback=None):

        type_match = re.match(COMMAND_TYPE_PATTERN, transaction)
        command_type = type_match.groups()[0]
        logger.debug("Command type %s found.", command_type)

        if not command_type or command_type not in PROCESSORS:
            self._log_error(transaction)
            logger.error("Transaction %s did not match any pattern.", transaction)
            return False

        logger.info("Transaction %s found to be type %s.", transaction, command_type)

        processor, validity_pattern = PROCESSORS[command_type]
        match = re.match(validity_pattern, transaction)

        if not match:
            # There is a special case that we must account for, in that there is a derivation
            # of DUMPLOG that does not contain a username, and applies to all users. If we
            # see 'DUMPLOG', we may fail to match on it, as it may not have a username. Here
            # we check for this special case explicitly.

            dumplog_match = re.match(DUMPLOG_PATTERN, transaction)
            if not dumplog_match:
                self._log_error(transaction)
                logger.debug("Pattern for transaction %s is invalid, discarding.", transaction)
                return False

            dumplog_groups = dumplog_match.groups()

            # We cannot fall through and use the code below, as it assumes that a specific
            # user is responsible for the command. Instead, initialize a new task to deal
            # with this command.
            asyncio.create_task(self._handle_dumplog(transaction, *dumplog_groups))

            # Return early as we don't want a specific user to deal with this command.
            return True
            

        # At this point, we have a valid and standard command (with a username)
        groups = match.groups()
        logger.info("Pattern %s matched %s.", validity_pattern, groups)

        # Note that we are not expecting the DUMPLOG (with no user) command at the moment,
        # as it should have been dealt with above and contains no username.
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
            queue = asyncio.Queue(loop=loop)
            self.users[username] = queue
            loop.create_task(self._handle_user(queue))
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
        work = lambda settings: processor(*groups, **settings)
        await queue.put((work, transaction, callback))
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

            data = {
                "timestamp": int(time.time() * 1000),
                "server": "DDJK",
                "transaction_num": transaction_num,
                "username": user_id,
                "command": command,
                "error_message": "Error while processing command"
            }
            message = {
                "type":"errorEvent",
                "data": data
            }
            self.publisher.publish_message(json.dumps(message))
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
            work_item, transaction, callback = await queue.get()
            logger.info("Work retreived for transaction %s.", transaction)

            async with self.pool.acquire() as conn:
                arguments = {
                        "conn": conn,
                        "publisher": self.publisher
                }

                try:
                    result = await work_item(arguments)
                    if callback:
                        await callback(result)

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
    await processor.register_transaction(transaction, loop)

    response = jsonify(success=True)
    return response

transaction_num = 0

@app.route('/api', methods=['POST'])
async def api():
    global transaction_num
    transaction_num += 1
    
    body = await request.data
    logger.info("Request received with body %s.", body.decode())
    payload = json.loads(body.decode())
    username = payload["username"]
    transaction = f"[{transaction_num}] {payload['command']}"

    queue = asyncio.Queue(loop=loop)
    result_dict = {
        "error": None
    }

    quote_pattern = PROCESSORS["QUOTE"][1]
    if re.match(quote_pattern, transaction):
        async def callback(result):
            price = result[0]
            stock = result[1]
            message = "Stock {} is valued at {:.2f}".format(stock, price)
            await queue.put(message)

        # Queue up the transaction for processing by an async worker.
        registered = await processor.register_transaction(transaction, callback)

        async_result = await queue.get()
        if async_result:
            result_dict["quote"] = async_result

    else:
        async def callback(result):
            await queue.put(result)

        # Queue up the transaction for processing by an async worker.
        registered = await processor.register_transaction(transaction, callback)
        async_result = await queue.get()
        if async_result:
            result_dict["error"] = async_result

    if not registered:
        return jsonify(success=False)
    
    return jsonify(result_dict)

@app.route('/status', methods=['POST'])
async def status():

    body = await request.data
    payload = json.loads(body.decode())
    username = payload["username"]

    async with processor.pool.acquire() as conn:
        async with conn.transaction():

            get_balance =   "SELECT balance FROM users " \
                            "WHERE username = $1;"

            balance = await conn.fetchval(get_balance, username)

            trigger_check = "SELECT * FROM triggers " \
                            "WHERE username = $1;"

            trigger_result = await conn.fetch(trigger_check, username)

            triggers = []
            if(trigger_result):
                for row in trigger_result:
                    triggers_row = {
                        "stock_symbol": row[1],
                        "type": row[2],
                        "trigger_amount": row[3],
                        "transaction_amount": row[4]
                    }
                    triggers.append(triggers_row)

            stock_check =   "SELECT * FROM stocks " \
                            "WHERE username = $1;"

            stock_results = await conn.fetch(stock_check, username)

            stocks = []
            if(stock_results):
                for row in stock_results:
                    stocks_row = {
                        "stock_symbol": row[1],
                        "quantity": row[2]
                    }
                    stocks.append(stocks_row)

            reservation_check = "SELECT " \
                                "stock_symbol, stock_quantity, price, amount " \
                                "FROM reserved " \
                                "WHERE username = $1" \
                                "AND type = $2;"

            buy_results = await conn.fetch(reservation_check, username, 'buy')

            buys = []
            if(buy_results):
                for row in buy_results:
                    buy_row = {
                        "stock_symbol": row[0],
                        "stock_quantity": row[1],
                        "price": row[2],
                        "amount": row[3]
                    }
                    buys.append(buy_row)

            sell_results = await conn.fetch(reservation_check, username, 'sell')

            sells = []
            if(sell_results):
                for row in sell_results:
                    sell_row = {
                        "stock_symbol": row[0],
                        "stock_quantity": row[1],
                        "price": row[2],
                        "amount": row[3]
                    }
                    sells.append(sell_row)

    info = {
        "balance": balance,
        "triggers": triggers,
        "stocks": stocks,
        "buys": buys,
        "sells": sells
    }
    return jsonify(info)


app.run(host="0.0.0.0", port="5000", loop=loop)
