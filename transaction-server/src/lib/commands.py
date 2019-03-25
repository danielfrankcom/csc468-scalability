from lib.xml_writer import *
from datetime import datetime

import asyncio
import logging
import time
import os

QUOTE_LIFESPAN = 60 # Time a quote is valid for (60 in production).

QUOTE_CACHE_HOST = "192.168.1.249"
QUOTE_CACHE_PORT = 6000
QUOTE_SERVER_PRESENT = os.environ['http_proxy']

logger = logging.getLogger(__name__)
loop = asyncio.get_event_loop()

# This must be initialized from the entry point to ensure that
# the loop matches. It is guaranteed to run before anything
# tries to access it, as the entry point calls this code before
# processing any transactions.
reservation_timestamp_queue = None

async def reservation_timeout_handler(loop, pool):
    """Helper function - used to cancel buy/sell orders after they timeout."""

    # Note that the expiry times stored in the queue do not necessarily correlate
    # with the reservation rows, due to clock drift and the passing of time
    # between method calls. The queue simply acts as a method to let the handler
    # go idle, and does not dictate which rows should be removed.
    # 
    # When the handler is active, it will remove every row that it can, regardless
    # of the state of the queue. It is possible that the handler wakes up and has
    # no reservations to process, as a buy/sell can be committed with no way to
    # remove the matching timestamp from the queue.
    global reservation_timestamp_queue
    reservation_timestamp_queue = asyncio.Queue(loop=loop)

    # Block until the first expiry time is available.
    expiry_time = await reservation_timestamp_queue.get()
    logging.debug("Expiry time %s retreived", expiry_time)

    while True:
        
        now = round(loop.time())
        sleep_time = expiry_time - now

        # If this is <= 0 then the method will return right away.
        logging.debug("Sleeping for %s", sleep_time)
        await asyncio.sleep(sleep_time)
        logging.debug("Done sleeping for %s", sleep_time)

        # Loops until all currently expired transactions have been dealt with.
        while True:
            async with pool.acquire() as conn:
                async with conn.transaction():

                    delete_reserved =   "DELETE FROM reserved       " \
                                        "WHERE timestamp < $1       " \
                                        "RETURNING *;               " 

                    target_timestamp = round(time.time(), 5)
                    reservation = await conn.fetchrow(delete_reserved, target_timestamp)

                    # There are no more expired reservations
                    if not reservation:
                        logging.debug("No reservations found, breaking")
                        break

                    logging.debug("Found reservation %s of type %s",
                            reservation["reservationid"], reservation["type"])

                    if reservation["type"] == "buy":

                        users_update =  "UPDATE users " \
                                        "SET balance = balance + $1 " \
                                        "WHERE username = $2;"
                        await conn.execute(users_update, reservation["amount"], reservation["username"])

                    else:

                        stocks_update = "UPDATE stocks " \
                                        "SET stock_quantity = stock_quantity + $1 " \
                                        "WHERE username = $2 " \
                                        "AND stock_symbol = $3;"
                        await conn.execute(stocks_update, reservation["stock_quantity"],
                                reservation["username"], reservation["stock_symbol"])

        expired = True
        while expired:
            expiry_time = await reservation_timestamp_queue.get()
            now = round(loop.time())
            expired = expiry_time <= now 
            logging.debug("Expired=%s based on now:%s, expiry:%s", expired, now, expiry_time)

async def get_quote(user_id, stock_symbol):
    """Fetch a quote from the quote cache."""

    request = "{symbol},{user}\n".format(symbol=stock_symbol, user=user_id)

    result = None
    if QUOTE_SERVER_PRESENT:
        reader, writer = await asyncio.open_connection(QUOTE_CACHE_HOST, QUOTE_CACHE_PORT)

        writer.write(request.encode())

        raw = await reader.recv(1024).decode()
        result = reader.split("\n")[0]

        writer.close()

        return result

    else:
        # This sleep will mock production delays
        # await asyncio.sleep(2)
        result = "20.00,BAD,usernamehere,1549827515,crytoKEY=123=o"

    price, symbol, username, timestamp, cryptokey = result.split(",")
    return float(price), int(timestamp), cryptokey, username

# quote() is called when a client requests a quote.  It will return a valid price for the
# stock as requested.
async def quote(transaction_num, user_id, stock_symbol, **settings):
    xml_tree = settings["xml_tree"]

    # get quote from server/cache
    new_price, time_of_quote, cryptokey, quote_user = await get_quote(user_id, stock_symbol)

    quote = QuoteServer()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "price": new_price, 
        "username": quote_user,
        "stockSymbol": stock_symbol,
        "quoteServerTime": time_of_quote,
        "cryptokey": cryptokey
    }
    quote.updateAll(**attributes)
    xml_tree.append(quote)

    return new_price, stock_symbol, user_id, time_of_quote, cryptokey

async def add(transaction_num, user_id, amount, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "ADD",
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    xml_tree.append(command)
    
    query =  "INSERT INTO users (username, balance) " \
             "VALUES ($1, $2) " \
             "ON CONFLICT (username) DO UPDATE " \
             "SET balance = users.balance + $2 " \
             "WHERE users.username = $1;"

    logger.info("Executing add command for transaction %s", transaction_num)
    async with conn.transaction():
        await conn.execute(query, user_id, float(amount))
        logger.debug("Balance update for %s sucessful.", transaction_num)
        
    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "action": "add", 
        "username": user_id,
        "funds": float(amount)
    }
    transaction.updateAll(**attributes)
    xml_tree.append(transaction)

async def _get_latest_reserved(transaction_type, user_id, conn):

    selection = "SELECT reservationid, stock_symbol, stock_quantity, amount " \
                "FROM reserved " \
                "WHERE type = $1 " \
                "AND username = $2 " \
                "AND timestamp = ( " \
                    "SELECT MAX(timestamp) " \
                    "FROM reserved " \
                    "WHERE type = $1 " \
                    "AND username = $2 " \
                ") " \
                "AND timestamp > $3;"

    target_timestamp = round(loop.time()) # Current time is threshold for expiry.
    return await conn.fetchrow(selection, transaction_type, user_id, target_timestamp)

async def buy(transaction_num, user_id, stock_symbol, amount, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "BUY",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    xml_tree.append(command)
    
    price, stock_symbol, user_id, time_of_quote, cryptokey = await quote(transaction_num, user_id, stock_symbol, **settings)

    stock_quantity = int(float(amount) / price)
    if stock_quantity <= 0:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": int(transaction_num),
            "command": "BUY",
            "username": user_id,
            "stockSymbol": stock_symbol,
            "funds": float(amount),
            "errorMessage": "Amount insufficient to purchase at least 1 stock" 
        }
        error.updateAll(**attributes)
        xml_tree.append(error)

        logger.info("Amount insufficient to purchase at least 1 stock for %s.", transaction_num)
        return

    assert stock_quantity > 0
    purchase_price = float(stock_quantity * price)


    logger.info("Executing buy command for transaction %s", transaction_num)
    async with conn.transaction():

        balance_check = "SELECT * FROM users " \
                        "WHERE username = $1 " \
                        "AND balance >= $2;"

        result = await conn.fetchrow(balance_check, user_id, purchase_price)

        if not result:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "BUY",
                "username": user_id,
                "stockSymbol": stock_symbol,
                "funds": purchase_price,
                "errorMessage": "Funds insufficient to purchase requested stock."
            }
            error.updateAll(**attributes)
            xml_tree.append(error)

            logger.info("Funds insufficient to purchase requested stock for %s", transaction_num)
            return


        balance_update =    "UPDATE users " \
                            "SET balance = balance - $1 " \
                            "WHERE username = $2;"

        # Only reserve the exact amount needed to buy the stock
        await conn.execute(balance_update, purchase_price, user_id)
        logger.debug("Balance update for %s sucessful.", transaction_num)

        # It is possible that we have an identical reservation already, however we add this
        # seperately since a COMMIT_BUY only needs to confirm the most recent, not both.
        reserved_update =   "INSERT INTO reserved " \
                            "(type, username, stock_symbol, stock_quantity, price, amount, timestamp) " \
                            "VALUES " \
                            "('buy', $1, $2, $3, $4, $5, $6);"

        timestamp = round(loop.time()) + QUOTE_LIFESPAN # Expiry time.
        await conn.execute(reserved_update, user_id, stock_symbol,
                stock_quantity, price, purchase_price, timestamp)
        logger.debug("Reserved update for %s sucessful.", transaction_num)

        # Mark for expiry in QUOTE_LIFESPAN seconds.
        await reservation_timestamp_queue.put(timestamp)

    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "action": "remove", 
        "username": user_id,
        "funds": float(amount)
    }
    transaction.updateAll(**attributes)
    xml_tree.append(transaction)

async def commit_buy(transaction_num, user_id, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "COMMIT_BUY",
        "username": user_id
    }
    command.updateAll(**attributes)
    xml_tree.append(command)
    
    async with conn.transaction():

        selected = await _get_latest_reserved("buy", user_id, conn)
        if not selected:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "COMMIT_BUY",
                "username": user_id,
                "errorMessage": "No BUY to commit" 
            }
            error.updateAll(**attributes)
            xml_tree.append(error)

            logger.info("No buy to commit for %s", transaction_num)
            return

        stocks_update = "INSERT INTO stocks (username, stock_symbol, stock_quantity) " \
                        "VALUES ($1, $2, $3) " \
                        "ON CONFLICT (username, stock_symbol) DO UPDATE " \
                        "SET stock_quantity = stocks.stock_quantity + $3 " \
                        "WHERE stocks.username = $1 " \
                        "AND stocks.stock_symbol = $2;"
        
        await conn.execute(stocks_update, user_id, selected["stock_symbol"], selected["stock_quantity"])
        
        reservation_delete =    "DELETE FROM reserved " \
                                "WHERE reservationid = $1;"

        await conn.execute(reservation_delete, selected["reservationid"])

async def cancel_buy(transaction_num, user_id, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "CANCEL_BUY",
        "username": user_id
    }
    command.updateAll(**attributes)
    xml_tree.append(command)

    async with conn.transaction():

        selected = await _get_latest_reserved("buy", user_id, conn)
        if not selected:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "CANCEL_BUY",
                "username": user_id,
                "errorMessage": "No BUY to cancel" 
            }
            error.updateAll(**attributes)
            xml_tree.append(error)

            logger.info("No buy to cancel for %s", transaction_num)
            return

        update_balance =    "UPDATE users " \
                            "SET balance = balance + $1 " \
                            "WHERE username = $2;"

        await conn.execute(update_balance, selected["amount"], user_id)

        delete_reserved =   "DELETE FROM reserved " \
                            "WHERE reservationid = $1;"

        await conn.execute(delete_reserved, selected["reservationid"])

    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "action": "add", 
        "username": user_id,
        "funds": float(selected["amount"])
    }
    transaction.updateAll(**attributes)
    xml_tree.append(transaction)

async def sell(transaction_num, user_id, stock_symbol, amount, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "SELL",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    xml_tree.append(command)

    price, stock_symbol, user_id, time_of_quote, cryptokey = await quote(transaction_num, user_id, stock_symbol, **settings)

    sell_quantity = int(float(amount) / price)
    if sell_quantity <= 0:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000),
            "server": "DDJK",
            "transactionNum": int(transaction_num),
            "command": "SELL",
            "username": user_id,
            "stockSymbol": stock_symbol,
            "funds": float(amount),
            "errorMessage": "Amount insufficient to sell at least 1 stock"
        }
        error.updateAll(**attributes)
        xml_tree.append(error)

        logger.info("Amount insufficient to sell at least 1 stock for %s.", transaction_num)
        return

    assert sell_quantity > 0

    async with conn.transaction():

        stock_check =   "SELECT stock_quantity FROM stocks " \
                        "WHERE username = $1 " \
                        "AND stock_symbol = $2 " \
                        "AND stock_quantity >= $3;"

        result = await conn.fetchrow(stock_check, user_id, stock_symbol, sell_quantity)

        if not result:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "BUY",
                "username": user_id,
                "stockSymbol": stock_symbol,
                "errorMessage": "Stock quantity insufficient to sell requested stock."
            }
            error.updateAll(**attributes)
            xml_tree.append(error)

            logger.info("Funds insufficient to purchase requested stock for %s", transaction_num)
            return

        sell_price = float(sell_quantity * price)

        stocks_update = "UPDATE stocks " \
                        "SET stock_quantity = stock_quantity - $1 " \
                        "WHERE username = $2 " \
                        "AND stock_symbol = $3;"

        await conn.execute(stocks_update, sell_quantity, user_id, stock_symbol)

        reserved_update =   "INSERT INTO reserved " \
                            "(type, username, stock_symbol, stock_quantity, price, amount, timestamp) " \
                            "VALUES " \
                            "('sell', $1, $2, $3, $4, $5, $6);"
                            
        timestamp = round(loop.time()) + QUOTE_LIFESPAN # Expiry time.
        await conn.execute(reserved_update, user_id, stock_symbol,
                sell_quantity, price, sell_price, timestamp)

        # Mark for expiry in QUOTE_LIFESPAN seconds.
        await reservation_timestamp_queue.put(timestamp)

async def commit_sell(transaction_num, user_id, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "COMMIT_SELL",
        "username": user_id
    }
    command.updateAll(**attributes)
    xml_tree.append(command)

    async with conn.transaction():

        selected = await _get_latest_reserved("sell", user_id, conn)
        if not selected:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "COMMIT_SELL",
                "username": user_id,
                "errorMessage": "No SELL to commit" 
            }
            error.updateAll(**attributes)
            xml_tree.append(error)

            logger.info("No sell to commit for %s", transaction_num)
            return

        users_update =  "UPDATE users " \
                        "SET balance = balance + $1 " \
                        "WHERE username = $2;"

        await conn.execute(users_update, selected["amount"], user_id)

        delete_reserved =   "DELETE FROM reserved " \
                            "WHERE reservationid = $1;"

        await conn.execute(delete_reserved, selected["reservationid"])

    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "action": "add", 
        "username": user_id,
        "funds": float(selected["amount"])
    }
    transaction.updateAll(**attributes)
    xml_tree.append(transaction)

async def cancel_sell(transaction_num, user_id, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "CANCEL_SELL",
        "username": user_id
    }
    command.updateAll(**attributes)
    xml_tree.append(command)

    async with conn.transaction():

        selected = await _get_latest_reserved("sell", user_id, conn)
        if not selected:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "CANCEL_SELL",
                "username": user_id,
                "errorMessage": "No SELL to cancel" 
            }
            error.updateAll(**attributes)
            xml_tree.append(error)

            logger.info("No sell to cancel for %s", transaction_num)
            return

        stocks_update = "UPDATE stocks " \
                        "SET stock_quantity = stock_quantity + $1 " \
                        "WHERE username = $2 " \
                        "AND stock_symbol = $3;"

        await conn.execute(stocks_update, selected["stock_quantity"], user_id, selected["stock_symbol"])

        reservation_delete =    "DELETE FROM reserved " \
                                "WHERE reservationid = $1;"

        await conn.execute(reservation_delete, selected["reservationid"])

# set_buy_amount allows a user to set a dollar amount of stock to buy.  This must be followed
# by set_buy_trigger() before the trigger goes 'live'. 
async def set_buy_amount(transaction_num, user_id, stock_symbol, amount, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]

    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "SET_BUY_AMOUNT",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    xml_tree.append(command)

    async with conn.transaction():

        get_existing = "SELECT transaction_amount  " \
                       "FROM triggers              " \
                       "WHERE username = $1        " \
                       "AND stock_symbol = $2      " \
                       "AND type = 'buy';          "

        # Does SET_BUY order exist for this user/stock combo?
        existing = await conn.fetchval(get_existing, user_id, stock_symbol)

        if existing:
            difference = float(amount) - existing
        else:
            difference = float(amount)

        # Confirm that the user has the appropriate funds in their account.
        balance_check = "SELECT balance         " \
                        "FROM users             " \
                        "WHERE username = $1;   " \

        balance = await conn.fetchval(balance_check, user_id)

        if not balance or balance < difference:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "SET_BUY_AMOUNT",
                "username": user_id,
                "errorMessage": "Insufficient Funds" 
            }
            error.updateAll(**attributes)
            xml_tree.append(error)
            return

        logger.debug("Balance of %s: %.02f is sufficient for %s", user_id, balance, transaction_num)

        users_update =  "UPDATE users               " \
                        "SET balance = balance - $1 " \
                        "WHERE username = $2        "

        # Adjust member's account balance.
        await conn.execute(users_update, difference, user_id)

        # If the order existed already, update it with the new BUY_AMOUNT, else create new record.
        triggers_update =   "INSERT INTO triggers                                                   " \
                            "(username, stock_symbol, type, transaction_amount, transaction_number) " \
                            "VALUES ($1, $2, 'buy', $3, $4)                                         " \
                            "ON CONFLICT (username, stock_symbol, type) DO UPDATE                   " \
                            "SET                                                                    " \
                            "transaction_amount = $3,                                               " \
                            "transaction_number = $4;                                               "

        await conn.execute(triggers_update, user_id, stock_symbol, float(amount), int(transaction_num))

    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "username": user_id,
        "funds": abs(float(difference))
    }
    if difference > 0:
        # money is to be removed from user account 
        attributes.update({"action": "remove"})
    else:
        # difference < 0, therefore money is being refunded back into user account
        attributes.update({"action": "add"})

    transaction.updateAll(**attributes)
    xml_tree.append(transaction)        

async def cancel_set_buy(transaction_num, user_id, stock_symbol, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "CANCEL_SET_BUY",
        "username": user_id,
        "stockSymbol": stock_symbol
    }
    command.updateAll(**attributes)
    xml_tree.append(command)
    
    async with conn.transaction():

        get_existing = "SELECT transaction_amount  " \
                       "FROM triggers              " \
                       "WHERE username = $1        " \
                       "AND stock_symbol = $2      " \
                       "AND type = 'buy';          "

        # Does SET_BUY order exist for this user/stock combo?
        refund_amount = await conn.fetchval(get_existing, user_id, stock_symbol)

        if not refund_amount:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "CANCEL_SET_BUY",
                "username": user_id,
                "errorMessage": "SET_BUY does not exist, no action taken"
            }
            error.updateAll(**attributes)
            xml_tree.append(error)

            logger.info("SET_BUY does not exist, no action taken")
            return

        logger.info("SET_BUY found, cancelling")
        logger.debug("Refund amount for %s: %.02f", transaction_num, refund_amount)

        triggers_delete =   "DELETE FROM triggers   " \
                            "WHERE username = $1    " \
                            "AND stock_symbol = $2  " \
                            "AND type = 'buy';      "

        await conn.execute(triggers_delete, user_id, stock_symbol)

        users_update =  "UPDATE users               " \
                        "SET balance = balance + $1 " \
                        "WHERE username = $2        "

        await conn.execute(users_update, refund_amount, user_id)

    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "action": "add", 
        "username": user_id,
        "funds": refund_amount
    }
    transaction.updateAll(**attributes)
    xml_tree.append(transaction)

async def set_buy_trigger(transaction_num, user_id, stock_symbol, amount, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "SET_BUY_TRIGGER",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(amount)
    }
    command.updateAll(**attributes)
    xml_tree.append(command)
    
    async with conn.transaction():

        get_existing = "SELECT transaction_amount  " \
                       "FROM triggers              " \
                       "WHERE username = $1        " \
                       "AND stock_symbol = $2      " \
                       "AND type = 'buy';          "

        # Does SET_BUY order exist for this user/stock combo?
        existing = await conn.fetchval(get_existing, user_id, stock_symbol)

        if not existing:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "SET_BUY_TRIGGER",
                "username": user_id,
                "errorMessage": "SET_BUY does not exist, no action taken"
            }
            error.updateAll(**attributes)
            xml_tree.append(error)

            logger.info("SET_BUY does not exist, no action taken")
            return

        triggers_update =   "UPDATE triggers             " \
                            "SET                         " \
                            "trigger_amount = $1,        " \
                            "transaction_number = $2     " \
                            "WHERE username = $3         " \
                            "AND stock_symbol = $4       " \
                            "AND type = 'buy';           "

        await conn.execute(triggers_update, float(amount), int(transaction_num), user_id, stock_symbol)

async def set_sell_amount(transaction_num, user_id, stock_symbol, requested_transaction, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "SET_SELL_AMOUNT",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(requested_transaction)
    }
    command.updateAll(**attributes)
    xml_tree.append(command)

    async with conn.transaction():

        user_check =    "SELECT username " \
                        "FROM users  " \
                        "WHERE username = $1; "

        exists = await conn.fetchval(user_check, user_id)
        if not exists:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "SET_BUY_TRIGGER",
                "username": user_id,
                "errorMessage": "User for SET_SELL_AMOUNT does not exist"
            }
            error.updateAll(**attributes)
            xml_tree.append(error)
            return

        # If a trigger already exists, we need to recalculate the required stock.
        get_existing =  "SELECT transaction_amount, trigger_amount  " \
                        "FROM triggers                              " \
                        "WHERE username = $1                        " \
                        "AND stock_symbol = $2                      " \
                        "AND type = 'sell';                         "

        existing = await conn.fetchrow(get_existing, user_id, stock_symbol)

        if existing:

            # This may be None if the trigger has not yet been set.
            trigger_amount = existing["trigger_amount"]

            if trigger_amount:
                # Trigger has previously been set, so we must update the
                # user's stocks.

                current_transaction = existing["transaction_amount"]

                # This is a sell, and therefore the trigger will only execute when
                # the price is equal to or higher than the requested value. This
                # means that the number of stock required will never be more than
                # the division of the transaction amount by the trigger.
                total_stock_required = int(float(requested_transaction) / trigger_amount)

                # There were previously stocks subtracted from the user, so we
                # must figure out how many extra we need to add/subtract.
                previously_subtracted = int(current_transaction / trigger_amount)
                difference = total_stock_required - previously_subtracted

                # Check if user owns enough stock to carry out this transaction.
                get_stock_quantity =    "SELECT stock_quantity " \
                                        "FROM stocks           " \
                                        "WHERE username = $1   " \
                                        "AND stock_symbol = $2;"

                stock_owned = await conn.fetchval(get_stock_quantity, user_id, stock_symbol)

                # Difference may be negative, however this check will still pass.
                if not stock_owned or stock_owned < difference:
                    error = ErrorEvent()
                    attributes = {
                        "timestamp": int(time.time() * 1000),
                        "server": "DDJK",
                        "transactionNum": int(transaction_num),
                        "command": "SET_SELL_AMOUNT",
                        "username": user_id,
                        "errorMessage": "User does not own enough shares of this type"
                    }
                    error.updateAll(**attributes)
                    xml_tree.append(error)
                    return

                logger.info("User owns enough stocks for transaction %s to proceed.", transaction_num)

                # Negative difference will result in an addition to the account here.
                stocks_update = "UPDATE stocks                            " \
                                "SET stock_quantity = stock_quantity - $1 " \
                                "WHERE username = $2                      " \
                                "AND stock_symbol = $3                    "

                await conn.execute(stocks_update, difference, user_id, stock_symbol)

            triggers_update =   "UPDATE triggers            " \
                                "SET                        " \
                                "transaction_amount = $1,   " \
                                "transaction_number = $2    " \
                                "WHERE username = $3        " \
                                "AND stock_symbol = $4      " \
                                "AND type = 'sell'          " \

            await conn.execute(triggers_update, float(requested_transaction),
                    int(transaction_num), user_id, stock_symbol)

        else:

            triggers_update =   "INSERT INTO triggers                                                   " \
                                "(username, stock_symbol, type, transaction_amount, transaction_number) " \
                                "VALUES ($1, $2, 'sell', $3, $4)                                        " \

            await conn.execute(triggers_update, user_id, stock_symbol,
                    float(requested_transaction), int(transaction_num))

async def cancel_set_sell(transaction_num, user_id, stock_symbol, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "CANCEL_SET_SELL",
        "username": user_id,
        "stockSymbol": stock_symbol
    }
    command.updateAll(**attributes)
    xml_tree.append(command)

    async with conn.transaction():

        get_existing = "SELECT transaction_amount, trigger_amount   " \
                       "FROM triggers                               " \
                       "WHERE username = $1                         " \
                       "AND stock_symbol = $2                       " \
                       "AND type = 'sell';                          "

        # Does SET_SELL order exist for this user/stock combo?
        existing = await conn.fetchrow(get_existing, user_id, stock_symbol)

        if not existing:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000),
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "CANCEL_SET_SELL",
                "username": user_id,
                "errorMessage": "SET_SELL does not exist, no action taken"
            }
            error.updateAll(**attributes)
            xml_tree.append(error)

            logger.info("SET_SELL does not exist, no action taken")
            return

        logger.info("SET_SELL found, cancelling")

        trigger_amount = existing["trigger_amount"]

        if trigger_amount:
            # There is a trigger amount set, so funds were subtracted from
            # the user's account, refund them.

            transaction_amount = existing["transaction_amount"]
            refund_amount = int(float(transaction_amount) / trigger_amount)
                            
            stocks_update = "UPDATE stocks                              " \
                            "SET stock_quantity = stock_quantity + $1   " \
                            "WHERE username = $2                        " \
                            "AND stock_symbol = $3;                     "

            await conn.execute(stocks_update, refund_amount, user_id, stock_symbol)

        triggers_delete =   "DELETE FROM triggers   " \
                            "WHERE username = $1    " \
                            "AND stock_symbol = $2  " \
                            "AND type = 'sell';      "

        await conn.execute(triggers_delete, user_id, stock_symbol)

async def set_sell_trigger(transaction_num, user_id, stock_symbol, requested_trigger, **settings):
    xml_tree = settings["xml_tree"]
    conn = settings["conn"]
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": int(transaction_num),
        "command": "SET_SELL_TRIGGER",
        "username": user_id,
        "stockSymbol": stock_symbol,
        "funds": float(requested_trigger)
    }
    command.updateAll(**attributes)
    xml_tree.append(command)

    async with conn.transaction():
    
        get_existing =  "SELECT transaction_amount, trigger_amount  " \
                        "FROM triggers                              " \
                        "WHERE username = $1                        " \
                        "AND stock_symbol = $2                      " \
                        "AND type = 'sell';                         "

        # Does SET_SELL order exist for this user/stock combo?
        existing = await conn.fetchrow(get_existing, user_id, stock_symbol)

        if not existing:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "SET_SELL_TRIGGER",
                "username": user_id,
                "errorMessage": "SET_SELL does not exist, no action taken"
            }
            error.updateAll(**attributes)
            xml_tree.append(error)

            logger.info("SET_SELL does not exist, no action taken")
            return

        # This may be None if the trigger has not yet been set.
        current_trigger = existing["trigger_amount"]
        transaction_amount = existing["transaction_amount"]

        # This is a sell, and therefore the trigger will only execute when
        # the price is equal to or higher than the requested value. This
        # means that the number of stock required will never be more than
        # the division of the transaction amount by the trigger.
        total_stock_required = int(transaction_amount / float(requested_trigger))

        if current_trigger:
            # There were previously stocks subtracted from the user, so we
            # must figure out how many extra we need to add/subtract.
            previously_subtracted = int(transaction_amount / current_trigger)
            difference = total_stock_required - previously_subtracted
        else:
            difference = total_stock_required

        # Check if user owns enough stock to carry out this transaction.
        get_stock_quantity =    "SELECT stock_quantity " \
                                "FROM stocks           " \
                                "WHERE username = $1   " \
                                "AND stock_symbol = $2;"

        stock_owned = await conn.fetchval(get_stock_quantity, user_id, stock_symbol)

        # Difference may be negative, however this check will still pass.
        if not stock_owned or stock_owned < difference:
            error = ErrorEvent()
            attributes = {
                "timestamp": int(time.time() * 1000), 
                "server": "DDJK",
                "transactionNum": int(transaction_num),
                "command": "SET_SELL_TRIGGER",
                "username": user_id,
                "errorMessage": "User does not own enough shares of this type"
            }
            error.updateAll(**attributes)
            xml_tree.append(error)
            return

        logger.info("User owns enough stocks for transaction %s to proceed.", transaction_num)

        # Negative difference will result in an addition to the account here.
        stocks_update = " UPDATE stocks                            " \
                        " SET stock_quantity = stock_quantity - $1 " \
                        " WHERE username = $2                      " \
                        " AND stock_symbol = $3                    "

        await conn.execute(stocks_update, difference, user_id, stock_symbol)


        triggers_update =   " UPDATE triggers                           " \
                            " SET trigger_amount = $1,                  " \
                            "     transaction_number = $2               " \
                            " WHERE username = $3                       " \
                            " AND stock_symbol = $4                     " \
                            " AND type = 'sell';                        "

        await conn.execute(triggers_update, float(requested_trigger), int(transaction_num), user_id, stock_symbol)

async def _process_trigger(record, pool, xml_tree):

    async with pool.acquire() as conn:
        async with conn.transaction():

            # Check that the provided transaction still exists, as it is possible
            # that it has been cancelled since the previous transaction.
            trigger_check = "SELECT transaction_number FROM triggers " \
                            "WHERE transaction_number = $1;          "

            exists = await conn.fetchval(trigger_check, record["transaction_number"])
            if not exists:
                # Transaction has been cancelled,
                # silently exit.
                return

            settings = {"xml_tree": xml_tree}
            results = await quote(record["transaction_number"], record["username"], record["stock_symbol"], **settings)
            price = results[0]

            trigger_amount = record["trigger_amount"]
            transaction_amount = record["transaction_amount"]

            if record["type"] == "buy" and price <= trigger_amount:
                # Buy has been "triggered"

                stocks_update = "INSERT INTO stocks (username, stock_symbol, stock_quantity) " \
                                "VALUES ($1, $2, $3) " \
                                "ON CONFLICT (username, stock_symbol) DO UPDATE " \
                                "SET stock_quantity = stocks.stock_quantity + $3 " \
                                "WHERE stocks.username = $1 " \
                                "AND stocks.stock_symbol = $2;"

                stock_quantity = int(transaction_amount / price)
                await conn.execute(stocks_update, record["username"], record["stock_symbol"], stock_quantity)

                balance_addition = transaction_amount - (price * stock_quantity)

            elif record["type"] == "sell" and price >= trigger_amount:
                # Sell has been "triggered"

                num_stock = int(transaction_amount / trigger_amount)
                balance_addition = num_stock * price

            else:
                # We do not want to run the remainder
                # of the code in this method.
                return

            users_update =  "UPDATE users               " \
                            "SET balance = balance + $1 " \
                            "WHERE username = $2;       "

            await conn.execute(users_update, balance_addition, record["username"])

            triggers_update =   "DELETE FROM triggers   " \
                                "WHERE username = $1    " \
                                "AND stock_symbol = $2  " \
                                "AND type = $3;         "

            await conn.execute(triggers_update, record["username"], record["stock_symbol"], record["type"])

    transaction = AccountTransaction()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": record["transaction_number"],
        "action": "add", 
        "username": record["username"],
        "funds": float(balance_addition)
    }
    transaction.updateAll(**attributes)
    xml_tree.append(transaction)

async def trigger_maintainer(pool, xml_tree):

    while True:
        start_time = round(loop.time())

        async with pool.acquire() as conn:
            async with conn.transaction():

                get_triggers =  "SELECT * FROM triggers             " \
                                "WHERE trigger_amount IS NOT NULL;  "

                triggers = await conn.fetch(get_triggers)

        # The list of triggers is no longer in a transaction, so we must be careful
        # how we deal with them. Within each async task, we will check whether or
        # not the trigger still exists in the context of the new transaction.

        logging.info("Trigger maintainer: %s triggers found to check.", len(triggers))

        tasks = [_process_trigger(record, pool, xml_tree) for record in triggers]
        await asyncio.gather(*tasks)

        logging.debug("Finished processing triggers")
        
        now = round(loop.time())
        sleep_time = QUOTE_LIFESPAN - (now - start_time)
        logging.debug("Trigger maintainer sleeping for %s seconds.", sleep_time)
        await asyncio.sleep(sleep_time)
        logging.debug("Trigger maintainer woke up")

# Deprecated.
def dumplog(transaction_num, filename, XMLTree):
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
        "command": "DUMPLOG"
    }
    command.updateAll(**attributes)
    XMLTree.append(command)

# Deprecated.
def dumplog_user(transaction_num, user_id, filename, XMLTree):
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

# Deprecated.
def display_summary(transaction_num, user_id, XMLTree):
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
