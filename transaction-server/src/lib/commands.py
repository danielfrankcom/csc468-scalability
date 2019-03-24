import time, datetime
import random
from lib.xml_writer import *
from itertools import count

import asyncio
import asyncpg
import logging
import os

QUOTE_LIFESPAN = 60.0 # period of time a quote is valid for (will be 60.0 for deployed software)

QUOTE_CACHE_HOST = "quote-cache"
QUOTE_CACHE_PORT = 6000
QUOTE_SERVER_PRESENT = os.environ['http_proxy']

logger = logging.getLogger(__name__)


async def get_quote(user_id, stock_symbol):
    """Fetch a quote from the quote cache."""

    request = "{symbol},{user}\n".format(symbol=stock_symbol, user=user_id)

    result = None
    if QUOTE_SERVER_PRESENT:
        reader, writer = await asyncio.open_connection(QUOTE_CACHE_HOST, QUOTE_CACHE_PORT)

        writer.write(query.encode())

        raw = await reader.recv(1024).decode()
        result = reader.split("\n")[0]

        writer.close()

        return result

    else:
        # This sleep will mock production delays
        # await asyncio.sleep(2)
        result = "20.01,BAD,usernamehere,1549827515,crytoKEY=123=o"

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
             "SET balance = (users.balance + $2) " \
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

# Helper function - used to cancel buy orders after they timeout
def _buy_timeout(user_id, stock_symbol, dollar_amount, conn, XMLTree):
    # todo
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM reserved WHERE    '
        'type = %s AND                              '
        'username = %s AND                          '
        'stock_symbol = %s AND                      '
        'amount = %s;                               ',
        ('buy', user_id, stock_symbol, dollar_amount))
    
    
    try:
        result = cursor.fetchone()
    except:
        conn.rollback()
        return

    if result is None: #order has already been manually confirmed or cancelled
        print("Timer for order is up, but the order is already gone")
        return
    else: # the reservation still exists, so delete it and refund the cash back to user's account
        reservationid = result[0]
        reserved_cash = result[6]
        cursor.execute('DELETE FROM reserved WHERE reservationid = %s;', (reservationid,))    
        cursor.execute('SELECT balance FROM users where username = %s;', (user_id,))
        try:
            result = cursor.fetchall()
        except:
            conn.rollback()
            return
        if result is None:
            print("Error - user does not exist!")
            return
        existing_balance = result[0][0]
        new_balance = existing_balance + reserved_cash
        cursor.execute('UPDATE users SET balance = %s WHERE username = %s;', (str(new_balance), (user_id)))
        conn.commit()        
        print('buy order timout - the following buy order is now cancelled: ', 
            user_id, stock_symbol, dollar_amount)

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

        timestamp = round(time.time(), 5)
        await conn.execute(reserved_update, user_id, stock_symbol,
                stock_quantity, price, purchase_price, timestamp)
        logger.debug("Reserved update for %s sucessful.", transaction_num)

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

    #threading.Timer(QUOTE_LIFESPAN, buy_timeout, args=(user_id, stock_symbol, amount, conn)).start()

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

    target_timestamp = round(time.time(), 5) - 60
    return await conn.fetchrow(selection, transaction_type, user_id, target_timestamp)


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

        stock_update =  "INSERT INTO stocks (username, stock_symbol, stock_quantity) " \
                        "VALUES ($1, $2, $3) " \
                        "ON CONFLICT (username, stock_symbol) DO UPDATE " \
                        "SET stock_quantity = (stocks.stock_quantity + $3) " \
                        "WHERE stocks.username = $1 " \
                        "AND stocks.stock_symbol = $2;"
        
        await conn.execute(stock_update, user_id, selected["stock_symbol"], selected["stock_quantity"])
        
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
                            
        timestamp = round(time.time(), 5)
        await conn.execute(reserved_update, user_id, stock_symbol,
                sell_quantity, price, sell_price, timestamp)

        # todo: create timer, when timer finishes have it cancel the buy
        #threading.Timer(QUOTE_LIFESPAN, buy_timeout, args=(user_id, stock_symbol, amount, conn)).start()

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

        stock_update =  "UPDATE stocks " \
                        "SET stock_quantity = (stocks.stock_quantity + $3) " \
                        "WHERE stocks.username = $1 " \
                        "AND stocks.stock_symbol = $2;"

        await conn.execute(stock_update, user_id, selected["stock_symbol"], selected["stock_quantity"])

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
        logger.debug("Balance of %s: %.02f", user_id, balance)

        if balance < difference:
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

        logger.info("Balance for transaction %s is sufficient.", transaction_num)

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

        if refund_amount is None:
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

        if existing is None:
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

#TODO: if set_sell of this stock already exists, account for that stock when
#      determining whether user owns enough stock to create set_sell order
#   Also, it is now apparent this logic needs to change.  amount is a dollar amount, this has 
#   been written under the assumption that it is a number of stock
def set_sell_amount(transaction_num, user_id, stock_symbol, amount, conn, XMLTree):
    cursor = conn.cursor()
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
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
    try:
        shares_owned = cursor.fetchone()
    except:
        conn.rollback()
        return
    if shares_owned is None:
        
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
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
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "errorMessage": "User dooes not own enough shares of this type"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)       
        return
    else:   # user owns sufficient shares to proceed, so remove them from user account and create order
        cursor.execute( ' UPDATE stocks                 '
                        ' SET stock_quantity = %s      '
                        ' WHERE username = %s           '
                        ' AND stock_symbol = %s         '
                        ,(amount, user_id, stock_symbol))
        # Does SET_SELL order exist for this user/stock combo?  If yes, modify record, else create new one
        cursor.execute( 'SELECT transaction_amount     '
                        'FROM triggers              '
                        'WHERE username = %s        '
                        'AND stock_symbol = %s      '
                        'AND type = %s;             '
                        ,(user_id, stock_symbol, 'sell')) 
        try:
            result = cursor.fetchone() # this is a tuple containing 1 string or None
        except:
            conn.rollback()
            return
        if result is None:
            cursor.execute( ' INSERT INTO triggers                                                      '
                            ' (username, stock_symbol, type, transaction_amount, transaction_number)    ' 
                            ' VALUES (%s, %s, %s, %s, %s);                                              '
                            ,(user_id, stock_symbol, 'sell', amount, transaction_num))
        else: #modify existing record
            cursor.execute( ' UPDATE triggers SET amount = amount + %s, transaction_number = %s '
                            ' WHERE username = %s                                               '
                            ' AND stock_symbol = %s                                             '
                            ' AND type = %s                                                     '
                            ,(amount, transaction_num, user_id, stock_symbol, 'sell'))
        conn.commit()
    return

def cancel_set_sell(transaction_num, user_id, stock_symbol, conn, XMLTree):
    cursor = conn.cursor()
    
    command = UserCommand()
    attributes = {
        "timestamp": int(time.time() * 1000), 
        "server": "DDJK",
        "transactionNum": transaction_num,
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
    try:
        result = cursor.fetchall()
    except:
        conn.rollback()
        return
    if len(result) == 0:
        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
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

def set_sell_trigger(transaction_num, user_id, stock_symbol, amount, conn, XMLTree):
    cursor = conn.cursor()
    
    try:
        command = UserCommand()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "SET_SELL_TRIGGER",
            "username": user_id,
            "stockSymbol": stock_symbol,
            "funds": float(amount)
        }
        command.updateAll(**attributes)
        XMLTree.append(command)
    except:
        return
    
    cursor.execute( 'SELECT transaction_amount from triggers        '
                    'WHERE username = %s    '
                    'AND stock_symbol = %s  '
                    'AND type = %s;         '
                    ,(user_id, stock_symbol, 'sell'))
    try:
        result = cursor.fetchone()
    except:
        conn.rollback()
        return
    if result is None:

        error = ErrorEvent()
        attributes = {
            "timestamp": int(time.time() * 1000), 
            "server": "DDJK",
            "transactionNum": transaction_num,
            "command": "BUY",
            "username": user_id,
            "errorMessage": "SET_SELL does not exist"
        }
        error.updateAll(**attributes)
        XMLTree.append(error)
        return
    else:
        cursor.execute( ' UPDATE triggers SET trigger_amount = %s,  '
                        '        transaction_number = %s            '  
                        ' WHERE username = %s                       '
                        ' AND stock_symbol = %s                     '
                        ' AND type = %s;                            '
                        ,(amount, transaction_num, user_id, stock_symbol, 'sell'))
        conn.commit()
    return 

# this method is called by an extra thread.  Every QUOTE_LIFESPAN period of time it goes
# through the triggers table.  For any row that posesses a trigger_value, a quote is
# obtained for that stock and if appropriate the buy/sell is triggered
def trigger_maintainer(conn, XMLTree):
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM triggers WHERE trigger_amount IS NOT NULL;')
    try:
        results = cursor.fetchall() # NOTE: this will not scale - we may have HUGE numbers of rows later
                                    # I've done this now though to avoid having stuff on cursor's buffer
    except:
        conn.rollback()
        return
    print("running trigger_maintainer")
    for row in results:
        print("row from 'triggers' as it is read:", row)
        user_id = row[0]
        stock_symbol = row[1]
        buy_or_sell = row[2]
        trigger_amount = row[3]
        transaction_amount = row[4]
        transaction_num= row[5]
        current_price = quote(transaction_num, user_id, stock_symbol, XMLTree)[0]

        # Debugging data
        print("row details: user_id:", user_id, 
            "stock_symbol:", stock_symbol, 
            "buy_or_sell:", buy_or_sell, 
            "trigger_amount:", trigger_amount, 
            "transaction_amount:", transaction_amount, 
            "transaction_number: ", transaction_num, 
            "current_price:", current_price)

        if buy_or_sell == 'buy' and current_price <= trigger_amount: # trigger the buy
            num_stocks_to_buy = int(transaction_amount/current_price)
            leftover_cash = transaction_amount - (current_price * num_stocks_to_buy)

            # if user already had this stock, then update amount
            cursor.execute( 'SELECT stock_quantity from stocks  '
                            'WHERE username = %s                '
                            'AND stock_symbol = %s              '
                            ,(user_id, stock_symbol))
            try:
                result = cursor.fetchone()
            except:
                conn.rollback()
                return

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
                "transactionNum": transaction_num,
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

        # NOTE: this method is still using the incorrect logic - it assums amount == num of stock
        # When making the C++ version, correct the logic to assume amount == dollar amount
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
                "transactionNum": transaction_num,
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
    #threading.Timer(QUOTE_LIFESPAN, trigger_maintainer, args=(conn)).start()

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

    # todo: verify if this can be removed - Daniel
    #time.sleep(30) # hack - fix me
    #XMLTree.write(filename)

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

    # This basically won't work at all atm - Daniel
    #time.sleep(30) # hack - fix me
    #XMLTree.writeFiltered(filename, user_id)


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
