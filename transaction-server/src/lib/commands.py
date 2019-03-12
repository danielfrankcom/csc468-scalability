import psycopg2
import random

def add(transaction_num, user_id, amount, conn):

    function =  "INSERT INTO users (username, balance) " \
                "VALUES ('{username}', {amount}) " \
                "ON CONFLICT (username) DO UPDATE " \
                "SET balance = (users.balance + {amount}) " \
                "WHERE users.username = '{username}';".format(username=user_id, amount=amount)
    
    print("about to execute " + str(transaction_num), flush=True)
    conn.cursor().execute(function, (user_id + str(random.randint(0, 1000)), amount))
    conn.commit()
    print("finished executing " + str(transaction_num), flush=True)
