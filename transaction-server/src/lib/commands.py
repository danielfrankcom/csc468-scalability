import psycopg2

def add(transaction_num, user_id, amount, conn):

    function =  "INSERT INTO users (username, balance) VALUES (%s, %s);"
    
    print("about to execute " + str(transaction_num), flush=True)
    conn.cursor().execute(function, (user_id, amount))
    conn.commit()
    print("finished executing " + str(transaction_num), flush=True)
