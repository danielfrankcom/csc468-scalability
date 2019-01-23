accounts = []

def add(user_id, amount):
    current_users = [i[0] for i in accounts]
    if user_id in current_users:
        index = current_users.index(user_id)
        accounts[index] = (user_id, float(accounts[index][1]) + float(amount))
        print(accounts)
    else:
        accounts.append((user_id, amount))
        print(accounts)

def quote(user_id, stock_symbol):
    return 0

def buy(user_id, stock_symbol, amount):
    current_users = [i[0] for i in accounts]
    if user_id in current_users:
        index = current_users.index(user_id)
        if float(accounts[index][1]) >= float(amount):
            while True:
                var = input("Please confirm or cancel the transaction (confirm/cancel): ")
                if(var == "confirm"):
                    #Need to implement keeping track of what stocks have been purchased
                    accounts[index] = (user_id, float(accounts[index][1]) - float(amount))
                elif(var == "cancel"):
                    break
            print(accounts)
        else:
            print("Insufficient Funds")
    else:
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
                add(user_id, amount)
        #BUY Command
        elif command == "buy":
            try:
                command, user_id, stock_symbol, amount = var.split()
            except ValueError:
                print("Invalid Input. <ADD, USER_ID, STOCK_SYMBOL, AMOUNT>")
            else:    
                buy(user_id, stock_symbol, amount)

        elif command == "quit":
            break

if __name__ == '__main__':
    main()

