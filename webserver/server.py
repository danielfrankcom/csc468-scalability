from flask import Flask
app = Flask(__name__)

@app.route("/")
def root():
    return "DDJK Web Server"

@app.route("/add/<userid>/<float:amount>")
def add(userid,amount):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/quote/<userid>/<stockSymbol>")
def quote(userid,stockSymbol):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/buy/<userid>/<stockSymbol>/<float:amount>")
def buy(userid,stockSymbol,amount):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/commit_buy/<userid>")
def commit_buy(userid):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string
    
@app.route("/cancel_buy/<userid>")
def cancel_buy(userid):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/sell/<userid>/<stockSymbol>/<float:amount>")
def sell(userid,stockSymbol,amount):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/commit_sell/<userid>")
def commit_sell(userid):
    pass

@app.route("/cancel_sell/<userid>")
def cancel_sell(userid):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/set_buy_amount/<userid>/<stockSymbol>/<float:amount>")
def set_buy_amount(userid,stockSymbol,amount):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/cancel_set_buy/<userid>/<stockSymbol>")
def cancel_set_buy(userid,stockSymbol):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/set_buy_trigger/<userid>/<stockSymbol>/<float:amount>")
def set_buy_trigger(userid,stockSymbol,amount):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/set_sell_amount/<userid>/<stockSymbol>/<float:amount>")
def set_sell_amount(userid,stockSymbol,amount):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/set_sell_trigger/<userid>/<stockSymbol>/<float:amount>")
def set_sell_trigger(userid,stockSymbol,amount):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/cancel_set_sell/<userid>/<stockSymbol>")
def cancel_set_sell(userid,stockSymbol):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/dumplog/<userid>/<filename>")
def dumplog(userid,filename):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/dumplog/<filename>")
def dumplog_all(filename):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string

@app.route("/display_summary/<userid>")
def display_summary(userid):
    args = locals()
    arg_string = ""
    for _,v in args.items():
        arg_string+='{0} '.format(v)
    return arg_string




