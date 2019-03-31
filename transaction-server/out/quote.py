from threading import Thread

import traceback
import random
import string
import socket
import time

def spinner():
    while True:

        success = False
        while not success:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                with s:
                    s.bind(("0.0.0.0", 1246))                    # source
                    s.connect(("quoteserve.seng.uvic.ca", 4444)) # destination

                    username = "".join(random.choice(string.ascii_lowercase) for x in range(10))
                    stock = "".join(random.choice(string.ascii_uppercase) for x in range(3))
                    message = stock + "," + username + "\n"

                    encoded = message.encode()
                    s.send(encoded)

                    data = s.recv(1024)
                    decoded = data.decode()

                    s.shutdown(socket.SHUT_RDWR)

                print(decoded.strip())
                success = True

            except:
                #traceback.print_exc()
                #time.sleep(1)
                pass

threads = [Thread(target=spinner) for i in range(5)]
for thread in threads:
    thread.start()
