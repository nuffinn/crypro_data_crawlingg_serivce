import requests
import threading
import queue

q = queue.Queue()
valid_proxies = []

with open("proxies.txt", "r") as f:
    proxies = f.read().split("\n")
    for p in proxies:
        q.put(p)

v = open("valid.txt", "a")

def check():
    global q
    while not q.empty():
        proxy = q.get()
        try:
            res = requests.get("https://www.coingecko.com/en/bitcoin",
                               proxies={"http": proxy,
                                        "https": proxy})
        except:
            continue
        if res.status_code == 200:
            v.write(proxy+ "\n")


for _ in range(10):
    threading.Thread(target=check).start()

v.close()
