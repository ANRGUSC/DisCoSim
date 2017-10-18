import threading

class SyncEcho():
    def __init__(self):
        self.print_lock = threading.Lock()

    def __call__(self,msg):
        with self.print_lock:
            print(msg)

echo = SyncEcho()