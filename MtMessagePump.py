# coding=utf-8 
import threading
import pickle
import socket
from multiprocessing import Queue

class MtMessageBump(threading.Thread):
        # receive thread
        class MtMpHelper(threading.Thread):
            def __init__(self, owner):
                self.owner = owner
                threading.Thread.__init__(self)
            
            def run(self):
                while not self.owner.abort:
                    try:
                        (recvData, addr) = self.owner.socket.recvfrom(2048)
                        msg = pickle.loads(recvData)
                        msg.source = addr[1] # why get port number?
                        self.owner.queue.put(msg)
                    except Exception as e:
                        pass
        def __init__(self, owner, port, timeout=20):
            self.owner = owner
            threading.Thread.__init__(self)
            self.abort = False
            self.timeout = timeout
            self.port = port
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 200000)
            self.socket.bind(('localhost', port))
            self.socket.settimeout(timeout)
            self.queue = Queue()
            self.helper = MtMessageBump.MtMpHelper(this)

        def run(self):
            self.helper.start()
            while not self.abort:
                message = self.waitForMessage()
                self.owner.recvMessage(message)

        def waitForMessage(self):
            try:
                msg = self.queue.get(True, 3)
                return msg
            except:
                return None

        def sendMessage(self, message):
            bytes = pickle.dumps(message)
            address = ('localhost', message.to)
            self.socket.sendto(bytes, address)
            return True
        
        def doAbort(self):
            self.abort = True
                        