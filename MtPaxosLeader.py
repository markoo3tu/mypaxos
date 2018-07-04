# coding = utf-8
from multiprocessing import Queue
import time
from MtMessage import MtMessage
from MtMessagePump import MtMessageBump
from MtInstanceRecord import MtInstanceRecord
from MtPaxosAcceptorProtocol import MtPaxosAcceptorProtocol
from MtPaxosLeaderProtocol import MtPaxosLeaderProtocol
import threading

class MtPaxosLeader:
    def __init__(self, port, leaders=None, acceptors=None):
        self.port = port
        self.leaders = leaders if leaders != None else []
        self.acceptors = acceptors if acceptors != None else []
        self.group = self.leaders + self.acceptors
        self.isPrimary = False
        self.proposalCount = 0
        self.msgPump = MtMessageBump(self, port)
        self.instances = {}
        self.hbListener = MtPaxosLeader.HeartbeatListener(self)
        self.hbSender = MtPaxosLeader.HeartbeatSender(self)
        self.highestInstance = -1
        self.stopped = True
        self.lasttime = time.time()
    
    class HeartbeatListener(threading.Thread):
        def __init__(self, leader):
            self.leader = leader
            self.queue = Queue()
            self.abort = False
            threading.Thread.__init__(self)
        
        def newHB(self, message):
            self.queue.put(message)

        def doAbort(self):
            self.abort = True
        
        def run(self):
            elapsed = 0
            while not self.abort:
                s = time.time()
                try:
                    hb = self.queue.get(True, 2)
                    # elect the one whose port number is greater
                    if hb.source > self.leader.port:
                        self.leader.setPrimary(False)
                except:
                    self.leader.setPrimary(True)

    class HeartbeatSender(threading.Thread):
        def __init__(self, leader):
            self.leader = leader
            self.abort = False
            threading.Thread.__init__(self)

        def doAbort(self):
            self.abort = True
        
        def run(self):
            while not self.abort:
                time.sleep(1)
                if self.leader.isPrimary:
                    msg = MtMessage(MtMessage.MSG_HEARTBEAT)
                    msg.source = self.leader.port
                    for l in self.leader.leaders:
                        msg.to = l
                        self.leader.sendMessage(msg)
    
    def sendMessage(self, message):
        self.msgPump.sendMessage(message)
    
    def start(self):
        self.hbSender.start()
        self.hbListener.start()
        self.msgPump.start()
        self.stopped = False
    
    def stop(self):
        self.hbSender.doAbort()
        self.hbListener.doAbort()
        self.msgPump.doAbort()
        self.stopped = True
        