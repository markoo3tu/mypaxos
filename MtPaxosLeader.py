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

    def setPrimary(self, primary):
        if self.isPrimary != primary:
            if primary:
                print(u"leader: {} changes to leader".format(self.port))
            else:
                print(u"leader: {} changes to no leader".format(self.port))
        self.isPrimary = primary
    
    def getGroup(self):
        return self.group

    def getLeaders(self):
        return self.leaders
    
    def getAcceptors(self):
        return self.acceptors
    
    def getQuorumSize(self):
        return int(len(self.getAcceptors()) / 2) + 1

    def getInstanceValue(self, instanceID):
        if instanceID in self.instances:
            return self.instances[instanceID].value
        return None
    
    def getHistory(self):
        return [self.getInstanceValue(i) for i in range(0, self.highestInstance + 1)]

    def getNumAccepted(self):
        return len([v for v in self.getHistory() if v != None])

    # ---------------------------------------------------
    def findAndFillGaps(self):
        # if no message is received, we take the chance to do a little cleanup
        for i in range(0, self.highestInstance):
            if self.getInstanceValue(i) == None:
                print('fill the None instance: {}'.format(i))
                self.newProposal(0, i)
        self.lasttime = time.time()
    
    def garbageCollect(self):
        for i in self.instances:
            self.instances[i].cleanProtocols()

    def recvMessage(self, message):
        if self.stopped:
            return
        if message == None:
            if self.isPrimary and time.time() - self.lasttime > 15.0:
                self.findAndFillGaps() # fill some proposal
                self.garbageCollect()
            return
        if message.command == MtMessage.MSG_HEARTBEAT:
            self.hbListener.newHB(message)
            return True
        if message.command == MtMessage.MSG_EXT_PROPOSE:
            print(u'leader:{} new request value:{} current instance:{}'.format(self.port, message.value, self.highestInstance))
            if self.isPrimary:
                self.newProposal(message.value)
            return True
        if self.isPrimary and message.command != MtMessage.MSG_ACCEPTOR_ACCEPT:
            self.instances[message.instanceID].getProtocol(message.proposalID).doTransition(message)
        if message.command == MtMessage.MSG_ACCEPTOR_ACCEPT:
            if message.instanceID not in self.instances:
                self.instances(message.instanceID) = MtInstanceRecord()
            record = self.instances[message.instanceID] 
            if message.proposalID not in record.cleanProtocols:
                protocol = MtPaxosLeaderProtocol(self)
                protocol.state = MtPaxosLeaderProtocol.STATE_AGREED
                protocol.proposalID = message.proposalID
                protocol.instanceID = message.instanceID
                protocol.value = message.value
                record.addProtocol(protocol)
                print(u'leader:{} new propose:{} instance:{} acceptor:{} value:{}'.format(self.port, message.proposalID, message.instanceID, message.source, message.value))
            else:
                protocol = record.getProtocol(message.proposalID)
            protocol.doTransition(message)
        return True

    def newProposal(self, value, instance=None):
        protocol = MtPaxosLeaderProtocol(self)
        if instance == None:
            self.highestInstance += 1
            instanceID = self.highestInstance
        else:
            instanceID = instance
        self.proposalCount += 1
        id = (self.port, self.proposalCount)
        if instanceID in self.instances:
            record = self.instances[instanceID]
        else:
            record = MtInstanceRecord()
            self.instances[instanceID] = record
        protocol.propose(value, id, instanceID)
        record.addProtocol(protocol)
    
    def notifyLeader(self, protocol, message)
        if protocol.state == MtPaxosLeaderProtocol.STATE_ACCEPTED:
            print(u'leader:{} accepted instance:{} value:{} sender:{}'.format(self.port, message.instanceID, message.value, message.source) )
            self.instances[message.instanceID].accepted = True
            self.instnaces[message.instanceID].value = message.value
            self.highestInstance = max(message.instanceID, self.highestInstance)
            return
        if protocol.state == MtPaxosLeaderProtocol.STATE_REJECTED:
            # try again using next instance
            self.proposalCount = max(self.proposalCount, message.highestPID[1])
            self.newProposal(message.value) 
            return True
        if protocol.state == MtPaxosLeaderProtocol.STATE_UNACCEPTED:
            pass
            





    