# coding = utf-8
from MtMessage import MtMessage
from MtMessagePump import MtMessageBump
from MtInstanceRecord import MtInstanceRecord
from MtPaxosAcceptorProtocol import MtPaxosAcceptorProtocol

class MtPaxosAcceptor:
    def __init__(self, port, leaders):
        self.port = port
        self.leaders = leaders
        self.instances = {} # instances dictionary
        self.msgPump = MtMessageBump(self, self.port)
        self.failed = False

    def start(self):
        self.msgPump.start()

    def stop(self):
        self.msgPump.doAbort()

    def fail(self):
        self.failed = True

    def recover(self):
        self.failed = False
    
    def sendMessage(self, message):
        self.msgPump.sendMessage(message)

    def recvMessage(self, message):
        if message == None:
            return
        if self.failed:
            return
        if message.command == MtMessage.MSG_PROPOSE:
            if message.instanceID not in self.instances:
                record = MtInstanceRecord()
                self.instances[message.instanceID] = record
            protocol = MtPaxosAcceptorProtocol(self)
            protocol.recvProposal(message)
            self.instances[message.instanceID].addProtocol(protocol)
        else:
            self.instances[message.instanceID].getProtocol(message.proposalID)
    
    def notifyClient(self, protocol, message):
        if protocol.state == MtPaxosAcceptorProtocol.STATE_PROPOSAL_ACCEPTED:
            self.instances[protocol.instanceID].value = message.value
            print(u'acceptor: %s accepted instance: %s %s, value:%s'.format(self.port, protocol.instanceID, message.instanceID, message.value))

    def getHighestAgreedProposal(self, instance):
        return self.instances[instance].highestID

    def getInstanceValue(self, instance):
        return self.instances[instance].value

        