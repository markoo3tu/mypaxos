#coding=utf-8

class MtMessage:
    MSG_ACCEPTOR_AGREE = 0 # Acceptor agreed 
    MSG_ACCEPTOR_ACCEPT = 1 # Acceptor accepted
    MSG_ACCEPTOR_REJECT = 2 # Acceptor rejected, offline
    MSG_ACCEPTOR_UNACCEPT = 3 # Acceptor online but disagree
    MSG_ACCEPT = 4 # Accept message sent when leader receive enough agreed acceptor
    MSG_PROPOSE = 5 # Propose message
    MSG_EXT_PROPOSE = 6 # client's request
    MSG_HEARTBEAT = 7 # Heartbeat 
    def __init__(self, command = None):
        self.command = command

    def copyAsReply(self, message):
        self.proposalID, self.instanceID, self.to, \
        self.source = message.proposalID, \
        message.instanceID, message.source, message.to
        self.value = message.value

        
