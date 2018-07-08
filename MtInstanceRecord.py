# coding = utf-8
import threading, socket, pickle, random
from multiprocessing import Queue
from MtPaxosLeaderProtocol import MtPaxosLeaderProtocol
class MtInstanceRecord:
    def __init__(self):
        self.protocols = {} # protocol dictionary
        self.highestID = (-1, -1) # The highest proposal id
        self.value = None
    def addProtocol(self, protocol): # Add new protocol
        # add to dictionary
        self.protocols[protocol.proposalID] = protocol
        if protocol.proposalID[1] > self.highestID[1] or (protocol.proposalID[1] == self.highestID[1] and protocol.proposalID[0] > self.highestID[0]):
            self.highestID = protocol.proposalID # the largest id win
    def getProtocol(self, protocolID):
        return self.protocols[protocolID]

    def cleanProtocols(self):
        keys = self.protocols.keys()
        for k in keys:
            protocol = self.protocols[k]
            if protocol.state == MtPaxosLeaderProtocol.STATE_ACCEPTED:
                print('Deleting protocol')
                del self.protocols[k]

