# coding = utf-8
import threading, socket, pickle, random
from multiprocessing import Queue
from MtPaxosLeaderProtocal import MtPaxosLeaderProtocal
class MtInstanceRecord:
    def __init__(self):
        self.protocals = {} # protocol dictionary
        self.highestID = (-1, -1) # The highest proposal id
        self.value = None
    def addProtocol(self, protocol): # Add new protocol
        # add to dictionary
        self.protocols[protocol.proposalID] = protocol
        if protocol.proposalID[1] > self.highestID[1] or (protocol.proposalID[1] == self.highestID[1] and protocol.proposalID[0] > self.highestID[0]):
            self.highestID = protocol.proposalID # the largest id win
    def getProtocol(self, protocolID):
        return self.protocals[protocolID]

    def cleanProtocols(self):
        keys = self.protocals.keys()
        for k in keys:
            protocol = self.protocals[k]
            if protocol.state == MtPaxosLeaderProtocal.STATE_ACCEPTED:
                print('Deleting protocol')
                del self.protocals[k]

