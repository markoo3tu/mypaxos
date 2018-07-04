# coding = utf -8
from MtMessage import MtMessage
class MtPaxosAcceptorProtocol:
    # State variables
    STATE_UNDEFINED = -1 # default state of protocol
    STATE_PROPOSAL_RECEIVED = 0 # received proposal
    STATE_PROPOSAL_REJECTED = 1 #
    STATE_PROPOSAL_AGREED = 2
    STATE_PROPOSAL_ACCEPTED = 3
    STATE_PROPOSAL_UNACCEPTED = 4

    def __init__(self, client):
        self.client = client
        self.state = MtPaxosAcceptorProtocol.STATE_UNDEFINED

    def recvProposal(self, message):
        if message.command == MtMessage.MSG_PROPOSE:
            self.proposalID = message.proposalID
            self.instanceID = message.instanceID
            (port, count) = self.client.getHighestAgreedProposal(message.instanceID)
            # check if id is the highest, count is prior to port
            if count < self.proposalID[1] or (count == self.proposalID[1] and port < self.proposalID[0] ):
                self.state = MtPaxosAcceptorProtocol.STATE_PROPOSAL_AGREED # agree to update
                print("accept: {} leader: {} proposal: {} instance: {} value: {}".format(message.to, message.source, message.proposalID, message.instanceID, message.value))

                value = self.client.getInstanceValue(message.instanceID)
                msg = MtMessage(MtMessage.MSG_ACCEPTOR_AGREE)
                msg.copyAsReply(message)
                msg.value = value
                msg.sequence = (port, value)
                self.client.sendMessage(msg)
            else:
                self.state = MtPaxosAcceptorProtocol.STATE_PROPOSAL_REJECTED
            return self.proposalID
        else:
            pass
    
    def doTransition(self, message):
        if self.state == MtPaxosAcceptorProtocol.STATE_PROPOSAL_AGREED and message.command == MtMessage.MSG_ACCEPT:
            self.state = MtPaxosAcceptorProtocol.STATE_PROPOSAL_ACCEPTED
            msg = MtMessage(MtMessage.MSG_ACCEPTOR_ACCEPT)
            msg.copyAsReply(message)
            print('acceptor: {} leader: {} instance: {} value: {}'.format(message.to, message.source, message.instanceID, message.value))
            for l in self.client.leaders:
                msg.to = l
                self.client.sendMessage(msg)
            self.notifyClient(message)
            return True
    
    def notifyClient(self, message):
        self.client.notifyClient(self, message)
        