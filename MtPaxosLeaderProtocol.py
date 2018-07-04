# coding = utf-8
from MtMessage import MtMessage
class MtPaxosLeaderProtocol:
    STATE_UNDEFINED = -1 # default state of protocol
    STATE_PROPOSED = 0 # received proposal
    STATE_REJECTED = 1 #
    STATE_AGREED = 2
    STATE_ACCEPTED = 3
    STATE_UNACCEPTED = 4

    def __init__(self, leader):
        self.leader = leader
        self.state = MtPaxosLeaderProtocol.STATE_UNDEFINED
        self.proposalID = {-1, -1}
        self.agreecout, self.acceptcount = (0,0)
        self.rejectcount, self.unacceptcount = (0,0)
        self.instanceID = -1
        self.highestseen = {-1, -1} # 
    
    def propose(self, value, pID, instanceID):
        self.proposalID = pID
        self.value = value
        self.instanceID = instanceID
        # create message
        message = MtMessage(MtMessage.MSG_PROPOSE)
        message.proposalID = pID
        message.instanceID = instanceID
        message.value = value

        for server in self.leader.getAcceptors():
            message.to = server
            self.leader.sendMessage(message)
        self.state = MtPaxosLeaderProtocol.STATE_PROPOSED
        return self.proposalID

    def doTransition(self, message):
        # state transition
        if self.state == MtPaxosLeaderProtocol.STATE_PROPOSED:
            if message.command == MtMessage.MSG_ACCEPTOR_AGREE:
                self.agreecout += 1
                print('leader:{} instance:{} agreecount:{} quorumsize:{} value:{}'.format(self.leader.port, message.instanceID, self.agreecout, self.leader.getQuorumSize(), message.value))
                if self.agreecout >= self.leader.getQuorumSize():
                    print('leader: {} instance:{} got promise value: {}'.format(self.leader.port, message.instanceID, message.value))
                    if message.value != None:
                        if message.sequence[0] > self.highestseen[0] or (message.sequence[0] == self.highestseen[0] and message.sequence[1] > self.highestseen[1]):
                            self.value = message.value
                            self.highestseen = message.sequence
                    self.state = MtPaxosLeaderProtocol.STATE_AGREED
                    # send accept message
                    msg = MtMessage(MtMessage.MSG_ACCEPT)
                    msg.copyAsReply(message)
                    msg.value = self.value
                    msg.leaderID = msg.to # What's this for?
                    for s in self.leader.getAcceptors():
                        msg.to = s
                        self.leader.sendMessage(msg)
                    self.leader.notifyLeader(self, message)
                return True
            elif message.command == MtMessage.MSG_ACCEPTOR_REJECT:
                self.rejectcount += 1
                if self.rejectcount >= self.leader.getQuorumSize():
                    self.state = MtPaxosLeaderProtocol.STATE_REJECTED
                    self.leader.notifyLeader(self, message)
                return True
        if self.state == MtPaxosLeaderProtocol.STATE_AGREED:
            if message.command == MtMessage.MSG_ACCEPTOR_ACCEPT:
                self.acceptcount += 1
                print('leader: {} instance:{} acceptcount:{} value: {} '.format(self.leader.port, message.instanceID, self.acceptcount, message.value))
                if self.acceptcount >= self.leader.getQuorumSize():
                    self.state = MtPaxosLeaderProtocol.STATE_ACCEPTED
                    self.leader.notifyLeader(self, message)
            if message.command == MtMessage.MSG_ACCEPTOR_UNACCEPT:
                self.unacceptcount += 1
                if self.unacceptcount >= self.leader.getQuorumSize():
                    self.state = MtPaxosLeaderProtocol.STATE_UNACCEPTED
                    self.leader.notifyLeader(self, message)
        pass
        


