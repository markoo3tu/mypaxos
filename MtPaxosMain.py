# coding = utf-8
import threading,socket,pickle,random
from multiprocessing import Queue 
from MtMessagePump import MtMessageBump
from MtMessage import MtMessage
from MtInstanceRecord import MtInstanceRecord
from MtPaxosLeader import MtPaxosLeader
from MtPaxosLeaderProtocol import MtPaxosLeaderProtocol
from MtPaxosAcceptorProtocol import MtPaxosAcceptorProtocol
from MtPaxosAcceptor import MtPaxosAcceptor

import time
SVR_PORT_ONE = 34321
SVR_PORT_TWO = 34322
CLIENT_PORT_ONE = 43420

if __name__ == '__main__':
    numclients = 5
    clients = [MtPaxosAcceptor(port, [SVR_PORT_ONE, SVR_PORT_TWO]) for port in range(CLIENT_PORT_ONE, CLIENT_PORT_ONE + numclients)]
    leader1 = MtPaxosLeader(SVR_PORT_ONE, [SVR_PORT_TWO], [c.port for c in clients])
    leader2 = MtPaxosLeader(SVR_PORT_TWO, [SVR_PORT_ONE], [c.port for c in clients])
    
    # start leaders
    leader1.start()
    leader2.setPrimary(True)
    leader2.start()
    for c in clients:
        c.start()

    # simulating clients failure
    clients[0].fail()
    clients[1].fail()

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    start=time.time()
    for i in range(30):
        m = MtMessage(MtMessage.MSG_EXT_PROPOSE)
        m.value = 0 + i
        m.to = SVR_PORT_TWO # notify leader
        mdata = pickle.dumps(m)
        s.sendto(mdata, ('localhost', m.to))

    print('leader: {} getNumAccepted: {}'.format(leader1.port, leader1.getNumAccepted()))
    time.sleep(10)
    leader1.stop()
    leader2.stop()
    for c in clients:
        c.stop()
    print ("leader: {} history: {}".format(leader1.port, leader1.getHistory()))
    print ("leader: {} history: {}".format(leader2.port, leader2.getHistory()))
    end=time.time()

    print('total time:{}'.format(end - start))
    


