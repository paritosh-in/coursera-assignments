/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */

//#define DEBUGLOG_1
//#define DEBUGLOG_2
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
    for (int i = 0; i < 6; i++) {
        NULLADDR[i] = 0;
    }
    this->memberNode = member;
    this->emulNet = emul;
    this->log = log;
    this->par = params;
    this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue <q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if (initThisNode(&joinaddr) == -1) {
#ifdef DEBUGLOG_1
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if (!introduceSelfToGroup(&joinaddr)) {
        finishUpThisNode();
#ifdef DEBUGLOG_1
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
    /*
     * This function is partially implemented and may require changes
     */
    int id = *(int *) (&memberNode->addr.addr);
    int port = *(short *) (&memberNode->addr.addr[4]);

    memberNode->bFailed = false;
    memberNode->inited = true;
    memberNode->inGroup = false;
    // node is up!
    memberNode->nnb = 0;
    memberNode->heartbeat = 0;
    memberNode->pingCounter = TFAIL;
    memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
    MessageHdr *msg;
#ifdef DEBUGLOG_1
    static char s[1024];
#endif

    if (0 == memcmp((char *) &(memberNode->addr.addr), (char *) &(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG_1
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    } else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *) (msg + 1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *) (msg + 1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG_1
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *) msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode() {
    /*
     * Your code goes here
     */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
        return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if (!memberNode->inGroup) {
        return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while (!memberNode->mp1q.empty()) {
        ptr = memberNode->mp1q.front().elt;
        size = memberNode->mp1q.front().size;
        memberNode->mp1q.pop();
        recvCallBack((void *) memberNode, (char *) ptr, size);
    }
    return;
}


MessageHdr *MP1Node::newMessage(MsgTypes type, Address addr, vector <MemberListEntry> memberList) {
    MessageHdr *msg;

    long memberListSize = memberList.size();
    size_t msgsize =
            sizeof(MessageHdr) + sizeof(addr.addr) + memberListSize * sizeof(MemberListEntry) + sizeof(long) + 1;
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // create JOINREQ message: format of data is {struct Address myaddr}
    msg->msgType = type;
    memcpy((char *) (msg + 1), &(addr.addr), sizeof(addr.addr));
    memcpy((char *) (msg + 1) + 1 + sizeof(addr.addr), &memberListSize, sizeof(long));
    std::copy(reinterpret_cast<char *>(memberList.data()),
              reinterpret_cast<char *>(memberList.data()) + memberListSize * sizeof(MemberListEntry),
              (char *) (msg + 1) + 1 + sizeof(addr.addr) + sizeof(long));

    return msg;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size) {
    /*
     * Your code goes here
     */

    Member *self = (Member *) env;
    MessageHdr *msg = (MessageHdr *) data;
    Address *addr = (Address *) malloc(sizeof(Address));
    memcpy((char *) (addr->addr), (char *) (msg + 1), sizeof(addr->addr));

    switch (msg->msgType) {
        case JOINREQ: {
#ifdef DEBUGLOG_1
            static char s[1024];
            sprintf(s, "Received JOINREQ from %d.%d.%d.%d:%d", addr->addr[0], addr->addr[1], addr->addr[2], addr->addr[3], addr->addr[4]);
            log->LOG(&self->addr, s);
#endif

            long *heartbeat = (long *) (msg + 1 + sizeof(addr->addr));

            int id = *(int *) (&addr->addr);
            short port = *(short *) (&addr->addr[4]);
            MemberListEntry mle(id, port, *heartbeat, this->par->getcurrtime());
            printf("\nMember Added : %d:%d", mle.id, mle.port);
            memberNode->memberList.push_back(mle);
            log->logNodeAdd(&self->addr, addr);
            MessageHdr *sendMsg = newMessage(JOINREP, self->addr, memberNode->memberList);
            emulNet->ENsend(&memberNode->addr, addr, (char *) (sendMsg), sizeof(MessageHdr) + sizeof(Address) +
                                                                         memberNode->memberList.size() *
                                                                         sizeof(MemberListEntry) + 1);
            free(sendMsg);
        }
            break;

        case JOINREP: {
            self->inGroup = true;

            long membershipSize;
            memcpy((char *) (&membershipSize), (char *) (msg + 1) + 1 + sizeof(addr->addr), sizeof(long));
            std::vector <MemberListEntry> membership;
            membership.resize(membershipSize);

            std::copy((char *) (msg + 1) + 1 + sizeof(long) + sizeof(addr->addr),
                      (char *) (msg + 1) + 1 + sizeof(long) + sizeof(addr->addr) +
                      membershipSize * sizeof(MemberListEntry), reinterpret_cast<char *>(membership.data()));
            updateMembershipList(membership);
#ifdef DEBUGLOG_1
            log->LOG(&self->addr, "Received JOINREP........................");
      printMembership(membership);
      log->LOG(&self->addr, "After Membership Update................");
      printMembership(memberNode->memberList);
#endif
        }
            break;
        case HEARTBEAT: {
            long membershipSize;
            memcpy((char *) (&membershipSize), (char *) (msg + 1) + 1 + sizeof(addr->addr), sizeof(long));
            std::vector <MemberListEntry> membership;
            membership.resize(membershipSize);
            std::copy((char *) (msg + 1) + 1 + sizeof(long) + sizeof(addr->addr),
                      (char *) (msg + 1) + 1 + sizeof(long) + sizeof(addr->addr) +
                      membershipSize * sizeof(MemberListEntry), reinterpret_cast<char *>(membership.data()));
#ifdef DEBUGLOG_1
            static char s[1024];
            sprintf(s, "Received Heartbeat from %d.%d.%d.%d:%d", addr->addr[0], addr->addr[1], addr->addr[2], addr->addr[3], addr->addr[4]);
            log->LOG(&self->addr, s);
            printMembership(membership);
            log->LOG(&self->addr, "Before Heartbeat Update....................");
            printMembership(memberNode->memberList);
#endif

            updateMembershipList(membership);

#ifdef DEBUGLOG_1
            log->LOG(&self->addr, "After Membership Update......................");
            printMembership(memberNode->memberList);
#endif
        }
            break;
    }

    free(msg);
    free(addr);
}

void MP1Node::printMembership(std::vector <MemberListEntry> memberList) {
    std::vector<MemberListEntry>::iterator it = memberList.begin();
    while (it != memberList.end()) {
        Address addr = getAddress((*it).id, (*it).port);
        static char s[1024];
        sprintf(s, "%d.%d.%d.%d:%d = %d", addr.addr[0], addr.addr[1], addr.addr[2], addr.addr[3], addr.addr[4],
                (*it).heartbeat);
        log->LOG(&memberNode->addr, s);
        ++it;
    }
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
#ifdef DEBUGLOG_2
    log->LOG(&memberNode->addr, "MP1Node::nodeLoopOps");
#endif
    memberNode->heartbeat++;
    memberNode->memberList[0].heartbeat = memberNode->heartbeat;
    memberNode->memberList[0].timestamp = par->getcurrtime();

    removeMembersIfFailed();
    sendHeartbeat();
    return;
}

void MP1Node::removeMembersIfFailed() {
#ifdef DEBUGLOG_2
    log->LOG(&memberNode->addr, "MP1Node::removeMembersIfFailed");
#endif
    auto newEnd = std::remove_if(memberNode->memberList.begin(), memberNode->memberList.end(),
                                 [this](MemberListEntry mle) {
                                     if (mle.timestamp + TREMOVE <= par->getcurrtime()) {
                                         Address addr = getAddress(mle.id, mle.port);
                                         log->logNodeRemove(&memberNode->addr, &addr);
                                         return true;
                                     }
                                 }
    );
    memberNode->memberList.erase(newEnd, memberNode->memberList.end());
}


void MP1Node::sendHeartbeat() {
#ifdef DEBUGLOG_2
    log->LOG(&memberNode->addr, "MP1Node::sendHeartbeat");
#endif
    std::random_device rd;
    std::mt19937 g(rd());
#ifdef DEBUGLOG_2
    printMembership(memberNode->memberList);
#endif
    std::vector <MemberListEntry> v;
    for (int i = 1; i < memberNode->memberList.size(); i++) {
        if (memberNode->memberList[i].timestamp + TFAIL > par->getcurrtime())
            v.push_back(memberNode->memberList[i]);
    }
#ifdef DEBUGLOG_2
    log->LOG(&memberNode->addr, "MP1Node::sendHeartbeat::Failed Filtering");
#endif
    std::shuffle(v.begin(), v.end(), g);

    v.push_back(memberNode->memberList[0]);
#ifdef DEBUGLOG_2
    printMembership(v);
#endif

    MessageHdr *msg = newMessage(HEARTBEAT, memberNode->addr, v);
    Address *addr = (Address *) malloc(sizeof(Address));
    int msgSize = sizeof(MessageHdr) + sizeof(Address) + v.size() * sizeof(MemberListEntry) + sizeof(long) + 1;
    static char s[1024];

#if 1
    if (v.size() > 1) {
        Address addr0 = getAddress(v[0].id, v[0].port);
        memcpy((char *) addr, (char *) (&addr0), sizeof(Address));
#ifdef DEBUGLOG_1
        sprintf(s, "Sending heartbeat to %d.%d.%d.%d:%d", addr0.addr[0], addr0.addr[1], addr0.addr[2], addr0.addr[3], addr0.addr[4]);
        log->LOG(&memberNode->addr, s);
        printMembership(v);
#endif
        emulNet->ENsend(&memberNode->addr, addr, (char *) (msg), msgSize);
    }

    if (v.size() > 2) {
        Address addr1 = getAddress(v[1].id, v[1].port);
        memcpy((char *) addr, (char *) (&addr1), sizeof(Address));
#ifdef DEBUGLOG_1
        sprintf(s, "Sending heartbeat to %d.%d.%d.%d:%d", addr1.addr[0], addr1.addr[1], addr1.addr[2], addr1.addr[3], addr1.addr[4]);
        log->LOG(&memberNode->addr, s);
#endif
        emulNet->ENsend(&memberNode->addr, addr, (char *) (msg), msgSize);
    }
#endif

    free(msg);
    free(addr);
}


/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
    return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *) (&joinaddr.addr) = 1;
    *(short *) (&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();

    MemberListEntry myEntry(getId(memberNode->addr), getPort(memberNode->addr), memberNode->heartbeat,
                            this->par->getcurrtime());
    memberNode->memberList.push_back(myEntry);
    memberNode->myPos = memberNode->memberList.begin();
}

void MP1Node::updateMembershipList(std::vector <MemberListEntry> receivedMemberList) {
    std::for_each(receivedMemberList.begin(), receivedMemberList.end(), [this](MemberListEntry mle) {
        std::vector<MemberListEntry>::iterator it = std::find_if(memberNode->memberList.begin(),
                                                                 memberNode->memberList.end(),
                                                                 [mle](const MemberListEntry &m) {
                                                                     return mle.id == m.id && mle.port == m.port;
                                                                 });
        if (it != memberNode->memberList.end()) {
            if ((int) (mle.heartbeat) > (int) ((*it).heartbeat)) {
                (*it).heartbeat = mle.heartbeat;
                (*it).timestamp = par->getcurrtime();
            }
        } else {
            mle.timestamp = par->getcurrtime();
            memberNode->memberList.push_back(mle);
            Address addr = getAddress(mle.id, mle.port);
            log->logNodeAdd(&memberNode->addr, &addr);
        }
    });
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr) {
    printf("%d.%d.%d.%d:%d \n", addr->addr[0], addr->addr[1], addr->addr[2],
           addr->addr[3], *(short *) &addr->addr[4]);
}

Address MP1Node::getAddress(int id, short port) {
    Address addr;
    memcpy(&(addr.addr[0]), &id, sizeof(int));
    memcpy(&(addr.addr[4]), &port, sizeof(short));
    return addr;
}

int MP1Node::getId(Address addr) {
    return *(int *) (&addr.addr[0]);
}

short MP1Node::getPort(Address addr) {
    return *(short *) (&addr.addr[4]);
}
