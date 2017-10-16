/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *address) {
    this->memberNode = memberNode;
    this->par = par;
    this->emulNet = emulNet;
    this->log = log;
    ht = new HashTable();
    transactions = map<int, Transaction>();
    this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
    delete ht;
    delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
    /*
     * Implement this. Parts of it are already implemented
     */
    vector <Node> curMemList;
    bool change = false;

    /*
     *  Step 1. Get the current membership list from Membership Protocol / MP1
     */
    curMemList = getMembershipList();

    /*
     * Step 2: Construct the ring
     */
    // Sort the list based on the hashCode
    sort(curMemList.begin(), curMemList.end());


    ring.clear();
    ring = curMemList;
    /*
     * Step 3: Run the stabilization protocol IF REQUIRED
     */
    // Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
    stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector <Node> MP2Node::getMembershipList() {
    unsigned int i;
    vector <Node> curMemList;
    for (i = 0; i < this->memberNode->memberList.size(); i++) {
        Address addressOfThisMember;
        int id = this->memberNode->memberList.at(i).getid();
        short port = this->memberNode->memberList.at(i).getport();
        memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
        memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
        curMemList.emplace_back(Node(addressOfThisMember));
    }
    return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
    std::hash <string> hashFunc;
    size_t ret = hashFunc(key);
    return ret % RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
    vector<Node> nodes = findNodes(key);
    Message message(g_transID++, this->getMemberNode()->addr, CREATE, key, value);
    recordTransaction(message);
    for(Node node : nodes) {
        dispatchMessage(message, node.getAddress());
    }
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key) {
    vector<Node> nodes = findNodes(key);
    Message message(g_transID++, getMemberNode()->addr, READ, key);
    recordTransaction(message);
    for(Node node : nodes) {
        dispatchMessage(message, node.getAddress());
    }
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value) {
    vector<Node> nodes = findNodes(key);
    Message message(g_transID++, getMemberNode()->addr, UPDATE, key, value);
    recordTransaction(message);
    for(Node node : nodes) {
        dispatchMessage(message, node.getAddress());
    }
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key) {
    vector<Node> nodes = findNodes(key);
    Message message(g_transID++, getMemberNode()->addr, DELETE, key);
    recordTransaction(message);
    for(Node node : nodes) {
        dispatchMessage(message, node.getAddress());
    }
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
    // Insert key, value, replicaType into the hash table
    return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
    // Read key from local hash table and return value
    return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
    // Update key in local hash table and return true or false
    return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
    // Delete the key from the local hash table
    return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
    /*
     * Implement this. Parts of it are already implemented
     */
    char *data;
    int size;

    /*
     * Declare your local variables here
     */

    // dequeue all messages and handle them
    while (!memberNode->mp2q.empty()) {
        /*
         * Pop a message from the queue
         */
        data = (char *) memberNode->mp2q.front().elt;
        size = memberNode->mp2q.front().size;
        memberNode->mp2q.pop();

        string message(data, data + size);

        Message msg(message);
        handleMessage(msg);

    }

    /*
     * This function should also ensure all READ and UPDATE operation
     * get QUORUM replies
     */
    checkForQuorum();
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector <Node> MP2Node::findNodes(string key) {
    size_t pos = hashFunction(key);
    vector <Node> addr_vec;
    if (ring.size() >= 3) {
        // if pos <= min || pos > max, the leader is the min
        if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size() - 1).getHashCode()) {
            addr_vec.emplace_back(ring.at(0));
            addr_vec.emplace_back(ring.at(1));
            addr_vec.emplace_back(ring.at(2));
        } else {
            // go through the ring until pos <= node
            for (int i = 1; i < ring.size(); i++) {
                Node addr = ring.at(i);
                if (pos <= addr.getHashCode()) {
                    addr_vec.emplace_back(addr);
                    addr_vec.emplace_back(ring.at((i + 1) % ring.size()));
                    addr_vec.emplace_back(ring.at((i + 2) % ring.size()));
                    break;
                }
            }
        }
    }
    return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if (memberNode->bFailed) {
        return false;
    } else {
        return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
    Queue q;
    return q.enqueue((queue <q_elt> *) env, (void *) buff, size);
}

/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
    vector<Node> newHros = getNewHROs();
    vector<Node> newHMRs = getNewHMRs();
    std::for_each(ht->hashTable.begin(), ht->hashTable.end(), [this, newHros, newHMRs](pair<string, string> kv) {
        vector<Node> replicas = findNodes(kv.first);
        if(getMemberNode()->addr == *(replicas[1].getAddress()) || newHros.size() >= 2) {
            std::for_each(replicas.begin(), replicas.end(), [this, newHros, kv](const Node& replica) {
                std::vector<Node>::const_iterator it = std::find_if(newHros.begin(), newHros.end(),
                                                              [replica](const Node& hro) {
                                                                  return hro.nodeHashCode == replica.nodeHashCode;
                                                              });
                if (it != newHros.end())
                    sendData(*it, kv.first);
            });

            return;
        }

        if(getMemberNode()->addr == *(replicas[0].getAddress())) {
            std::for_each(replicas.begin(), replicas.end(), [this, newHMRs, kv](const Node& replica) {
                std::vector<Node>::const_iterator it = std::find_if(newHMRs.begin(), newHMRs.end(),
                                                              [replica](const Node& hmr) {
                                                                  return hmr.nodeHashCode == replica.nodeHashCode;
                                                              });
                if (it != newHMRs.end())
                    sendData(*it, kv.first);
            });
            return;
        }

        /*if(Node(getMemberNode()->addr).nodeHashCode != replicas[2].getHashCode()) {
            ht->deleteKey(kv.first);
            return;
        }*/
    });
}

// coordinator dispatches messages to corresponding nodes
void MP2Node::dispatchMessage (Message message, Address* address) {
    emulNet->ENsend(&(getMemberNode()->addr), address, message.toString());
}

void MP2Node::handleMessage(Message message) {
    switch (message.type) {
        case CREATE:
            if(message.transID == -1) {
                createKeyValue(message.key, message.value, PRIMARY);
                /*if(amOwner(message.key)) {
                    for(Node node: hasMyReplicas) {
                        dispatchMessage(message, node.getAddress());
                    }
                }*/
            } else {
                if(createKeyValue(message.key, message.value, PRIMARY)) {
                    log->logCreateSuccess(&(getMemberNode()->addr), false, message.transID, message.key, message.value);
                    message.type = REPLY;
                    dispatchMessage(message, &(message.fromAddr));
                } else {
                    log->logCreateFail(&(getMemberNode()->addr), false, message.transID, message.key, message.value);
                }
            }
            break;

        case READ:
            {
                string ret = readKey(message.key);
                if(ret != "") {
                    log->logReadSuccess(&(getMemberNode()->addr), false, message.transID, message.key, ret);
                    Message reply(message.transID, getMemberNode()->addr, READREPLY, message.key, ret);
                    dispatchMessage(reply, &(message.fromAddr));
                } else {
                    log->logReadFail(&(getMemberNode()->addr), false, message.transID, message.key);
                }
            }
            break;

        case UPDATE:
            if(updateKeyValue(message.key, message.value, PRIMARY)) {
                log->logUpdateSuccess(&(getMemberNode()->addr), false, message.transID, message.key, message.value);
                Message reply(message.transID, getMemberNode()->addr, REPLY, message.key);
                dispatchMessage(reply, &(message.fromAddr));
            } else {
                log->logUpdateFail(&(getMemberNode()->addr), false, message.transID, message.key, message.value);
            }
            break;
            

        case DELETE:
            if(deletekey(message.key)) {
                log->logDeleteSuccess(&(getMemberNode()->addr), false, message.transID, message.key);
                Message reply(message.transID, getMemberNode()->addr, REPLY, true);
                dispatchMessage(reply, &(message.fromAddr));
            } else {
                log->logDeleteFail(&(getMemberNode()->addr), false, message.transID, message.key);
            }
            break;
            

        case REPLY:
        case READREPLY:
            recordTransactionReply(message);
            break;
    }
}

void MP2Node::recordTransaction (Message message) {
    Transaction transaction;
    transaction.timestamp = par->getcurrtime();
    transaction.request = message.toString();

    transactions.emplace(message.transID, transaction);
}

void MP2Node::recordTransactionReply (Message message) {
    map<int, Transaction>::iterator search;

    search = transactions.find(message.transID);
    if (search != transactions.end())
        search->second.responses.push_back(message.toString());
}

void MP2Node::checkForQuorum() {
    std::for_each(transactions.begin(), transactions.end(), [this](pair<int, Transaction> mPair) {
        if(mPair.second.responses.size() >= QUORUM) {
            logSuccess(mPair.second);
            removeTrnsaction(mPair.first);
        }
        if(par->getcurrtime() > mPair.second.timestamp + RTT) {
            logFailure(mPair.second);
            removeTrnsaction(mPair.first);
        }
    });
}

void MP2Node::logSuccess(Transaction transaction) {
    Message message(transaction.request);
    switch (message.type) {
        case CREATE:
            log->logCreateSuccess(&(getMemberNode()->addr), true, message.transID, message.key, message.value);
            break;

        case UPDATE:
            log->logUpdateSuccess(&(getMemberNode()->addr), true, message.transID, message.key, message.value);
            break;

        case DELETE:
            log->logDeleteSuccess(&(getMemberNode()->addr), true, message.transID, message.key);
            break;

        case READ:
            Message response(transaction.responses[0]);
            log->logReadSuccess(&(getMemberNode()->addr), true, message.transID, message.key, response.value);
            break;
    }
}

void MP2Node::logFailure(Transaction transaction) {
    Message message(transaction.request);
    switch (message.type) {
        case CREATE:
            log->logCreateFail(&(getMemberNode()->addr), true, message.transID, message.key, message.value);
            break;

        case READ:
            log->logReadFail(&(getMemberNode()->addr), true, message.transID, message.key);
            break;

        case UPDATE:
            log->logUpdateFail(&(getMemberNode()->addr), true, message.transID, message.key, message.value);
            break;

        case DELETE:
            log->logDeleteFail(&(getMemberNode()->addr), true, message.transID, message.key);
            break;
    }
}

void MP2Node::removeTrnsaction(int transId) {
    transactions.erase(transId);
}


vector<Node> MP2Node::getNewHROs() {
    Node self(getMemberNode()->addr);
    vector<Node> hros;

    self.computeHashCode();

    std::vector<Node>::const_iterator it = std::find_if(ring.begin(), ring.end(), [this, self](const Node& node){
        return node.nodeHashCode == self.nodeHashCode;
    });

    if(it != ring.end()) {
        hros.push_back(it[-1]);
        hros.push_back(it[-2]);
    }

    vector<Node> newHros;

    std::set_difference(
            hros.begin(), hros.end(),
            haveReplicasOf.begin(), haveReplicasOf.end(),
            std::back_inserter(newHros)
    );

    haveReplicasOf.clear();
    haveReplicasOf = hros;

    return newHros;
}

vector<Node> MP2Node::getNewHMRs() {
    Node self(getMemberNode()->addr);
    vector<Node> hmrs;

    self.computeHashCode();

    std::vector<Node>::const_iterator it = std::find_if(ring.begin(), ring.end(), [this, self](const Node& node){
        return node.nodeHashCode == self.nodeHashCode;
    });

    if(it != ring.end()) {
        hmrs.push_back(it[1]);
        hmrs.push_back(it[2]);
    }
    vector<Node> newHmrs;

    std::set_difference(
            hmrs.begin(), hmrs.end(),
            hasMyReplicas.begin(), hasMyReplicas.end(),
            std::back_inserter(newHmrs)
    );

    hasMyReplicas.clear();
    hasMyReplicas = hmrs;

    return newHmrs;
}

void MP2Node::sendData(Node node, string k) {
    Message message(-1, getMemberNode()->addr, CREATE, k, ht->read(k));
    emulNet->ENsend(&(getMemberNode()->addr), node.getAddress(), message.toString());
}

bool MP2Node::amOwner(string key) {
    vector<Node> nodes = findNodes(key);
    return Node(getMemberNode()->addr).getHashCode() == nodes[0].getHashCode();
}


void MP2Node::clearUnrelevantData() {

}