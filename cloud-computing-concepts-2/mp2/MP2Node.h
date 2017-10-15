/**********************************
 * FILE NAME: MP2Node.h
 *
 * DESCRIPTION: MP2Node class header file
 **********************************/

#ifndef MP2NODE_H_
#define MP2NODE_H_

/**
 * Header files
 */
#include "stdincludes.h"
#include "EmulNet.h"
#include "Node.h"
#include "HashTable.h"
#include "Log.h"
#include "Params.h"
#include "Message.h"
#include "Queue.h"

#include <map>

struct Transaction {
    int timestamp;
    string request;
    vector<string> responses;
};

#define QUORUM 2
#define RTT 5

/**
 * CLASS NAME: MP2Node
 *
 * DESCRIPTION: This class encapsulates all the key-value store functionality
 * 				including:
 * 				1) Ring
 * 				2) Stabilization Protocol
 * 				3) Server side CRUD APIs
 * 				4) Client side CRUD APIs
 */
class MP2Node {
private:
    // Vector holding the next two neighbors in the ring who have my replicas
    vector <Node> hasMyReplicas;
    // Vector holding the previous two neighbors in the ring whose replicas I have
    vector <Node> haveReplicasOf;
    // Ring
    vector <Node> ring;
    // Hash Table
    HashTable *ht;

    // Hash Table to store transactions for quorum.
    map<int, Transaction> transactions;

    // Member representing this member
    Member *memberNode;
    // Params object
    Params *par;
    // Object of EmulNet
    EmulNet *emulNet;
    // Object of Log
    Log *log;

    vector <Node> getMembershipList();

    size_t hashFunction(string key);

    void findNeighbors();

    static int enqueueWrapper(void *env, char *buff, int size);

    // coordinator dispatches messages to corresponding nodes
    void dispatchMessage(Message message, Address* address);

    // server
    bool createKeyValue(string key, string value, ReplicaType replica);

    string readKey(string key);

    bool updateKeyValue(string key, string value, ReplicaType replica);

    bool deletekey(string key);

    // stabilization protocol - handle multiple failures
    void stabilizationProtocol();

    void handleMessage(Message msg);

    void recordTransaction(Message msg);

    void recordTransactionReply(Message msg);

    void checkForQuorum();

    void logSuccess(Transaction);

    void logFailure(Transaction);

    void removeTrnsaction(int);

public:
    MP2Node(Member *memberNode, Params *par, EmulNet *emulNet, Log *log, Address *addressOfMember);

    Member *getMemberNode() {
        return this->memberNode;
    }

    // client side CRUD APIs
    void clientCreate(string key, string value);

    void clientRead(string key);

    void clientUpdate(string key, string value);

    void clientDelete(string key);

    // find the addresses of nodes that are responsible for a key
    vector <Node> findNodes(string key);

    // handle messages from receiving queue
    void checkMessages();

    // receive messages from Emulnet
    bool recvLoop();

    // ring functionalities
    void updateRing();

    ~MP2Node();
};

#endif /* MP2NODE_H_ */
