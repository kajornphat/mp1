/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <sstream>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
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
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
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
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
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
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
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
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

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

void MP1Node::addSelfToGroup(Address *grp_coord_addr, long grp_coord_hearbeat) {
    this->memberNode->inGroup = true;
    this->memberNode->heartbeat += 1;
    
    this->addMemberListEntry(*(int*)(&this->memberNode->addr.addr),
                                    *(short*)(&this->memberNode->addr.addr[4]),
                                    this->memberNode->heartbeat);

    if (!(*grp_coord_addr == this->memberNode->addr)) {
        this->memberNode->nnb += 1;
        this->addMemberListEntry(*(int*)(grp_coord_addr->addr),
                                *(short*)(&this->memberNode->addr.addr[4]),
                                grp_coord_hearbeat);
    }
}
/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        this->memberNode->heartbeat += 1;
        
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
        
        

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif


        cout << "Send JOINREQ message to introducer member\n";
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        // printf("%s", msg);
        
        this->memberNode->heartbeat += 1;

        free(msg);

        // cout << "Add member after send JOINREQ message to introducer\n";
        // cout << memberNode->memberList.front().getid();
        // memberNode->memberList.push_back(
        //     MemberListEntry()
        // )
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){

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
    if( !memberNode->inGroup ) {
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
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);

    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	
    MessageHdr *recv_msg, *send_msg;
    Address recv_msg_sendr;

    int send_msgsize;
    int this_id = *(int*)(&this->memberNode->addr.addr);
    short this_port = *(short*)(&this->memberNode->addr.addr[4]);

    /* 
    Increment heartbeat when msg is received or sent 
    Send JOINREP to a JOIN message
    */

    this->memberNode->heartbeat += 1;

    recv_msg = (MessageHdr *) malloc(size * sizeof(char));
    memcpy((char *)(recv_msg), data, size);
    size_t recv_msg_pos = 0 + sizeof(MsgTypes);
    memcpy(&recv_msg_sendr, (char *)(recv_msg) + recv_msg_pos, sizeof(Address));
    recv_msg_pos += sizeof(Address);

    switch(recv_msg->msgType) {
        case(JOINREQ):
            if(this->memberNode->inited &&
                !this->memberNode->bFailed) {
                    
                    cout << this_id << " have Receive JOINREQ message\n";
                    //Increment heartbeat when sent 
                    this->memberNode->heartbeat += 1;

                    send_msgsize = sizeof(MessageHdr) + sizeof(Address) + sizeof(long);
                    send_msg = (MessageHdr *) malloc(sizeof(send_msgsize));
                    send_msg->msgType = JOINREP;
                    memcpy((char *)(send_msg)+sizeof(MsgTypes), &this->memberNode->addr.addr, sizeof(Address));
                    memcpy((char *)(send_msg)+sizeof(MsgTypes)+sizeof(Address),
                        &this->memberNode->heartbeat,
                        sizeof(long));
                    
                    this->emulNet->ENsend(&this->memberNode->addr, &recv_msg_sendr, (char *)send_msg, send_msgsize);

                    // Write Join
                    this->addMemberListEntry(*(int*)(&recv_msg_sendr.addr),
                                            *(short*)(&recv_msg_sendr.addr[4]),
                                            *(long *)((char *)recv_msg + sizeof(MessageHdr) +
                                                sizeof(recv_msg_sendr.addr)));

                    free(send_msg);
                }
                //break JOINREQ case
                break;

        case(JOINREP):

            cout << this_id << " have Receive JOINREP mssage\n";
            long recv_msg_sendr_heartbeat;
            memcpy(&recv_msg_sendr_heartbeat,
                    (char *)recv_msg + sizeof(MessageHdr) + sizeof(recv_msg_sendr.addr),
                    sizeof(long));
            this->addSelfToGroup(&recv_msg_sendr, recv_msg_sendr_heartbeat);
            break;

        case(MMBRTBL):

            cout << this_id << " Receive message to UPDATE table";
            this->memberNode->memberList[0].heartbeat = this->memberNode->heartbeat;

            size_t recv_msg_members_n;
            memccpy(&recv_msg_members_n,
                        (char *)(recv_msg)+recv_msg_pos,
                            sizeof(size_t));
            recv_msg_pos += sizeof(size_t);

            for (size_t recv_msg_members_pos = 0; recv_msg_members_pos < recv_msg_members_n;
                recv_msg_members_pos++) {

                    int recv_msg_member_id;
                    short recv_msg_member_port;
                    long recv_msg_member_heartbeat;

                    memcpy(&recv_msg_member_id, (char *)(recv_msg)+recv_msg_pos,
                            sizeof(recv_msg_member_id));
                    recv_msg_pos += sizeof(recv_msg_member_id);

                    memcpy(&recv_msg_member_port, (char *)(recv_msg)+recv_msg_pos,
                            sizeof(recv_msg_member_port));
                    recv_msg_pos += sizeof(recv_msg_member_port);

                    memcpy(&recv_msg_member_heartbeat, (char *)(recv_msg)+recv_msg_pos,
                            sizeof(recv_msg_member_heartbeat));
                    recv_msg_pos += sizeof(recv_msg_member_heartbeat);

                    //Condition
                    if ((recv_msg_member_id == this_id) && (recv_msg_member_port == this_port))
                        continue;

                    bool recv_msg_member_fnd = false;
                    for (this->memberNode->myPos = this->memberNode->memberList.begin();
                        this->memberNode->myPos != this->memberNode->memberList.end();
                        this->memberNode->myPos++){

                            if ((recv_msg_member_id == this->memberNode->myPos->id) &&
                                (recv_msg_member_port == this->memberNode->myPos->port)) {

                                    recv_msg_member_fnd = true;

                                    if (recv_msg_member_heartbeat <= this->memberNode->myPos->heartbeat)
                                        break;
                                    else {
                                        this->memberNode->myPos->heartbeat = recv_msg_member_heartbeat;
                                        this->memberNode->myPos->timestamp = this->par->getcurrtime();
                                    }
                                }
                        }
                }

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
    // // introduceSelfToGroup()
    // if(par->getcurrtime() < 10)
    // {
    //     cout << memberNode->addr.getAddress() << "\tat time :" << par->getcurrtime() << "\tsize :" << memberNode->memberList.size() << "\n";
    //      cout << "\t member list ";
    //      for(std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it)
    //      {
    //          cout << (*(it)).getid() << " : " ;
    //      }
    //      cout << '\n';
    // }
    
}

void MP1Node::addMemberListEntry(int id, short port, long heartbeat){
    #ifdef DEBUGLOG
        Address newNode;
        memcpy(&newNode.addr[0], &id, sizeof(int));
        memcpy(&newNode.addr[4], &port, sizeof(short));        
        this->log->logNodeAdd(&this->memberNode->addr, &newNode);
    #endif

    MemberListEntry *newMemberListEntry = new MemberListEntry(id, port, heartbeat,
                                            (long)this->par->getcurrtime());
    this->memberNode->memberList.emplace_back(*newMemberListEntry);    
    delete(newMemberListEntry);    
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
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
    memberNode->memberList.clear();

}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

void MP1Node::addMember(){

}