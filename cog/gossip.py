"""
Gossip protocol for node-node communication

- Periodically sent gossip message to n nodes
- Each node starts with a set of peer nodes (sub set of all possible nodes).
- Each time a gossip message is received, both nodes merge their list of peer nodes,
this way nodes are discovered.

In Cog network, gossip will let each node know which nodes are alive and
which node index partitions are reside.

Any node can be contacted by the client and issued a query or write request. The gossip keeps
peer node information updated on each node. Each node would therefore know where to
route read/write requests


- Push based vs Pull based vs Push + Pull based
Push based is slower at spreading information since push is initiated only at infected nodes (the nodes with the updates)
which mean many suseptible nodes(the nodes that does not have the updates just yet) will have to wait a long time before
it gets selected by an infected node.

Pull spreads information much faster, since pulls are intiated by nodes that do not have the data just yet.

Push-pull remains the the best case. In this code we will implement pull based.

Steen, M. V., & Tanenbaum, A. S. (2017). Distributed systems. Upper Saddle River, NJ: Prentice-Hall.


"""

import socket
import random
from threading import Thread
import sys
from time import sleep
import json

class Node:

    def __init__(self, port, seed_peer, state, gossip_size=1):
        """
        ID is part of state.
        :param port:
        :param seed_peers:
        :param state:
        :return:
        """
        print port
        print seed_peer
        print state

        self.port = int(port)
        self.peers = [int(port),int(seed_peer)]
        self.state = state
        self.gossip_size = gossip_size
        Thread(target=self.gossip_receiver).start()

        while True:
            self.send_gossip()
            sleep(2)


    def send_gossip(self):
        """
        sends state information to peer to peers
        :return:
        """
        print "sending..."
        for peer in random.sample(self.peers, self.gossip_size):
            if peer is not int(self.port):
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                gossip = {"state":self.state, "peers": self.peers}
                print "send gossip ->"+ str(peer) + json.dumps(gossip)
                sock.sendto(json.dumps(gossip), ("127.0.0.1", peer))

    def gossip_receiver(self):
        """
        Server - waits for gossip from peers
        Updates state.
        :return:
        """
        print "Listening for gossip"
        sock = socket.socket(socket.AF_INET,  # Internet
                             socket.SOCK_DGRAM)  # UDP
        sock.bind(("127.0.0.1", self.port))

        prev_size = 0

        while True:
            gossip, addr = sock.recvfrom(1024)
            gossip = json.loads(gossip)
            #print gossip
            #self.peers.update(gossip.peers)
            for peer in gossip['peers']:
                if peer not in self.peers:
                    self.peers.append(peer)
            if len(gossip['peers']) > prev_size:
                print "<- update received"
                print gossip['peers']

            prev_size = len(self.peers)


node = Node(sys.argv[1], sys.argv[2], sys.argv[3])

#python gossip.py 5003 5001 "state"