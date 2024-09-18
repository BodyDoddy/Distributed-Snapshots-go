package asg3

import (
	"log"
)

// The main participant of the distributed snapshot protocol.
// nodes exchange token messages and marker messages among each other.
// Token messages represent the transfer of tokens from one node to another.
// Marker messages represent the progress of the snapshot process. The bulk of
// the distributed protocol is implemented in `HandlePacket` and `StartSnapshot`.
type Snapshot struct {
	id       int
	tokens   int // key = node ID, value = num tokens
	messages []*MsgSnapshot
}
type Node struct {
	sim           *ChandyLamportSim
	id            string
	tokens        int
	outboundLinks map[string]*Link // key = link.dest
	inboundLinks  map[string]*Link // key = link.src

	// TODO: add more fields here (what does each node need to keep track of?)
	ssstate   *SyncMap                //map[int]*Snapshot
	Recording map[int]map[string]bool // key = src
	completed map[int]map[string]bool
}

// A unidirectional communication channel between two nodes
// Each link contains an event queue (as opposed to a packet queue)
type Link struct {
	src      string
	dest     string
	msgQueue *Queue
}

func CreateNode(id string, tokens int, sim *ChandyLamportSim) *Node {
	return &Node{
		sim:           sim,
		id:            id,
		tokens:        tokens,
		outboundLinks: make(map[string]*Link),
		inboundLinks:  make(map[string]*Link),
		// TODO: You may need to modify this if you make modifications above
		ssstate:   NewSyncMap(),                  //map[int]*Snapshot
		Recording: make(map[int]map[string]bool), // key = src
		completed: make(map[int]map[string]bool),
	}
}

// Add a unidirectional link to the destination node
func (node *Node) AddOutboundLink(dest *Node) {
	if node == dest {
		return
	}
	l := Link{node.id, dest.id, NewQueue()}
	node.outboundLinks[dest.id] = &l
	dest.inboundLinks[node.id] = &l
}

// Send a message on all of the node's outbound links
func (node *Node) SendToNeighbors(message Message) {
	for _, nodeId := range getSortedKeys(node.outboundLinks) {
		link := node.outboundLinks[nodeId]
		node.sim.logger.RecordEvent(
			node,
			SentMsgRecord{node.id, link.dest, message})
		link.msgQueue.Push(SendMsgEvent{
			node.id,
			link.dest,
			message,
			node.sim.GetReceiveTime()})
	}
}

// Send a number of tokens to a neighbor attached to this node
func (node *Node) SendTokens(numTokens int, dest string) {
	if node.tokens < numTokens {
		log.Fatalf("node %v attempted to send %v tokens when it only has %v\n",
			node.id, numTokens, node.tokens)
	}
	message := Message{isMarker: false, data: numTokens}
	node.sim.logger.RecordEvent(node, SentMsgRecord{node.id, dest, message})
	// Update local state before sending the tokens
	node.tokens -= numTokens
	link, ok := node.outboundLinks[dest]
	if !ok {
		log.Fatalf("Unknown dest ID %v from node %v\n", dest, node.id)
	}

	link.msgQueue.Push(SendMsgEvent{
		node.id,
		dest,
		message,
		node.sim.GetReceiveTime()})
}

func (node *Node) HandlePacket(src string, message Message) {

	if message.isMarker {
		if _, found := node.ssstate.LoadOrStore(message.data, &Snapshot{message.data, node.tokens, make([]*MsgSnapshot, 0)}); !found {
			node.SendToNeighbors(message)
			node.completed[message.data] = map[string]bool{}
			node.Recording[message.data] = map[string]bool{}
			for _, source := range getSortedKeys(node.inboundLinks) {
				if src != source {
					node.Recording[message.data][source] = true
				} else {
					node.Recording[message.data][source] = false
				}
			}
		} else {
			node.Recording[message.data][src] = false
		}

		node.completed[message.data][src] = true

		if len(node.completed[message.data]) == len(node.inboundLinks) {
			node.sim.NotifyCompletedSnapshot(node.id, message.data)
		}
	}
	if !message.isMarker {
		node.tokens += message.data

		node.ssstate.Range(func(k, v interface{}) bool {
			if snapshotId, isInt := k.(int); isInt {
				if Recording, found := node.Recording[snapshotId][src]; found && Recording {
					if snapshot, ok := v.(*Snapshot); ok {
						snapshot.messages = append(snapshot.messages, &MsgSnapshot{src, node.id, message})
					}
				}
			}

			return true
		})
	}
}

func (node *Node) StartSnapshot(snapshotId int) {

	// ToDo: Write this method
	node.ssstate.LoadOrStore(snapshotId, &Snapshot{snapshotId, node.tokens, make([]*MsgSnapshot, 0)})
	node.Recording[snapshotId] = map[string]bool{}
	node.completed[snapshotId] = map[string]bool{}
	for _, src := range getSortedKeys(node.inboundLinks) {
		node.Recording[snapshotId][src] = true
	}
	node.SendToNeighbors(Message{isMarker: true, data: snapshotId})
}
