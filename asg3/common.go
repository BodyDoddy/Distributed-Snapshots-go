package asg3

import (
	"fmt"
	"log"
	"reflect"
	"sort"
)

const debug = false

// The output of the Chandy Lamport algorithm
type GlobalSnapshot struct {
	id       int            // snapshot id
	tokenMap map[string]int // key = node ID, value = num tokens
	messages []*MsgSnapshot // snapshots of all messages recorded across all links
}

// This should be used to store a message recorded in the link src->dest during the snapshot
type MsgSnapshot struct {
	src     string
	dest    string
	message Message
}

// This type represents the content of messages exchanged between nodes during the simulation.
// A wrapped version that contains the sender and the receiver can be found in SendMsgEvent
type Message struct {
	isMarker bool // if true, the message is a marker, if false, the message is a token transfer
	data     int  // if the message is a marker message, data carries the snapshot id, else data carries the number of tokens transferred
}

func (m Message) String() string {
	if m.isMarker {
		return fmt.Sprintf("marker(%v)", m.data)
	} else {
		return fmt.Sprintf("token(%v)", m.data)
	}
}

// An event that represents the sending of a message.
// This is expected to be queued in link.msgQueue
type SendMsgEvent struct {
	src     string
	dest    string
	message Message
	// Note: The message will be received by the node at or after this time step. This value is generated by the simulator at random.
	// See the assignment document for mode detail.
	receiveTime int
}

// ================================================
//  Events injected into the system by the CL Simulator
// ================================================

// An event parsed from the .event files that represent the passing of tokens
// from one node to another
type PassTokenEvent struct {
	src    string
	dest   string
	tokens int
}

// An event parsed from the .event files that represent the initiation of the
// chandy-lamport snapshot algorithm
type SnapshotEvent struct {
	nodeId string
}

//=========================
// Events used by the logger for debugging purposes only
//=========================

// used by the logger for debugging only
type SentMsgRecord struct {
	src     string
	dest    string
	message Message
}

func (m SentMsgRecord) String() string {
	if m.message.isMarker {
		return fmt.Sprintf("%v sent marker(%v) to %v", m.src, m.message.data, m.dest)
	} else {
		return fmt.Sprintf("%v sent %v tokens to %v", m.src, m.message.data, m.dest)
	}
}

// used by the logger for debugging only
type StartSnapshotRecord struct {
	nodeId     string
	snapshotId int
}

func (m StartSnapshotRecord) String() string {
	return fmt.Sprintf("%v startSnapshot(%v)", m.nodeId, m.snapshotId)
}

// used by the logger for debugging only
type EndSnapshotRecord struct {
	nodeId     string
	snapshotId int
}

func (m EndSnapshotRecord) String() string {
	return fmt.Sprintf("%v endSnapshot(%v)", m.nodeId, m.snapshotId)
}

// used by the logger for debugging only
type ReceivedMsgRecord struct {
	src     string
	dest    string
	message Message
}

func (m ReceivedMsgRecord) String() string {
	if m.message.isMarker {
		return fmt.Sprintf("%v received marker(%v) from %v", m.dest, m.message.data, m.src)
	} else {
		return fmt.Sprintf("%v received %v tokens from %v", m.dest, m.message.data, m.src)
	}
}

// ========================
// Helper methods
//=========================

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// Retrieve the keys of a map m in sorted order.
func getSortedKeys(m interface{}) []string {
	v := reflect.ValueOf(m)
	if v.Kind() != reflect.Map {
		log.Fatal("Error: Attempted to get sorted keys of a non-map: ", m)
	}
	keys := make([]string, 0)
	for _, k := range v.MapKeys() {
		keys = append(keys, k.String())
	}
	sort.Strings(keys)
	return keys
}
