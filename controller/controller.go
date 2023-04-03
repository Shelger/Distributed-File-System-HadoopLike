package main

import (
	"P1-SHELGER/messages"
	"crypto/md5"
	"encoding/binary"
	"log"
	"net"
	"os"
	"sort"
	"time"

)


var spaceMap = map[string]uint64{}
var reqHandled = map[string]uint32{}
var checksums = map[string][]byte{}
var filechunks = map[string][]string{}
var hop int = 2

func gcd(a, b int) int {
    if b == 0 {
        return a
    }
    return gcd(b, a%b)
}
// Function to calculate LCM of two integers
func lcm(a, b int) int {
    return (a * b) / gcd(a, b)
}
func modifyHop(ringsize int) {
	if ringsize < 3 {
		hop = 1
		return
	}
	for i:=2; i<ringsize; i++ {
		if lcm(i, ringsize) == i * ringsize {
			hop = i
		}
	}
}

func lostMetadata(node string, ring *Ring) (string, string) {
	delete(spaceMap, node)
	delete(reqHandled, node)
	// if ring remove node here?
	idx := 0
	from := 0
	for i, n := range ring.nodes {
		if n.addr == node {
			idx = (i + 1) % len(ring.nodes)
			from = (i + hop) % len(ring.nodes)
		}
	}
	next := ring.nodes[idx]
	next_hop := ring.nodes[from]
	for _, nodes := range filechunks {
		for i, n := range nodes {
			if n == node {
				nodes = append(nodes[:i], nodes[i+1:]...)
				nodes = append(nodes, next.addr)
			}
		}
	}
	ring.RemoveNode(node)
	return next.addr, next_hop.addr
}

func handleClient(msgHandler *messages.MessageHandler, ring *Ring, start *time.Time, addr *string) {
	defer msgHandler.Close()

	for {
		wrapper, err := msgHandler.Receive()
		if err != nil {
			log.Println(err)
		}
		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_Sn:
			*addr = wrapper.GetSn().GetAddr()
			handleNewNode(msgHandler, wrapper.GetSn().GetAddr(), wrapper.GetSn().GetSpace(), ring)
			continue
		case *messages.Wrapper_StorageReq:
			handleStorage(msgHandler, msg.StorageReq, ring)
			continue
		case *messages.Wrapper_RetrievalReq:
			handleRetrieval(msgHandler, msg.RetrievalReq)
			continue
		case *messages.Wrapper_DeleteReq:
			handleDelete(msgHandler, msg.DeleteReq, ring)
			continue
		case *messages.Wrapper_ListReq:
			handleList(msgHandler)
			continue
		case *messages.Wrapper_InfoReq:
			handleInfo(msgHandler, ring)
			continue
		case *messages.Wrapper_AskCopyReq:
			log.Println("Send copy nodes address back...")
			handleAskCopyReq(msgHandler, msg.AskCopyReq, ring)
			continue
		case *messages.Wrapper_FileChecksumReq:
			log.Println(wrapper.GetFileChecksumReq().GetChecksum())
			// response tell client that file has already been in the system
			fileName := wrapper.GetFileChecksumReq().GetFileName()
			if _, ok := checksums[fileName]; ok { 
				msgHandler.SendResponse(true, fileName)
			} else {
				msgHandler.SendResponse(false, fileName)
				checksums[wrapper.GetFileChecksumReq().GetFileName()] = wrapper.GetFileChecksumReq().GetChecksum()
			}
			continue
		case *messages.Wrapper_AskChecksumReq:
			msgHandler.SendChecksumVerification(checksums[wrapper.GetAskChecksumReq().GetFileName()])
			continue
		case *messages.Wrapper_Hb:
			*start = time.Now()
			handleHeartBeat(msgHandler, msg.Hb, ring)
			continue
		case nil:
			log.Println("Received an empty message, terminating client")
			return
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

func handleNewNode(msgHandler *messages.MessageHandler, addr string, space uint64, ring *Ring) {
	ring.AddNode(addr)
	spaceMap[addr] = space
	modifyHop(len(ring.nodes))
	// send to all other nodes
}

func findNext(addr string, ring *Ring) (string, string) {
	index := 0
	for i, node := range ring.nodes {
		if node.addr == addr {
			index = i
		}
	}
	idx1 := (index + hop) % len(ring.nodes)
	idx2 := (index + 2*hop) % len(ring.nodes)
	return ring.nodes[idx1].addr, ring.nodes[idx2].addr
}

func handleDelete(msgHandler *messages.MessageHandler, request *messages.DeleteRequest, ring *Ring) {
	log.Println("Delete " + request.FileName)
	if _, ok := checksums[request.FileName]; !ok {
		msgHandler.SendResponse(false, "No file.")
		return
	}  else if _, ok := filechunks[request.FileName]; !ok {
		msgHandler.SendResponse(false, "No file.")
		return
	}
	nodes := filechunks[request.FileName]
	temp := make([]string, 0)
	for _, node := range nodes {
		next1, next2 := findNext(node, ring)
		if !contains(nodes, next1) && !contains(temp, next1) {
			temp = append(temp, next1)
		}
		if !contains(nodes, next2) && !contains(temp, next2) {
			temp = append(temp, next2)
		}
	}
	nodes = append(nodes, temp...)
	log.Println(nodes)
	msg := messages.AskNodeResponse{NodeName: nodes}
	w1 := &messages.Wrapper{
		Msg: &messages.Wrapper_AskNode{AskNode: &msg},
	}
	msgHandler.Send(w1)
	delete(checksums, request.FileName)
	delete(filechunks, request.FileName)

}

func handleAskCopyReq(msgHandler *messages.MessageHandler, request *messages.AskCopyNodesRequest, ring *Ring) {
	index := 0
	for i, node := range ring.nodes {
		if node.addr == request.Node1 {
			index = i
		}
	}
	idx1 := (index + hop) % len(ring.nodes)
	idx2 := (index + 2*hop) % len(ring.nodes)
	msgHandler.SendAskCopyNodesResponse(ring.nodes[idx1].addr, ring.nodes[idx2].addr)
}

func contains(slice []string, target string) bool {
    for _, item := range slice {
        if item == target {
            return true
        }
    }
    return false
}
func handleStorage(msgHandler *messages.MessageHandler, request *messages.StorageRequest, ring *Ring) {
	log.Println("PUT", request.FileName)
	node := ring.GetNode(&request.Chunk)
	if _, ok := filechunks[request.FileName]; !ok {
		filechunks[request.FileName] = make([]string, 0)
	}
	if !contains(filechunks[request.FileName], node.addr) {
		filechunks[request.FileName] = append(filechunks[request.FileName], node.addr)
	}
	log.Println(filechunks[request.FileName])
	msg := messages.StorageNode{Addr: node.addr, Id: node.id}
	w1 := &messages.Wrapper{
		Msg: &messages.Wrapper_Sn{Sn: &msg},
	}
	msgHandler.Send(w1)
}

func handleRetrieval(msgHandler *messages.MessageHandler, request *messages.RetrievalRequest) {
	log.Println("GET", request.FileName)
	if _, ok := filechunks[request.FileName]; !ok {
		log.Println("File not exists!")
		// msgHandler.SendResponse(false, "File not exists!")
		return
	}
	nodes := filechunks[request.FileName]
	log.Println(nodes)
	msg := messages.AskNodeResponse{NodeName: nodes}
	w1 := &messages.Wrapper{
		Msg: &messages.Wrapper_AskNode{AskNode: &msg},
	}
	msgHandler.Send(w1)
}

func handleList(msgHandler *messages.MessageHandler) {
	log.Println("List Files stored in the system")
	keys := make([]string, 0, len(filechunks))
	for k := range filechunks {
    	keys = append(keys, k)
	}
	msgHandler.SendListResponse(keys)
}

func handleHeartBeat(msgHandler *messages.MessageHandler, request *messages.Heartbeat, ring *Ring) {
	log.Println(request.NodeName + " Heartbeat Received")

	spaceMap[request.NodeName] = request.Space
	reqHandled[request.NodeName] = request.NumOfHandled

	next1, next2 := findNext(request.NodeName, ring)
	log.Println("the next is"+next1)
	log.Println("the next is"+next2)
	msgHandler.SendAskCopyNodesResponse(next1, next2)
}

func handleInfo(msgHandler *messages.MessageHandler, ring *Ring) {
	log.Println("Get Information")
	nodeNames := make([]string, 0)
	for _, node := range ring.nodes {
		nodeNames = append(nodeNames, node.addr)
	}
	log.Println(spaceMap)
	log.Println(reqHandled)
	var totalSpace uint64 = 0
	for _, sp := range spaceMap {
		totalSpace += sp
	}
	var totalHandled uint32 = 0
	for _, hd := range reqHandled {
		totalHandled += hd
	}
	msgHandler.SendInfoResponse(nodeNames, totalSpace, totalHandled)
}

func hashKey(data []byte) uint32 {
	hash := md5.Sum(data)
	return binary.LittleEndian.Uint32(hash[:])
}

// create node
type Node struct {
	id   uint32
	addr string
}
type Nodes []Node
type Ring struct {
	nodes Nodes
}
func (n Nodes) Len() int           { return len(n) }
func (n Nodes) Swap(i, j int)      { n[i], n[j] = n[j], n[i] }
func (n Nodes) Less(i, j int) bool { return n[i].id < n[j].id }

// struct ring helps record nodes
func NewRing() *Ring {
	return &Ring{}
}
func (r *Ring) AddNode(addr string) {
	hash := md5.Sum([]byte(addr))
	id := binary.LittleEndian.Uint32(hash[:])
	r.nodes = append(r.nodes, Node{id, addr})
	sort.Sort(r.nodes)
}
func (r *Ring) RemoveNode(addr string) {
	for i, node := range r.nodes {
		if node.addr == addr {
			r.nodes = append(r.nodes[:i], r.nodes[i+1:]...)
			break
		}
	}
}
func (r *Ring) GetNode(key *[]byte) Node {
	hash := md5.Sum(*key)
	id := binary.LittleEndian.Uint32(hash[:])

	i := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i].id >= id
	})

	if i >= len(r.nodes) {
		i = 0
	}
	return r.nodes[i]
}

// func checkShutdown() {
// 	for {
// 		findShutdown()
// 		time.Sleep(5 * time.Second)
// 	}
// }

func findShutdown(start *time.Time, addr *string, ring *Ring) {
	for {
		elapsed := time.Since(*start).Seconds()
		if elapsed > 15 {
			log.Println(*addr + " Connection Timeout!!!!!!!!!!!!!!!")
			next, from := lostMetadata(*addr, ring)
			saveNode(from, next)
			modifyHop(len(ring.nodes))
			log.Println(hop)
			return
		}
		// time.Sleep(3 * time.Second)
	}
}
// if node lost, pass its next node
func saveNode(addr string, next string) {
	log.Println("Copy files for fault tolerance!")
	conn, err := net.Dial("tcp", addr + ":29333")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	msgHandler := messages.NewMessageHandler(conn)
	defer conn.Close()
	msgHandler.SendSaveRequest(next)
}

func main() {
	listener, err := net.Listen("tcp", "orion01.cs.usfca.edu:29000")
	if err != nil {
		log.Fatalln(err.Error())
		os.Exit(1)
	}
	defer listener.Close()
	// create a list of storage nodes active
	ring := NewRing()
	// nodes := make(map[int]string)
	// go checkShutdown()
	for {
		start := time.Now()
		address := ""
		log.Println("Listening...")
		if conn, err := listener.Accept(); err == nil {
			log.Println("Accepted connection", conn.RemoteAddr())
			handler := messages.NewMessageHandler(conn)
			// handle new added node
			go handleClient(handler, ring, &start, &address)
			// Sleep for a while so that addr can be assigned
			time.Sleep(time.Second)
		}
		if address!= "" {
			go findShutdown(&start, &address, ring)
		}
	}
}
