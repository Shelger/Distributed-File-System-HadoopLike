package messages

import (
	"encoding/binary"
	"log"
	"net"
	"google.golang.org/protobuf/proto"
)

type MessageHandler struct {
	conn net.Conn
}

func NewMessageHandler(conn net.Conn) *MessageHandler {
	m := &MessageHandler{
		conn: conn,
	}

	return m
}

func (m *MessageHandler) ReadN(buf []byte) error {
	bytesRead := uint64(0)
	for bytesRead < uint64(len(buf)) {
		n, err := m.conn.Read(buf[bytesRead:])
		if err != nil {
			return err
		}
		bytesRead += uint64(n)
	}
	return nil
}

func (m *MessageHandler) Read(p []byte) (n int, err error) {
	return m.conn.Read(p)
}

func (m *MessageHandler) Write(p []byte) (n int, err error) {
	return m.conn.Write(p)
}

func (m *MessageHandler) WriteN(buf []byte) error {
	bytesWritten := uint64(0)
	for bytesWritten < uint64(len(buf)) {
		n, err := m.conn.Write(buf[bytesWritten:])
		if err != nil {
			return err
		}
		bytesWritten += uint64(n)
	}
	return nil
}

func (m *MessageHandler) Send(wrapper *Wrapper) error {
	serialized, err := proto.Marshal(wrapper)
	if err != nil {
		return err
	}

	prefix := make([]byte, 8)
	binary.LittleEndian.PutUint64(prefix, uint64(len(serialized)))
	m.WriteN(prefix)
	m.WriteN(serialized)

	return nil
}

func (m *MessageHandler) Receive() (*Wrapper, error) {
	prefix := make([]byte, 8)
	m.ReadN(prefix)

	payloadSize := binary.LittleEndian.Uint64(prefix)
	payload := make([]byte, payloadSize)
	m.ReadN(payload)

	wrapper := &Wrapper{}
	err := proto.Unmarshal(payload, wrapper)
	return wrapper, err
}

func (m *MessageHandler) Close() {
	m.conn.Close()
}

// use this as the request send to the controller asking for storage nodes
func (m *MessageHandler) SendStorageRequest(chunk *[]byte, size uint64, fileName string, idx int32, checksum []byte) error {
	msg := StorageRequest{Chunk: *chunk, Size: size, FileName: fileName, Idx: idx, Checksum: checksum}
	wrapper := &Wrapper{
		Msg: &Wrapper_StorageReq{StorageReq: &msg},
	}
	return m.Send(wrapper)
}

// this will be response sent by controller
// info about which nodes to store chunks
func (m *MessageHandler) AskStorageNodes() {
	// msg := StorageNode{node: }
}

func (m *MessageHandler) SendRetrievalRequest(fileName string) error {
	msg := RetrievalRequest{FileName: fileName}
	wrapper := &Wrapper{
		Msg: &Wrapper_RetrievalReq{RetrievalReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendChecksumVerification(checksum []byte) error {
	checkMsg := ChecksumVerification{Checksum: checksum}
	checkWrapper := &Wrapper{
		Msg: &Wrapper_Checksum{Checksum: &checkMsg},
	}
	return m.Send(checkWrapper)
}

func (m *MessageHandler) SendResponse(ok bool, str string) error {
	msg := Response{Ok: ok, Message: str}
	wrapper := &Wrapper{
		Msg: &Wrapper_Response{Response: &msg},
	}

	return m.Send(wrapper)
}

func (m *MessageHandler) SendRetrievalResponse(ok bool, str string, size uint64, chunk []byte, fileName string) error {
	resp := Response{Ok: ok, Message: str}
	msg := RetrievalResponse{Resp: &resp, Size: size, Chunk: chunk, FileName: fileName}
	wrapper := &Wrapper{
		Msg: &Wrapper_RetrievalResp{RetrievalResp: &msg},
	}

	return m.Send(wrapper)
}

func (m *MessageHandler) ReceiveResponse() (bool, string) {
	resp, err := m.Receive()
	if err != nil {
		return false, ""
	}
	return resp.GetResponse().Ok, resp.GetResponse().Message
}

func (m *MessageHandler) ReceiveRetrievalResponse() (bool, string, uint64, []byte) {
	resp, err := m.Receive()
	if err != nil {
		return false, "", 0, nil
	}

	rr := resp.GetRetrievalResp().GetResp()
	log.Println(rr.Message)
	return rr.Ok, rr.Message, resp.GetRetrievalResp().Size, resp.GetRetrievalResp().Chunk
}

func (m *MessageHandler) SendDeleteRequest(fileName string) error {
	msg := DeleteRequest{FileName: fileName}
	wrapper := &Wrapper{
		Msg: &Wrapper_DeleteReq{DeleteReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendListRequest() error {
	msg := ListRequest{Ls: true}
	wrapper := &Wrapper{
		Msg: &Wrapper_ListReq{ListReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendListResponse(keys []string) error {
	msg := ListResponse{FileName: keys}
	wrapper := &Wrapper{
		Msg: &Wrapper_ListResp{ListResp: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendHeartBeat(alive bool, space uint64, num_of_handled uint32, hostname string) error {
	msg := Heartbeat{Alive: true, Space: space, NumOfHandled: num_of_handled, NodeName: hostname}
	wrapper := &Wrapper{
		Msg: &Wrapper_Hb{Hb: &msg},
	}
	return m.Send(wrapper)
}

// func (m *MessageHandler) ReceiveHearBeat() (uint64, uint32, string){
// 	resp, err := m.Receive()
// 	if err != nil {
// 		return 0, 0, ""
// 	}
// 	return resp.GetHb().GetSpace(), resp.GetHb().GetNumOfHandled(), resp.GetHb().GetNodeName()
// }

func (m *MessageHandler) SendInfoRequest() error {
	msg := InfoRequest{Info: true}
	wrapper := &Wrapper{
		Msg: &Wrapper_InfoReq{InfoReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendInfoResponse(nodes []string, space uint64, handled uint32) error {
	msg := InfoResponse{ActiveNode: nodes, Space: space, NumOfHandled: handled}
	wrapper := &Wrapper{
		Msg: &Wrapper_InfoResp{InfoResp: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) ReceiveInfoResponse() ([]string, uint64, uint32){
	resp, err := m.Receive()
	if err != nil {
		return nil, 0, 0
	}
	return resp.GetInfoResp().GetActiveNode(), resp.GetInfoResp().GetSpace(), resp.GetInfoResp().GetNumOfHandled()
}

func (m *MessageHandler) SendFileChecksumRequest(filename string, checksum []byte) error {
	msg := FileChecksumRequest{FileName: filename, Checksum: checksum}
	wrapper := &Wrapper{
		Msg: &Wrapper_FileChecksumReq{FileChecksumReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendAskChecksumRequest(filename string) error {
	msg := AskChecksumRequest{FileName: filename}
	wrapper := &Wrapper{
		Msg: &Wrapper_AskChecksumReq{AskChecksumReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendAskCopyNodesRequest(node string) error {
	msg := AskCopyNodesRequest{Node1: node}
	wrapper := &Wrapper{
		Msg: &Wrapper_AskCopyReq{AskCopyReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendAskCopyNodesResponse(node1 string, node2 string) error {
	msg := AskCopyNodesRequest{Node1: node1, Node2: node2}
	wrapper := &Wrapper{
		Msg: &Wrapper_AskCopyReq{AskCopyReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) ReceiveAskCopyNodesResponse() (string, string){
	resp, err := m.Receive()
	if err != nil {
		return "", ""
	}
	return resp.GetAskCopyReq().GetNode1(), resp.GetAskCopyReq().GetNode2()
}

func (m *MessageHandler) SendInternalTransferRequest(fileName string, chunk []byte) error {
	msg := InternalFileTransferRequest{Path: fileName, Chunk: chunk}
	wrapper := &Wrapper{
		Msg: &Wrapper_InternalTransferReq{InternalTransferReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) ReceiveInternalTransferResponse() (string, []byte){
	resp, err := m.Receive()
	if err != nil {
		return "", nil
	}
	return resp.GetInternalTransferReq().GetPath(), resp.GetInternalTransferReq().GetChunk()
}

func (m *MessageHandler) SendSaveRequest(node string) error {
	msg := SaveRequest{NextNode: node}
	wrapper := &Wrapper{
		Msg: &Wrapper_SaveReq{SaveReq: &msg},
	}
	return m.Send(wrapper)
}

func (m *MessageHandler) SendWakeUpRequest(hostname string, free uint64) error {
	wake := StorageNode{Addr: hostname, Space: free}
	w1 := &Wrapper{
		Msg: &Wrapper_Sn{Sn: &wake},
	}
	return m.Send(w1)
}

func (m *MessageHandler) SendFileSaveRequest(filename string) error {
	msg := AskSaveFileRequest{FileName: filename}
	wrapper := &Wrapper{
		Msg: &Wrapper_AskSaveFileReq{AskSaveFileReq: &msg},
	}
	return m.Send(wrapper)
}

