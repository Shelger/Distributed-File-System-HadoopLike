package main

import (
	"P1-SHELGER/messages"
	// "P1-SHELGER/util"
	"bufio"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sort"
	"io/ioutil"
	"encoding/hex"
	"P1-SHELGER/util"
	"sync"
)

// /bigdata/mmalensek/project1
func put(msgHandler *messages.MessageHandler, filePath string, chunk_num int) int {
	fmt.Println("PUT", filePath)

	// Get file size and make sure it exists
	info, err := os.Stat(filePath)
	if err != nil {
		log.Fatalln(err)
	}

	// Tell the server we want to store this file
	// msgHandler.SendStorageRequest(fileName, uint64(info.Size()))
	// if ok, _ := msgHandler.ReceiveResponse(); !ok {
	// 	return 1
	// }
	chunk_size := info.Size() / int64(chunk_num)
	file, _ := os.Open(filePath)
	md5 := md5.New()
	content, _ := ioutil.ReadFile(filePath)
	md5.Write(content)
	tokens := strings.Split(filePath, "/")
	fileName := tokens[len(tokens)-1]
	cs := md5.Sum(nil)
	// log.Printf("%x\n", cs)
	msgHandler.SendFileChecksumRequest(fileName, cs)
	existed, _ := msgHandler.ReceiveResponse()
	if existed {
		log.Println("File has already Existed! Exit now.")
		return 1
	}
	md5.Reset()

	idx := 0
	chunkReader := bufio.NewReader(file)
	// jumpout := false
	// output := false
	// StoreChunks := func() {
		
	// }
	for {
		chunk := make([]byte, chunk_size)
		if idx == (chunk_num-1) {
			chunk = make([]byte, info.Size()-int64(idx)*chunk_size)
		}
		bytesRead, err := chunkReader.Read(chunk)
		// log.Println(chunk)
		if err != nil && err != io.EOF {
			panic(err)
		}
		if bytesRead == 0 {
			break
		}
		msgHandler.SendStorageRequest(&chunk, uint64(len(chunk)), fileName, int32(idx), nil)
		wrapper_controller, err := msgHandler.Receive()
		if err != nil {
			log.Println(err)
			return 1
		}
		node := wrapper_controller.GetSn()
		// log.Println(node.Addr)


		conn, err := net.Dial("tcp", node.Addr + ":29333")
		if err != nil {
			log.Fatalln(err.Error())
			return 1
		}
		handler := messages.NewMessageHandler(conn)
		defer conn.Close()

		md5.Write(chunk)
		checksum := md5.Sum(nil)
		handler.SendStorageRequest(&chunk, uint64(len(chunk)), fileName, int32(idx), checksum)

		handler.SendChecksumVerification(checksum)
		md5.Reset()
		idx++
	}
	file.Close()
	fmt.Println("Storage complete!")
	return 0
}

// wait for checksum
func contains(slice []string, target string) bool {
    for _, item := range slice {
        if item == target {
            return true
        }
    }
    return false
}
func get(msgHandler *messages.MessageHandler, fileName string) int {
	fmt.Println("GET", fileName)

	file, err := os.OpenFile("new"+fileName, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Println(err)
		return 1
	}
	msgHandler.SendRetrievalRequest(fileName)
	// ok, _, size := msgHandler.ReceiveRetrievalResponse()
	// if !ok {
	// 	return 1
	// }
	wrapper_controller, err := msgHandler.Receive()
	if err != nil {
		log.Println(err)
		return 1
	}
	nodes := wrapper_controller.GetAskNode().GetNodeName()
	// log.Println(nodes)

	type ChunkInfo struct {
		Chunk []byte
		Idx int
	}
	chunks := make([]ChunkInfo, 0)
	visited := make([]string, 0)
	RetrieveNodes := func(node string, wg *sync.WaitGroup) {
		defer wg.Done()
		conn, err := net.Dial("tcp", node + ":29333")
		if err != nil {
			log.Fatalln(err.Error())
			return
		}
		handler := messages.NewMessageHandler(conn)
		defer conn.Close()

		handler.SendRetrievalRequest(fileName)
		md5 := md5.New()
		for {
			wrapper, _ := handler.Receive()
			if wrapper.GetResponse() != nil {
				break
			}
			temp_filename := wrapper.GetRetrievalResp().GetFileName()
			// log.Println(temp_filename)
			tokens := strings.Split(temp_filename, "_")
			// log.Println(node)
			// if tokens[len(tokens)-1] == "9" {
			// 	log.Println(temp_filename)
			// }
			if contains(visited, tokens[len(tokens)-1]) {
				continue
			}
			visited = append(visited, tokens[len(tokens)-1])
			idx, _ := strconv.Atoi(tokens[len(tokens)-1])
			hashBytes, _ := hex.DecodeString(tokens[len(tokens)-2])
			md5.Write(wrapper.GetRetrievalResp().GetChunk())
			// log.Println(idx)

			// log.Println(tokens[len(tokens)-1])
			if !util.VerifyChecksum(md5.Sum(nil), hashBytes) {
				log.Println("FAILED to store chunk. Invalid checksum.")
			}
			md5.Reset()
			temp_chunk := ChunkInfo{Chunk: wrapper.GetRetrievalResp().GetChunk(), Idx: idx}
			chunks = append(chunks, temp_chunk)
		}
	}

	var wg sync.WaitGroup
	for _, node := range nodes {
		wg.Add(1)
		go RetrieveNodes(node, &wg)
	}
	wg.Wait()

	// reorder the chunks
	sort.Slice(chunks, func(i, j int) bool {
        return chunks[i].Idx < chunks[j].Idx
    })
	// for _, val := range chunks {
	// 	log.Println(val.Chunk);
	// }
	// log.Println(chunks)
	content := make([]byte, 0)
	for _, chunk := range chunks {
		content = append(content, chunk.Chunk...)
	}

	msgHandler.SendAskChecksumRequest(fileName)
	wrapper_controller, _ = msgHandler.Receive()
	checksum := wrapper_controller.GetChecksum().GetChecksum()
	md5 := md5.New()
	md5.Write(content)
	// log.Println(checksum)
	// log.Println(md5.Sum(nil))
	// file.Write(content)
	if util.VerifyChecksum(md5.Sum(nil), checksum) {
		log.Println("Successfully stored file.")
	} else {
		log.Println("FAILED to store file. Invalid checksum.")
		file.Close()
		return 1
	}
	file.Write(content)
	file.Close()
	return 0
}

func delete(msgHandler *messages.MessageHandler, fileName string) int {
	fmt.Println("DELETE", fileName)
	msgHandler.SendDeleteRequest(fileName)
	wrapper_controller, err := msgHandler.Receive()
	if err != nil {
		log.Println(err)
		return 1
	}
	switch msg := wrapper_controller.Msg.(type) {
		case *messages.Wrapper_Response:
			log.Println("No file!")
			return 1
		case *messages.Wrapper_AskNode:
			nodes := wrapper_controller.GetAskNode().GetNodeName()
			for _, node := range nodes {
				conn, err := net.Dial("tcp", node + ":29333")
				if err != nil {
					log.Fatalln(err.Error())
					return 1
				}
				handler := messages.NewMessageHandler(conn)
				defer conn.Close()
				handler.SendDeleteRequest(fileName)
			}
			return 0
		case nil:
			log.Println("Received an empty message, terminating client")
			return 1
		default:
			log.Printf("Unexpected message type: %T", msg)
			return 1
		}
}

func list(msgHandler *messages.MessageHandler) int {
	fmt.Println("List Files")
	msgHandler.SendListRequest()
	wrapper, err := msgHandler.Receive()
	if err != nil {
		log.Println(err)
		return 1
	}
	list := wrapper.GetListResp().GetFileName()
	for _, file := range list {
		fmt.Println(file)
	}
	return 0
}

func info(msgHandler *messages.MessageHandler) int {
	fmt.Println("Get Information")
	msgHandler.SendInfoRequest()
	nodes, space, handled := msgHandler.ReceiveInfoResponse()

	fmt.Println("Active Nodes: ")
	for _, node := range nodes {
		fmt.Println(node)
	}
	fmt.Println("Space(MB): " + strconv.FormatUint(space, 10))
	fmt.Println("Number of handled requests: " + strconv.FormatUint(uint64(handled), 10))
	return 0
}

func main() {
	fileName := ""
	action := ""
	chunk_num := 3
	if len(os.Args) > 1 {
		action = strings.ToLower(os.Args[1])
		if len(os.Args) > 2 {
			fileName = os.Args[2]
		}
		if len(os.Args) > 3 {chunk_num, _ = strconv.Atoi(os.Args[3])}
	} else {
		reader := bufio.NewReader(os.Stdin)
		fmt.Println("Enter Action(put/get/delete/ls/info): ")
		action, _ = reader.ReadString('\n')
		action = strings.TrimRight(action, "\r\n")
		if action == "put" || action == "get" || action == "delete"{
			fmt.Println("Enter file name: ")
			fileName, _ = reader.ReadString('\n')
			fileName = strings.TrimRight(fileName, "\r\n")
			if action == "put" {
				fmt.Println("Enter the chunk number(3 is default): ")
				input_num, _ := reader.ReadString('\n')
				input_num = strings.TrimRight(input_num, "\r\n")
				if input_num != "" {
					chunk_num, _ = strconv.Atoi(input_num)
				}
			}
		}
	}
	if action != "put" && action != "get" && action != "delete" && action != "ls" && action != "info" {
		log.Fatalln("Invalid action", action)
	}

	conn, err := net.Dial("tcp", "orion01:29000")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	msgHandler := messages.NewMessageHandler(conn)
	defer conn.Close()

	if len(os.Args) > 3 {
		chunk_num, err = strconv.Atoi(os.Args[3])
	}
	if err != nil {
		panic(err)
	}

	if action == "put" {
		os.Exit(put(msgHandler, fileName, chunk_num))
	} else if action == "get" {
		os.Exit(get(msgHandler, fileName))
	} else if action == "delete" {
		os.Exit(delete(msgHandler, fileName))
	} else if action == "ls" {
		os.Exit(list(msgHandler))
	} else {
		os.Exit(info(msgHandler))
	}
} 
