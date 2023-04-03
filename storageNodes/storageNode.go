package main

import (
	"P1-SHELGER/messages"
	"P1-SHELGER/util"
	"crypto/md5"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
	"strconv"
	"path/filepath"
	"io/ioutil"
	"syscall"
	"strings"
	"encoding/hex"
)

var disk string = "/bigdata/students/yzhang433/"
var path string = "/bigdata/students/yzhang433/Project1/"
var num_of_handled uint32 = 0
var hostname string = ""
var nextNode1 string = ""
var nextNode2 string = ""

func handleStorage(msgHandler *messages.MessageHandler, request *messages.StorageRequest) {
	log.Println("Attempting to store", request.FileName)

	filepath := path + request.FileName + "_" + fmt.Sprintf("%x", request.Checksum) + "_" + strconv.Itoa(int(request.Idx))
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	if err != nil {
		msgHandler.SendResponse(false, err.Error())
		msgHandler.Close()
		return
	}

	msgHandler.SendResponse(true, "Ready for data")
	md5 := md5.New()
	w := io.MultiWriter(file, md5)
	// io.CopyN(w, msgHandler, int64(request.Size)) /* Write and checksum as we go */
	w.Write(request.Chunk)
	file.Close()
	serverCheck := md5.Sum(nil)

	clientCheckMsg, _ := msgHandler.Receive()
	clientCheck := clientCheckMsg.GetChecksum().Checksum

	if util.VerifyChecksum(serverCheck, clientCheck) {
		log.Println("Successfully stored file.")
	} else {
		log.Println("FAILED to store file. Invalid checksum.")
	}

	transferCopy(nextNode1, request, "copy1")
	transferCopy(nextNode2, request, "copy2")
	log.Println("Copies have been stored!")
}

func transferCopy(addr string, request *messages.StorageRequest, copy string) {
	conn, err := net.Dial("tcp", addr + ":29333")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	msgHandler := messages.NewMessageHandler(conn)
	defer conn.Close()
	filepath := path + request.FileName + "_" + copy + "_" + fmt.Sprintf("%x", request.Checksum) + "_" + strconv.Itoa(int(request.Idx))
	log.Println("Store copy " + filepath)
	msgHandler.SendInternalTransferRequest(filepath, request.Chunk)
}

func handleInternalTransfer(msgHandler *messages.MessageHandler, request *messages.InternalFileTransferRequest) {
	log.Println("Transfer copy...")
	file, _ := os.OpenFile(request.Path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0666)
	file.Write(request.Chunk)
	file.Close()
	log.Println("Successfully copied!" + request.Path)
}

func handleRetrieval(msgHandler *messages.MessageHandler, request *messages.RetrievalRequest) {
	log.Println("Attempting to retrieve", request.FileName)
	// Get file size and make sure it exists
	prefix := request.FileName
	log.Println(prefix)
	pattern := filepath.Join(path + prefix + "_*")
	files, err := filepath.Glob(pattern)
	if err != nil {panic(err)}
	md5 := md5.New()
	for _, file := range files {
		chunk, _ := ioutil.ReadFile(file)
		md5.Write(chunk)
		checksum := md5.Sum(nil)
		tokens := strings.Split(file, "_")
		hashBytes, _ := hex.DecodeString(tokens[len(tokens)-2])
		if util.VerifyChecksum(checksum, hashBytes) {
			log.Println("File is okay.")
		} else {
			log.Println("File Broke.")
			chunk = coverFile(file)
		}
		msgHandler.SendRetrievalResponse(true, "Ready to send", uint64(len(chunk)), chunk, file)
		md5.Reset()
	}
	log.Println("successfully out")
	msgHandler.SendResponse(true, "0")
	// msgHandler.SendChecksumVerification(checksum)
}

func handleFileRescue(msgHandler *messages.MessageHandler, request *messages.AskSaveFileRequest) {
	log.Println("Resuce File now")
	files, err := ioutil.ReadDir(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	if err != nil {panic(err)}
	aim := request.FileName
	tokens := strings.Split(aim, "/")
	tokens1 := strings.Split(tokens[len(tokens)-1], "_")
	log.Println("FILE BROKEN " + aim)
	for _, file := range files {
		fn := file.Name()
		log.Println("Check file " + fn)
		tokens2 := strings.Split(fn, "_")
		if tokens1[0] == tokens2[0] && tokens1[len(tokens1)-1] == tokens2[len(tokens2)-1] {
			chunk, _ := ioutil.ReadFile(fn)
			msgHandler.SendInternalTransferRequest(fn, chunk)
			return
		}
	}
}

func coverFile(filename string) []byte {
	log.Println("Let's save the chunk!")
	conn, err := net.Dial("tcp", nextNode1 + ":29333")
	if err != nil {
		log.Fatalln(err.Error())
		return nil
	}
	msgHandler := messages.NewMessageHandler(conn)
	defer conn.Close()
	msgHandler.SendFileSaveRequest(filename)
	_, chunk := msgHandler.ReceiveInternalTransferResponse()
	file, err := os.OpenFile(filename, os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return nil
	}
	defer file.Close()

	// Truncate the file to remove its contents
	err = file.Truncate(0)
	if err != nil {
		fmt.Println("Error truncating file:", err)
		return nil
	}

	// Write new contents to the file
	_, err = file.Write(chunk)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return nil
	}
	return chunk
}

func handleDelete(msgHandler *messages.MessageHandler, request *messages.DeleteRequest) {
	log.Println("Attempting to delete", request.FileName)
	// Get file size and make sure it exists
	prefix := request.FileName
	log.Println(prefix)
	pattern := filepath.Join(path + prefix + "_*")
	files, err := filepath.Glob(pattern)
	if err != nil {panic(err)}
	// md5 := md5.New()
	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			// Handle the error
			fmt.Println("Error:", err)
			continue
		}
	}
	log.Println("Successfully delete")
}

func handleSaveRequest(request *messages.SaveRequest) {
	log.Println("A node has been lost!")
	conn, err := net.Dial("tcp", request.NextNode + ":29333")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	msgHandler := messages.NewMessageHandler(conn)
	defer conn.Close()
	files, err := ioutil.ReadDir(path)
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, file := range files {
		// Check if the file is not a directory
		if !file.IsDir() {
			fileName := ""
			chunk, _ := ioutil.ReadFile(file.Name())
			tokens := strings.Split(file.Name(), "_")
			idx := tokens[len(tokens)-1]
			checksum := tokens[len(tokens)-2]
			if tokens[len(tokens)-3] == "copy1" || tokens[len(tokens)-3] == "copy2" {
				for i := 0; i < len(tokens)-3; i++ {
					fileName += tokens[i]
				}
			} else {
				return
			}
			transferSave(fileName, checksum, idx, chunk, msgHandler)
		}
	}
}

func transferSave(filename string, checksum string, idx string, chunk []byte, msgHandler *messages.MessageHandler) {
	tokens := strings.Split(filename, "_")
	filepath := ""
	if tokens[len(tokens)-1] == "copy1" {
		for i:=0; i<len(tokens)-1; i++ {
			filepath += tokens[i]
		}
		filepath += "_" + checksum + "_" + idx
	} else if tokens[len(tokens)-1] == "copy2" {
		for i:=0; i<len(tokens)-1; i++ {
			filepath += tokens[i]
		}
		filepath += "_copy1_" + checksum + "_" + idx
	} 
	log.Println("Store copy " + filepath)
	msgHandler.SendInternalTransferRequest(filepath, chunk)
}

func handleClient(msgHandler *messages.MessageHandler) {
	defer msgHandler.Close()

	num_of_handled++
	for {
		wrapper, err := msgHandler.Receive()
		if err != nil {
			log.Println(err)
		}

		switch msg := wrapper.Msg.(type) {
		case *messages.Wrapper_StorageReq:
			handleStorage(msgHandler, msg.StorageReq)
			continue
		case *messages.Wrapper_RetrievalReq:
			handleRetrieval(msgHandler, msg.RetrievalReq)
			continue
		case *messages.Wrapper_DeleteReq:
			handleDelete(msgHandler, msg.DeleteReq)
			continue
		case *messages.Wrapper_InternalTransferReq:
			handleInternalTransfer(msgHandler, msg.InternalTransferReq)
			continue
		case *messages.Wrapper_AskCopyReq:
			nextNode1, nextNode2 = wrapper.GetAskCopyReq().GetNode1(), wrapper.GetAskCopyReq().GetNode2()
			log.Println("copy1 " + nextNode1)
			log.Println("copy2 " + nextNode2)
			continue
		case *messages.Wrapper_SaveReq:
			log.Println("A node has been failed! Let's help it!" )
			handleSaveRequest(msg.SaveReq)
			continue
		case *messages.Wrapper_AskSaveFileReq:
			log.Println("File rescue")
			handleFileRescue(msgHandler, msg.AskSaveFileReq)
		case nil:
			log.Println("Received an empty message, terminating client")
			return
		default:
			log.Printf("Unexpected message type: %T", msg)
		}
	}
}

var stat syscall.Statfs_t

func handleHeartBeat(msgHandler *messages.MessageHandler, hostname string) {
	for {
		syscall.Statfs(disk, &stat)
		free := stat.Bavail * uint64(stat.Bsize) / (1024 * 1024)
		msg := messages.Heartbeat{Alive: true, Space: free, NumOfHandled: num_of_handled, NodeName: hostname}
		wrapper := &messages.Wrapper{
			Msg: &messages.Wrapper_Hb{Hb: &msg},
		}
		msgHandler.Send(wrapper)
		nextNode1, nextNode2 = msgHandler.ReceiveAskCopyNodesResponse()
		time.Sleep(5 * time.Second)
	}
}

func main() {
	// args0 for address
	if len(os.Args) < 1 {
		fmt.Printf("Not enough arguments. Usage: %s server:port put|get file-name [download-dir]\n", os.Args[0])
		os.Exit(1)
	}
	time.Sleep(5 * time.Second)
	conn, err := net.Dial("tcp", "orion01:29000")
	if err != nil {
		log.Fatalln(err.Error())
		return
	}
	msgHandler := messages.NewMessageHandler(conn)
	defer conn.Close()

	syscall.Statfs(path, &stat)
	hostname,_ = os.Hostname()
	free := stat.Bavail * uint64(stat.Bsize)
	msgHandler.SendWakeUpRequest(hostname, free)
	go handleHeartBeat(msgHandler, hostname)

	listener, err := net.Listen("tcp", hostname + ":29333")
	if err != nil {
		log.Fatalln(err.Error())
		os.Exit(1)
	}
	defer listener.Close()
	for {
		log.Println("Listening...")
		if conn2, err := listener.Accept(); err == nil {
			log.Println("Accepted connection", conn2.RemoteAddr())
			handler2 := messages.NewMessageHandler(conn2)
			go handleClient(handler2)
		}
	}
}
