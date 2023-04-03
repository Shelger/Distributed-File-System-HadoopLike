# Project 1: Distributed File System

1. Components and Features:<br />
  i. Parallel storage/retrieval<br />
  ii. Interoperability<br />
  iii. Fault Tolerance<br />
      Replicas
2. Diagram:<br />
  ![Structure Chart](Flowchart.jpeg "Flow Chart")

| From | Buffer | To |
| -------- | -------- | -------- |
| Storage Node | Sn - Tell controller the node has been added  | Controller |
| Storage Node | Hb - Heartbeat | Controller |
| Client | put/get/info/ls - Get nodes information from controller | Controller |
| Client | put/get - Communicate with nodes | Storage Node |
| Storage Node | response - Provide resources for client | Client |
3. Command Line:<br />
  ./client <br />
  put <filename> (<number_of_chunks> optional)          store files into nodes<br />
  get <filename>                                        retrive data from nodes<br />
  delete <filename>                                     delete files and their duplicates in the nodes<br />
  ls                                                    list the nodes storing files<br />
  info                                                  log the space of the disk in GB, active nodes, and how many requests are handled by the node<br />


To Start the project:<br />
Suppose you are in the path of main dir of the project<br />
  Step 1: Run the controller.go in the controller dir<br />
          $go run controller/controller.go<br />
  Step 2: Run as many as Orion machines you like<br />
          $go run storageNodes/storageNode.go<br />
  Step 3: Run client applications<br />
          $go run client put/get/delete/ls/info ....<br />

          
Or easily apply by:<br />
  Step 1: $./startup.sh<br />
  Step 2: $go run client put/get/delete/....<br />
  Step 3: $./stop.sh<br />
