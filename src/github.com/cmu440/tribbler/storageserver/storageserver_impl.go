package storageserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type storageServer struct {
	topMap    map[string]interface{} // Main hash table, stores everything
	nodeID    uint32
	servers   []storagerpc.Node // List of all servers in the ring
	count     int               // Number of servers in the ring
	countLock sync.Mutex
	keyLocks  map[string]chan int
	// also used to initially count slave servers
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// nodeID is a random, unsigned 32-bit ID identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, nodeID uint32) (StorageServer, error) {

	// Set upt this server's info
	serverInfo := storagerpc.Node{HostPort: fmt.Sprintf("localhost:%d", port), NodeID: nodeID}
	var ss storageServer

	if masterServerHostPort == "" {

		// If this is the master server, set up a list of servers
		var servers = make([]storagerpc.Node, numNodes)
		servers[0] = serverInfo

		// Create the master server
		ss = storageServer{topMap: make(map[string]interface{}), nodeID: nodeID,
			servers: servers, count: 1, countLock: sync.Mutex{}, keyLocks: make(map[string]chan int)}

	} else {
		// Try to connect to the master at most five times
		args := storagerpc.RegisterArgs{ServerInfo: serverInfo}
		var reply storagerpc.RegisterReply
		var err error
		var master *rpc.Client
		for try := 1; try <= 5; try++ {
			master, err = rpc.DialHTTP("tcp", masterServerHostPort)
			if err == nil {
				break
			}
			if try == 5 {
				return nil, err
			}
			time.Sleep(time.Millisecond * 20)
		}
		for i := 1; i <= 5; i++ {
			master.Call("StorageServer.RegisterServer", args, &reply)
			if reply.Status == storagerpc.OK {
				// All servers are connected, create this slave server
				ss = storageServer{topMap: make(map[string]interface{}), nodeID: nodeID,
					servers: reply.Servers, count: numNodes, countLock: sync.Mutex{}, keyLocks: make(map[string]chan int)}
				break
			}
			// Wait one second, try to connect to master again
			if i == 5 {
				return nil, errors.New("couldn't connect to master")
			}
			time.Sleep(time.Millisecond * 20)

		}
	}

	// Start listening for connections from other storageServers and libstores
	rpc.RegisterName("StorageServer", &ss)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", serverInfo.HostPort)
	if e != nil {
		return nil, errors.New("Storage server couldn't start listening")
	}
	go http.Serve(l, nil)

	return &ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {

	serverID := args.ServerInfo.NodeID
	seen := false

	for i := 0; i < ss.count; i++ {
		if ss.servers[i].NodeID == serverID {
			seen = true
		}
	}

	// Add this server to the list
	if seen == false {
		ss.countLock.Lock()
		ss.servers[ss.count] = args.ServerInfo
		ss.count++
		ss.countLock.Unlock()
	}
	// If all servers have connected, send the OK and reply with server list
	if ss.count == len(ss.servers) {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
	} else {
		reply.Status = storagerpc.NotReady
		reply.Servers = nil
	}
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {

	// Reply with OK and servers only if all have connected
	if ss.count == len(ss.servers) {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
		return nil
	} else {
		reply.Status = storagerpc.NotReady
		return nil
	}
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {

	key := args.Key
	if rightStorageServer(ss, key) == false {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	if data, found := ss.topMap[key]; found {
		if str, ok := data.(string); ok {

			// key was found and had valid string data
			reply.Status = storagerpc.OK
			reply.Value = str
			return nil
		} else {

			// key value is corrupted, not a string
			return errors.New("bad value")
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {

	key := args.Key
	if rightStorageServer(ss, key) == false {
		reply.Status = storagerpc.WrongServer

		return nil
	}
	if _, found := ss.keyLocks[key]; found == false {
		ss.keyLocks[key] = make(chan int, 1)
	} else {
		<-ss.keyLocks[key]
	}

	if _, found := ss.topMap[key]; found {
		delete(ss.topMap, key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	ss.keyLocks[key] <- 1
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	key := args.Key
	if rightStorageServer(ss, key) == false {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	if data, found := ss.topMap[key]; found {
		if strList, ok := data.([]string); ok {
			// key was found, had valid []string data
			reply.Status = storagerpc.OK
			reply.Value = strList
		} else {
			// key was found with string data, return empty list (This shouldn't happen)
			reply.Status = storagerpc.OK
			reply.Value = make([]string, 0)
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	key := args.Key
	if rightStorageServer(ss, key) == false {
		reply.Status = storagerpc.WrongServer

		return nil
	}

	if _, found := ss.keyLocks[key]; found == false {
		ss.keyLocks[key] = make(chan int, 1)
	} else {
		<-ss.keyLocks[key]
	}

	reply.Status = storagerpc.OK
	ss.topMap[key] = args.Value
	ss.keyLocks[key] <- 1
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	key := args.Key

	if rightStorageServer(ss, key) == false {
		reply.Status = storagerpc.WrongServer
		ss.keyLocks[key] <- 1

		return nil
	}

	if _, found := ss.keyLocks[key]; found == false {
		ss.keyLocks[key] = make(chan int, 1)
	} else {
		<-ss.keyLocks[key]
	}

	if lst, found := ss.topMap[key]; found {
		if l, ok := lst.([]string); ok {

			for i := 0; i < len(l); i++ {
				if l[i] == args.Value {
					// value was already in list
					reply.Status = storagerpc.ItemExists
					ss.keyLocks[key] <- 1

					return nil
				}
			}
			// value was not in list, append to the end

			reply.Status = storagerpc.OK

			ss.topMap[key] = append(l, args.Value)
		} else {
			// list was corrputed, shouldn't happen
			ss.keyLocks[key] <- 1

			return errors.New("List to remove from is wrong type")
		}
	} else {
		// This key hasn't had a list made yet, make new list and insert value
		l := make([]string, 1)
		l[0] = args.Value

		ss.topMap[key] = l
		reply.Status = storagerpc.OK
	}
	ss.keyLocks[key] <- 1

	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key

	if rightStorageServer(ss, key) == false {
		reply.Status = storagerpc.WrongServer
		return nil
	}

	if _, found := ss.keyLocks[key]; found == false {
		ss.keyLocks[key] = make(chan int, 1)
	} else {
		<-ss.keyLocks[key]
	}

	if lst, found := ss.topMap[key]; found {
		if l, ok := lst.([]string); ok {

			for i := 0; i < len(l); i++ {
				if l[i] == args.Value {
					// found item in list, remove it and return
					reply.Status = storagerpc.OK
					ss.topMap[key] = append(l[:i], l[i+1:]...)
					ss.keyLocks[key] <- 1

					return nil
				}
			}
			// item was not in the list
			reply.Status = storagerpc.ItemNotFound
			ss.keyLocks[key] <- 1

			return nil
		} else {
			// list was corrupted, shouldn't happen
			ss.keyLocks[key] <- 1

			return errors.New("List to remove from is wrong type")
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
		ss.keyLocks[key] <- 1

		return nil
	}
}

func rightStorageServer(ss *storageServer, key string) bool {
	nextIndex := -1
	minIndex := -1
	hash := libstore.StoreHash(key)
	for i := 0; i < len(ss.servers); i++ {
		server := ss.servers[i]
		loc := server.NodeID
		if (loc >= hash) && ((nextIndex == -1) || (loc < ss.servers[nextIndex].NodeID)) {
			nextIndex = i
		}
		if (minIndex == -1) || (loc < ss.servers[minIndex].NodeID) {
			minIndex = i
		}
	}
	if nextIndex == -1 {
		nextIndex = minIndex
	}
	return ss.nodeID == ss.servers[nextIndex].NodeID
}
