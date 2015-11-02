package storageserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type storageServer struct {
	topMap  map[string]interface{} // Main hash table, stores everything
	servers []storagerpc.Node      // List of all servers in the ring
	count   int                    // Number of servers in the ring
	rwLock  *sync.Mutex            // Lock for any reading and writing to this server,
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
		ss = storageServer{topMap: make(map[string]interface{}), servers: servers, count: 1, rwLock: &sync.Mutex{}}

		// Start listening for rpc calls from slaves and libstores
		rpc.RegisterName("storageServer", &ss)
		rpc.HandleHTTP()
		l, e := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if e != nil {
			fmt.Println(e)
			return nil, errors.New("Master server couldn't start listening")
		}
		go http.Serve(l, nil)
	} else {
		// Try to connect to the master at most five times
		args := storagerpc.RegisterArgs{ServerInfo: serverInfo}
		master, err := rpc.DialHTTP("tcp", masterServerHostPort)
		var reply storagerpc.RegisterReply
		if err != nil {
			return nil, err
		}
		for i := 0; i < 5; i++ {
			master.Call("storageServer.RegisterServer", args, &reply)
			fmt.Println(fmt.Sprintf("%d", reply.Status))
			if reply.Status == storagerpc.OK {
				// All servers are connected, create this slave server
				ss = storageServer{topMap: make(map[string]interface{}), servers: reply.Servers, count: numNodes, rwLock: &sync.Mutex{}}
				break
			} else {
				// Wait one second, try to connect to master again
				time.Sleep(1000 * time.Millisecond)
				if i == 4 {
					return nil, errors.New("couldn't connect to master")
				}
			}
		}
	}

	return &ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {

	if ss.count >= len(ss.servers) {
		return errors.New("Too many servers connected")
	}

	// Add this server to the list
	ss.rwLock.Lock()
	ss.servers[ss.count] = args.ServerInfo
	ss.count++
	ss.rwLock.Unlock()

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
	if data, found := ss.topMap[key]; found {
		if str, ok := data.(string); ok {
			reply.Status = storagerpc.OK
			reply.Value = str
			return nil
		} else {
			return errors.New("bad value")
		}
	} else {
		fmt.Println(key)
		fmt.Println("not found?  Whatttttttttt?")
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	key := args.Key
	if _, found := ss.topMap[key]; found {
		delete(ss.topMap, args.Key)
		reply.Status = storagerpc.OK
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	key := args.Key
	if data, found := ss.topMap[key]; found {
		if strList, ok := data.([]string); ok {
			reply.Status = storagerpc.OK
			reply.Value = strList
		} else {
			return errors.New("bad value")
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	if _, found := ss.topMap[key]; found {
		reply.Status = storagerpc.ItemExists
	} else {
		ss.topMap[args.Key] = args.Value
		reply.Status = storagerpc.OK
	}
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	if lst, found := ss.topMap[key]; found {
		if l, ok := lst.([]string); ok {
			reply.Status = storagerpc.OK
			ss.topMap[args.Key] = append(l, args.Value)
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	if lst, found := ss.topMap[key]; found {
		if l, ok := lst.([]string); ok {
			for i := 0; i < len(l); i++ {
				if l[i] == args.Value {
					reply.Status = storagerpc.OK
					ss.topMap[args.Key] = append(l[:i], l[i+1:]...)
					return nil
				}
			}
			reply.Status = storagerpc.ItemNotFound
			return nil
		} else {
			return errors.New("List to remove from is wrong type")
		}
	} else {
		reply.Status = storagerpc.KeyNotFound
		return nil
	}
}
