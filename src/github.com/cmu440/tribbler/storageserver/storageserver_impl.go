package storageserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type storageServer struct {
	topMap  map[string]interface{}
	servers []storagerpc.Node
	count   int
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

	serverInfo := storagerpc.Node{HostPort: fmt.Sprintf("localhost:%d", port), NodeID: nodeID}
	var ss storageServer
	if masterServerHostPort == "" {

		// Set up server info
		var servers = make([]storagerpc.Node, numNodes)
		servers[0] = serverInfo
		ss = storageServer{topMap: make(map[string]interface{}), servers: servers, count: 1}

		// Start listening for rpc calls from slaves and libstore
		rpc.RegisterName("storageServer", &ss)
		rpc.HandleHTTP()
		l, e := net.Listen("tcp", fmt.Sprintf(":%d", port))
		if e != nil {
			fmt.Println(e)
			return nil, errors.New("Master server couldn't start listening")
		}
		go http.Serve(l, nil)
		//		for ss.count < numNodes {
		//			time.Sleep(1000 * time.Millisecond)
		//			fmt.Println("waiting for registers")
		//		}
	} else {
		// Try to connect to the at most five times
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
				ss = storageServer{topMap: make(map[string]interface{}), servers: reply.Servers, count: numNodes}
				break
			} else {
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

	fmt.Println("getting register call")
	// Need to lock this first so that we don't overwrite
	ss.servers[ss.count] = args.ServerInfo
	ss.count++
	fmt.Println(fmt.Sprintf("count: %d", ss.count))
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
	fmt.Println("trying to send servers")
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
	//	split := strings.Split(key, ":")
	//	typ := split[len(split)-1]
	//	if !(len(split) == 2) {
	//		return nil
	//	}
	fmt.Println("Getting!")
	data := ss.topMap[key]
	fmt.Println(data)
	if str, ok := data.(string); ok {
		fmt.Println("Is okay!")
		reply.Value = str
		return nil
	} else {
		fmt.Println("Get failed")
		return errors.New("bad value")
	}

	/*	if typ == "usrid" {
			*reply = ss.topMap[key]
		} else if typ == "sublist" {
			*reply = ss.topMap[key]
			return nil
		} else if typ == "triblist" {
			*reply = ss.topMap[key]
			return nil
		} else {
			subs := strings.Split(typ, "_")
			if len(subs) == 3 && subs[0] == "post" {
				timestamp := subs[1]
				tiebreak := subs[2]
				*reply = ss.topMap[key]

			}
		} */
	return errors.New("not implemented")
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	delete(ss.topMap, args.Key)
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	data := ss.topMap[args.Key]
	if strList, ok := data.([]string); ok {
		reply.Value = strList
		return nil
	}
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	fmt.Println("Putting")
	ss.topMap[args.Key] = args.Value
	fmt.Println(fmt.Sprintf("Just put %s: %s", args.Key, ss.topMap[args.Key]))
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	lst := ss.topMap[args.Key]
	if l, ok := lst.([]string); ok {
		ss.topMap[args.Key] = append(l, args.Value)
	}
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	lst := ss.topMap[args.Key]
	if l, ok := lst.([]string); ok {
		for i := 0; i < len(l); i++ {
			if l[i] == args.Value {
				ss.topMap[args.Key] = append(l[:i], l[i+1:]...)
				return nil
			}
		}
	}
	return errors.New("toRemove not in list")
}
