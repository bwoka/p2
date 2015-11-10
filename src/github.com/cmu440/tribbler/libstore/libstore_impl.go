package libstore

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"time"
)

type libstore struct {
	myHostPort string
	mode       LeaseMode
	servers    []storagerpc.Node // The list of servers
	clients    []*rpc.Client     // List of ongoing connections to servers
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {

	ls := new(libstore)
	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))

	var client *rpc.Client
	var err error
	// Connect to master storage server
	for try := 1; try <= 5; try++ {

		client, err = rpc.DialHTTP("tcp", masterServerHostPort)
		if err == nil {
			break
		}
		if try == 5 {
			return nil, err
		}
		time.Sleep(time.Millisecond * 250)
	}
	var args storagerpc.GetServersArgs
	var reply storagerpc.GetServersReply
	var servers []storagerpc.Node

	// Try five times to request the list of storage servers from the master
	for i := 1; i <= 5; i++ {
		client.Call("StorageServer.GetServers", args, &reply)
		if reply.Status == storagerpc.OK {
			// Success!  Store list of servers
			servers = reply.Servers
			break
		} else {
			// Failure, sleep one second and try again
			if i == 5 {
				return nil, errors.New("Couldn't connect to storage server")
			}
			time.Sleep(time.Millisecond * 250)
		}
	}
	// Create libstore and save the connection to the master server
	var clients = make([]*rpc.Client, len(reply.Servers))

	// WRONG!  StorageServer does not assume servers[0] is master
	//clients[0] = client
	for j := 0; j < len(servers); j++ {
		if servers[j].HostPort == masterServerHostPort {
			clients[j] = client
			break
		}
	}
	ls.myHostPort = myHostPort
	ls.mode = mode
	ls.servers = servers
	ls.clients = clients
	return ls, nil

	// So it won't yell at me for fmt
	fmt.Println(1)
	return nil, nil
}

func (ls *libstore) Get(key string) (string, error) {

	args := &storagerpc.GetArgs{Key: key}
	var reply storagerpc.GetReply
	client := getConnection(ls, getStorageServerIndex(ls, key))
	client.Call("StorageServer.Get", args, &reply)
	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	} else {
		return "", errors.New("Key does not exist")
	}
}

func (ls *libstore) Put(key, value string) error {

	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	client := getConnection(ls, getStorageServerIndex(ls, key))
	client.Call("StorageServer.Put", args, &reply)
	if reply.Status == storagerpc.OK {
		return nil
	} else {
		// Shouldn't happen
		return errors.New("Error on Put")
	}
}

func (ls *libstore) Delete(key string) error {

	args := &storagerpc.DeleteArgs{Key: key}
	var reply storagerpc.DeleteReply
	client := getConnection(ls, getStorageServerIndex(ls, key))
	client.Call("StorageServer.Delete", args, &reply)
	if reply.Status == storagerpc.OK {
		return nil
	} else {
		return errors.New("Key does not exist")
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	args := &storagerpc.GetArgs{Key: key}
	var reply storagerpc.GetListReply
	client := getConnection(ls, getStorageServerIndex(ls, key))
	client.Call("StorageServer.GetList", args, &reply)
	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	} else {
		return nil, errors.New("Key does not exist")
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply
	client := getConnection(ls, getStorageServerIndex(ls, key))
	client.Call("StorageServer.RemoveFromList", args, &reply)
	if reply.Status == storagerpc.OK {
		return nil
	} else if reply.Status == storagerpc.KeyNotFound {
		return errors.New("Key does not exist")
	} else {
		return errors.New("Item does not exist")
	}
}

func (ls *libstore) AppendToList(key, newItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: newItem}
	var reply storagerpc.PutReply
	client := getConnection(ls, getStorageServerIndex(ls, key))
	client.Call("StorageServer.AppendToList", args, &reply)
	if reply.Status == storagerpc.OK {
		return nil
	} else {
		return errors.New("Item exists")
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}

func getStorageServerIndex(ls *libstore, key string) int {

	// nextIndex is the index of the correct storageserver
	nextIndex := -1
	// minIndex is the index of the ss with minimum nodeID
	minIndex := -1
	hash := StoreHash(key)
	for i := 0; i < len(ls.servers); i++ {
		server := ls.servers[i]
		loc := server.NodeID
		// If we found a new best server index
		if (loc >= hash) && ((nextIndex == -1) || (loc < ls.servers[nextIndex].NodeID)) {
			nextIndex = i
		}
		// If we found a new minimum
		if (minIndex == -1) || (loc < ls.servers[minIndex].NodeID) {
			minIndex = i
		}
	}
	// If we never found anything with a greater nodeID than the hash, return minimum
	if nextIndex == -1 {
		nextIndex = minIndex
	}
	return nextIndex
}

func getConnection(ls *libstore, serverIndex int) *rpc.Client {
	// Open a connection to this storage server if necessary
	if ls.clients[serverIndex] == nil {
		client, err := rpc.DialHTTP("tcp", ls.servers[serverIndex].HostPort)
		if err != nil {
			return nil
		}
		ls.clients[serverIndex] = client
	}
	return ls.clients[serverIndex]
}
