package libstore

import (
	"errors"
	//"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"time"
)

type libstore struct {
	myHostPort string
	mode       LeaseMode
	servers    []storagerpc.Node // The list of servers
	conns      []connection      // List of ongoing connections to servers
}

type connection struct {
	server storagerpc.Node
	client *rpc.Client
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

	// Connect to master storage server
	client, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	var args storagerpc.GetServersArgs
	var reply storagerpc.GetServersReply
	var servers []storagerpc.Node

	// Try five times to request the list of storage servers from the master
	for i := 0; i < 5; i++ {
		client.Call("StorageServer.GetServers", args, &reply)
		if reply.Status == storagerpc.OK {
			// Success!  Store list of servers
			servers = reply.Servers
			break
		} else {
			// Failure, sleep one second and try again
			time.Sleep(1000 * time.Millisecond)
			if i == 4 {
				return nil, errors.New("Couldn't connect to storage server")
			}
		}
	}
	// Create libstore and save the connection to the master server
	var connections = make([]connection, len(reply.Servers))
	connections[0] = connection{server: servers[0], client: client}
	libstore := libstore{myHostPort: myHostPort, mode: mode,
		servers: servers, conns: connections}
	// rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	return &libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {

	args := &storagerpc.GetArgs{Key: key}
	var reply storagerpc.GetReply
	ls.conns[0].client.Call("StorageServer.Get", args, &reply)
	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	} else {
		return "", errors.New("Key does not exist")
	}
}

func (ls *libstore) Put(key, value string) error {

	args := &storagerpc.PutArgs{Key: key, Value: value}
	var reply storagerpc.PutReply
	ls.conns[0].client.Call("StorageServer.Put", args, &reply)
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
	ls.conns[0].client.Call("StorageServer.Delete", args, &reply)
	if reply.Status == storagerpc.OK {
		return nil
	} else {
		return errors.New("Key does not exist")
	}
}

func (ls *libstore) GetList(key string) ([]string, error) {
	args := &storagerpc.GetArgs{Key: key}
	var reply storagerpc.GetListReply
	ls.conns[0].client.Call("StorageServer.GetList", args, &reply)
	if reply.Status == storagerpc.OK {
		return reply.Value, nil
	} else {
		return nil, errors.New("Key does not exist")
	}
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	args := &storagerpc.PutArgs{Key: key, Value: removeItem}
	var reply storagerpc.PutReply
	ls.conns[0].client.Call("StorageServer.RemoveFromList", args, &reply)
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
	ls.conns[0].client.Call("StorageServer.AppendToList", args, &reply)
	if reply.Status == storagerpc.OK {
		return nil
	} else {
		return errors.New("Item exists")
	}
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
