package libstore

import (
	"errors"
	"fmt"
	//	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"time"
)

type libstore struct {
	myHostPort string
	mode       LeaseMode
	servers    []storagerpc.Node
	conns      []connection
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
	client, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	var args storagerpc.GetServersArgs
	var reply storagerpc.GetServersReply
	var servers []storagerpc.Node
	for i := 0; i < 5; i++ {
		client.Call("storageServer.GetServers", args, &reply)
		if reply.Status == storagerpc.OK {
			servers = reply.Servers
			break
		} else {
			fmt.Println(fmt.Sprintf("status: %d", reply.Status))
			time.Sleep(1000 * time.Millisecond)
			if i == 4 {
				return nil, errors.New("Couldn't connect to storage server")
			}
		}
	}
	var connections = make([]connection, len(reply.Servers))
	connections[0] = connection{server: servers[0], client: client}
	libstore := libstore{myHostPort: myHostPort, mode: mode,
		servers: servers, conns: connections}
	//	rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
	return &libstore, nil
}

func (ls *libstore) Get(key string) (string, error) {
	var args storagerpc.GetArgs
	var reply storagerpc.GetReply
	args.Key = key
	ls.conns[0].client.Call("storageServer.Get", args, &reply)
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	var args storagerpc.PutArgs
	var reply storagerpc.PutReply
	args.Key = key
	args.Value = value
	ls.conns[0].client.Call("storageServer.Put", args, &reply)
	return nil
}

func (ls *libstore) Delete(key string) error {
	return errors.New("not implemented")
}

func (ls *libstore) GetList(key string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	return errors.New("not implemented")
}

func (ls *libstore) AppendToList(key, newItem string) error {
	return errors.New("not implemented")
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	return errors.New("not implemented")
}
