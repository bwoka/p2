package storageserver

import (
	"errors"

	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	// TODO: implement this!
	topMap map[string]interface{}

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
	return (make(storageServer{topMap:make(map[string]interface{})}),nil)
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	key=args.key
	split:=Split(key, ":")
	typ:=split[len(split)-1]
	if !(len(split)=="2"){
		return error
	}
	if typ=="usrid"{
		*reply=topMap[args.key]
	}
	else if typ=="sublist"{
		*reply=topMap[args.key]
		return nil
	}
	else if typ=="triblist"{
		*reply=topMap[args.key]
		return nil
	}
	else{
		subs:=Split(typ,"_")
		if len(subs)==3 && subs[0]="post"{
			timestamp:=subs[1]
			tiebreak:=subs[2]
			*reply=topMap[args.key]

		}
	}
	return errors.New("not implemented")
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {
	Delete(topMap, args.key)
	return nil
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	*reply=topMap[args.key]
	return nil
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	*reply=topMap[args.key]=topMap[args.value]
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	lst:=topMap[args.key]
	topMap[args.key]=lst+args.toAppend
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
		lst:=topMap[args.key]
		topMap[args.key]=lst+args.toAppend
		for i:=0; i<len(lst);i++{
			if lst[i]==args.toRemove{
				topMap[args.key]=lst[:i]+lst[i+1:]
				return nil
			}
		}
	return errors.New("toRemove not in list")
}
