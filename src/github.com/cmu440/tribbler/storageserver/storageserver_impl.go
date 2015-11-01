package storageserver

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/storagerpc"
)

type storageServer struct {
	// TODO: implement this!
	topMap map[string]interface{}
}

type GetArgs struct {
	key string
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
	ss := storageServer{topMap: make(map[string]interface{})}
	return &ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	return errors.New("not implemented")
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {
	key := args.Key
	//	split := strings.Split(key, ":")
	//	typ := split[len(split)-1]
	//	if !(len(split) == 2) {
	//		return nil
	//	}
	data := ss.topMap[key]
	if str, ok := data.(string); ok {
		reply.Value = str
		return nil
	} else {
		return nil
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
	ss.topMap[args.Key] = ss.topMap[args.Value]
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
