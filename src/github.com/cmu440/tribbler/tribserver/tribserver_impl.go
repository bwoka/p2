package tribserver

import (
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
)

type tribServer struct {
	ls libstore.Libstore
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {

	// Create the libstore for this server
	ls, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		return nil, errors.New("Couldn't start libstore for Tribserver")
	}
	ts := tribServer{ls: ls}

	// Start listening for connections from TribClients
	rpc.RegisterName("TribServer", &ts)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", myHostPort)
	if e != nil {
		return nil, errors.New("Tribserver couldn't start listening")
	}
	go http.Serve(l, nil)
	return &ts, nil

	// Get rid of this.  I didn't want to keep commenting out fmt for testing when I didn't use it
	fmt.Println("this is here so I don't throw an error for not using fmt")
	return nil, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {

	user := util.FormatUserKey(args.UserID)

	// Check that this user doesn't already exist
	if _, err := ts.ls.Get(user); err != nil {
		ts.ls.Put(user, user)
		reply.Status = tribrpc.OK
	} else {
		reply.Status = tribrpc.Exists
	}
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	userkey := util.FormatUserKey(args.UserID)
	targetkey := util.FormatUserKey(args.TargetUserID)

	usersubs := util.FormatSubListKey(args.UserID)
	target := args.TargetUserID

	// Make sure user exists
	if _, eu := ts.ls.Get(userkey); eu != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	// Make sure targetUser exists
	if _, et := ts.ls.Get(targetkey); et != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	// Make sure user isn't already subscribed to target
	err := ts.ls.AppendToList(usersubs, target)
	if err == nil {
		reply.Status = tribrpc.OK
	} else {
		reply.Status = tribrpc.Exists
	}
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	userkey := util.FormatUserKey(args.UserID)
	targetkey := util.FormatUserKey(args.TargetUserID)

	usersubs := util.FormatSubListKey(args.UserID)
	target := args.TargetUserID

	// Make sure user exists
	if _, eu := ts.ls.Get(userkey); eu != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	// Make sure target exists
	if _, et := ts.ls.Get(targetkey); et != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	// Make sure user is subscribed to target
	if err := ts.ls.RemoveFromList(usersubs, target); err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
	} else {
		reply.Status = tribrpc.OK
	}
	return nil
}

func (ts *tribServer) GetSubscriptions(args *tribrpc.GetSubscriptionsArgs, reply *tribrpc.GetSubscriptionsReply) error {

	userkey := util.FormatUserKey(args.UserID)

	usersubs := util.FormatSubListKey(args.UserID)

	// Make sure user exists
	if _, eu := ts.ls.Get(userkey); eu != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	if lst, err := ts.ls.GetList(usersubs); err == nil {
		reply.Status = tribrpc.OK
		reply.UserIDs = lst
		return nil
	} else {
		// The user has no subscriptions yet
		reply.Status = tribrpc.OK
		reply.UserIDs = make([]string, 0)
		return nil
	}
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	userkey := util.FormatUserKey(args.UserID)

	// Make sure user exists
	if _, eu := ts.ls.Get(userkey); eu != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	userkey := util.FormatUserKey(args.UserID)

	// Make sure user exists
	if _, eu := ts.ls.Get(userkey); eu != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userkey := util.FormatUserKey(args.UserID)

	// Make sure user exists
	if _, eu := ts.ls.Get(userkey); eu != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userkey := util.FormatUserKey(args.UserID)

	// Make sure user exists
	if _, eu := ts.ls.Get(userkey); eu != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	return nil
}
