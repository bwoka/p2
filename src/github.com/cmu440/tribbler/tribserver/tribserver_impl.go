package tribserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"time"
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

	ts := new(tribServer)

	// Create the libstore for this server
	ls, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Never)
	if err != nil {
		fmt.Println("Failed to create libstore")
		return nil, errors.New("Couldn't start libstore for Tribserver")
	}
	ts.ls = ls

	// Start listening for connections from TribClients
	rpc.RegisterName("TribServer", tribrpc.Wrap(ts))
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", myHostPort)
	if e != nil {
		fmt.Println("Failed to listen with Tribserver")
		return nil, errors.New("Tribserver couldn't start listening")
	}
	go http.Serve(l, nil)
	return ts, nil

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

	postkey := util.FormatPostKey(args.UserID, time.Now().UnixNano())
	// make sure postkey is unique
	for {
		if _, err := ts.ls.Get(postkey); err == nil {
			postkey = util.FormatPostKey(args.UserID, time.Now().UnixNano())
			continue
		}
		break
	}

	// Create the Tribble
	tribble := tribrpc.Tribble{UserID: args.UserID, Posted: time.Now(),
		Contents: args.Contents}
	value, err := json.Marshal(tribble)
	if err != nil {
		return errors.New("Couldn't create Tribble")
	}

	// Hash the Tribble and add the postkey to the user's TribList
	if ep := ts.ls.Put(postkey, string(value)); ep != nil {
		return errors.New("Couldn't post Tribble")
	}
	if ea := ts.ls.AppendToList(util.FormatTribListKey(args.UserID), postkey); ea != nil {
		return errors.New("Couldn't add TribbleID to user list")
	}
	reply.PostKey = postkey
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	userkey := util.FormatUserKey(args.UserID)

	// Make sure user exists
	if _, eu := ts.ls.Get(userkey); eu != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	postkey := args.PostKey

	// Make sure the Tribble exists
	if err := ts.ls.Delete(postkey); err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	// Make sure the Tribble is in this user's list
	if err := ts.ls.RemoveFromList(util.FormatTribListKey(args.UserID), postkey); err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userkey := util.FormatUserKey(args.UserID)

	// Make sure user exists
	if _, eu := ts.ls.Get(userkey); eu != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// Store list of marshalled Tribbles in lst.  Return empty list if user had no subscriptions.
	var lst []string
	var err error
	if lst, err = ts.ls.GetList(util.FormatTribListKey(args.UserID)); err != nil {
		reply.Status = tribrpc.OK
		reply.Tribbles = make([]tribrpc.Tribble, 0)
		return nil
	}

	// Find out how many posts we will return
	var recentPosts []string
	if len(lst) > 100 {
		recentPosts = lst[len(lst)-100:]
	} else {
		recentPosts = lst
	}

	// Loop through this users Tribbles in reverse order and grab up to 100
	tribbles := make([]tribrpc.Tribble, len(recentPosts))
	var mtribble string
	var tribble tribrpc.Tribble
	for i := 0; i < len(tribbles); i++ {
		mtribble, _ = ts.ls.Get(recentPosts[len(tribbles)-1-i])
		json.Unmarshal([]byte(mtribble), &tribble)
		tribbles[i] = tribble
	}
	reply.Status = tribrpc.OK
	reply.Tribbles = tribbles
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	userkey := util.FormatUserKey(args.UserID)

	// Make sure user exists
	if _, eu := ts.ls.Get(userkey); eu != nil {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	// Get list of subscriptions.  return empty list if user has no subscriptions.
	var subs []string
	var esubs error
	if subs, esubs = ts.ls.GetList(util.FormatSubListKey(args.UserID)); esubs != nil {
		reply.Status = tribrpc.OK
		reply.Tribbles = make([]tribrpc.Tribble, 0)
		return nil
	}

	numTribbles := 0

	// Get the last 100 tribble IDs from each subscribed user
	tribIDs := make([][]string, len(subs))
	for i := 0; i < len(subs); i++ {
		if lst, err := ts.ls.GetList(util.FormatTribListKey(subs[i])); err == nil {
			if len(lst) > 100 {
				tribIDs[i] = lst[len(lst)-100:]
			} else {
				tribIDs[i] = lst
			}
			numTribbles += len(lst)
		}
	}

	// lastTribs is the most recent tribble seen from each subscribed user
	lastTribs := make([]tribrpc.Tribble, len(subs))
	// index is the list of indexes into tribIDs for each subscribed user
	index := make([]int, len(subs))

	if numTribbles > 100 {
		numTribbles = 100
	}

	finalTribbles := make([]tribrpc.Tribble, numTribbles)
	var mtribble string
	var tribble tribrpc.Tribble

	// valid[i] marks if an unused Tribble is located at lastTribs[i]
	valid := make([]bool, len(lastTribs))

	for j := 0; j < numTribbles; j++ {
		minIndex := -1
		for check := 0; check < len(lastTribs); check++ {
			// If this user has more tribbles left
			if index[check] < len(tribIDs[check]) {
				// If we need to get a new tribble for this user
				if valid[check] == false {
					mtribble, _ = ts.ls.Get(tribIDs[check][len(tribIDs[check])-1-index[check]])
					json.Unmarshal([]byte(mtribble), &tribble)
					lastTribs[check] = tribble
					valid[check] = true
				}
				// This tribble is more
				if minIndex == -1 || cmpLessTribble(lastTribs[check], lastTribs[minIndex]) {
					minIndex = check
				}
			}
		}
		copyTribble := lastTribs[minIndex]
		finalTribbles[j] = copyTribble
		valid[minIndex] = false
		index[minIndex] += 1

	}

	reply.Status = tribrpc.OK
	reply.Tribbles = finalTribbles
	return nil
}

// Returns true if t1 is a more recent Tribble than t2
func cmpLessTribble(t1, t2 tribrpc.Tribble) bool {
	time1 := t1.Posted
	time2 := t2.Posted
	return time1.UnixNano() > time2.UnixNano()
}
