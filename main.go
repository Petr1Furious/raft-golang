package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

var randGenerator = rand.New(rand.NewSource(time.Now().UnixNano()))

///////////////////////////////////////////////////////////////////////////
// Data Structures for Raft
///////////////////////////////////////////////////////////////////////////

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

type Resource struct {
	ID      string
	Content map[string]string
}

type LogCommand struct {
	ID      string
	Content map[string]string
}

type LogEntry struct {
	Term    int
	Command *LogCommand
}

type Store struct {
	casMu     sync.Mutex
	resources map[string]*Resource
}

func NewStore() *Store {
	return &Store{
		resources: make(map[string]*Resource),
	}
}

func (s *Store) Apply(cmd *LogCommand) string {
	log.Printf("Applying command: %v", cmd)

	if cmd.Content == nil {
		delete(s.resources, cmd.ID)
	} else {
		s.resources[cmd.ID] = &Resource{
			ID:      cmd.ID,
			Content: cmd.Content,
		}
	}

	return cmd.ID
}

func (s *Store) Get(id string) (*Resource, bool) {
	r, ok := s.resources[id]
	if !ok {
		return nil, false
	}
	copy := &Resource{
		ID:      r.ID,
		Content: make(map[string]string, len(r.Content)),
	}
	for k, v := range r.Content {
		copy.Content[k] = v
	}
	return copy, true
}

func (s *Store) GetCreateCommand(content map[string]string) *LogCommand {
	id := fmt.Sprintf("%d", time.Now().UnixNano())
	return &LogCommand{
		ID:      id,
		Content: content,
	}
}

func (s *Store) GetPutCommand(id string, content map[string]string) *LogCommand {
	return &LogCommand{
		ID:      id,
		Content: content,
	}
}

// Update the Store's GetPatchCommand to accept expected and new content
func (s *Store) GetPatchCommand(id string, expectedContent map[string]string, newContent map[string]string) *LogCommand {
	if resource, ok := s.resources[id]; ok {
		if !reflect.DeepEqual(resource.Content, expectedContent) {
			return nil // CAS failed
		}
	} else {
		return nil // Resource does not exist
	}

	return &LogCommand{
		ID:      id,
		Content: newContent,
	}
}

func (s *Store) GetDeleteCommand(id string) *LogCommand {
	return &LogCommand{
		ID: id,
	}
}

///////////////////////////////////////////////////////////////////////////
// Raft Node
///////////////////////////////////////////////////////////////////////////

type Node struct {
	mu          sync.Mutex
	peers       []string // list of peer addresses
	address     string
	state       State
	term        int
	votedFor    *string
	log         []LogEntry
	commitIndex int
	lastApplied int
	store       *Store
	nextIndex   []int
	matchIndex  []int
	leaderID    *string

	electionTimer *time.Timer

	httpClient *http.Client
}

func NewNode(id int, address string, peers []string) *Node {
	n := &Node{
		address:     address,
		peers:       peers,
		state:       Follower,
		store:       NewStore(),
		httpClient:  &http.Client{Timeout: 500 * time.Millisecond},
		commitIndex: -1,
		lastApplied: -1,
	}
	n.resetElectionTimer()
	return n
}

func (n *Node) resetElectionTimer() {
	if n.electionTimer != nil {
		n.electionTimer.Stop()
	}
	timeout := 150 + randGenerator.Intn(150)
	n.electionTimer = time.NewTimer(time.Duration(timeout) * time.Millisecond)
}

func (n *Node) run() {
	for {
		select {
		case <-n.electionTimer.C:
			n.startElection()
		case <-time.After(50 * time.Millisecond):
			n.mu.Lock()
			if n.state == Leader {
				n.resetElectionTimer()
				n.sendHeartbeats()
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.term++
	n.votedFor = &n.address
	termAtStart := n.term
	votes := 1
	n.resetElectionTimer()
	n.mu.Unlock()

	log.Printf("Starting election, term: %d", termAtStart)

	done := make(chan struct{})
	timeout := time.After(2 * time.Second)

	for _, peer := range n.peers {
		if peer == n.address {
			continue
		}
		go func(peer string) {
			lastLogIndex := len(n.log) - 1
			var lastLogTerm int
			if lastLogIndex >= 0 {
				lastLogTerm = n.log[lastLogIndex].Term
			} else {
				lastLogTerm = -1
			}
			args := RequestVoteArgs{
				Term:         termAtStart,
				CandidateID:  n.address,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			reply := n.sendRequestVote(peer, args)
			n.mu.Lock()
			defer n.mu.Unlock()
			if reply.Term > n.term {
				n.convertToFollower(reply.Term)
				close(done)
				return
			}
			if n.term == termAtStart && n.state == Candidate {
				if reply.VoteGranted {
					votes++
					if votes > len(n.peers)/2 {
						n.convertToLeader()
						close(done)
					}
				}
			}
		}(peer)
	}

	select {
	case <-done:
		// Election completed
	case <-timeout:
		log.Printf("Election timed out")
	}
}

func (n *Node) convertToFollower(newTerm int) {
	log.Printf("Converting to follower, term: %d", newTerm)

	n.state = Follower
	n.term = newTerm
	n.votedFor = nil
	n.leaderID = nil
	n.resetElectionTimer()
}

func (n *Node) convertToLeader() {
	log.Printf("Converting to leader, term: %d", n.term)

	n.state = Leader
	n.leaderID = &n.address
	n.nextIndex = make([]int, len(n.peers))
	n.matchIndex = make([]int, len(n.peers))
	for i := range n.peers {
		n.nextIndex[i] = len(n.log)
		n.matchIndex[i] = -1
	}
}

func (n *Node) sendHeartbeats() {
	if n.state != Leader {
		return
	}
	term := n.term

	for i, peer := range n.peers {
		if peer == n.address {
			continue
		}
		go n.sendAppendEntries(i, peer, term)
	}
}

// Client command apply (leader only), expects lock to be held
func (n *Node) leaderAppendCommand(cmd *LogCommand) (string, error) {
	if n.state != Leader {
		n.mu.Unlock()
		return "", fmt.Errorf("not leader")
	}
	entry := LogEntry{Term: n.term, Command: cmd}
	log.Printf("Leader appending entry: %v", entry)
	n.log = append(n.log, entry)
	index := len(n.log) - 1
	term := n.term
	n.mu.Unlock()

	for i, peer := range n.peers {
		if peer == n.address {
			continue
		}
		go n.sendAppendEntries(i, peer, term)
	}

	timeout := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return "", fmt.Errorf("commit timeout")
		case <-ticker.C:
			n.mu.Lock()
			if n.commitIndex >= index {
				n.mu.Unlock()
				return cmd.ID, nil
			}
			n.mu.Unlock()
		}
	}
}

func (n *Node) applyToStateMachine() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		entry := n.log[n.lastApplied]
		_ = n.store.Apply(entry.Command)
	}
}

func (n *Node) updateCommitIndex() {
	for i := len(n.log) - 1; i > n.commitIndex; i-- {
		matchCount := 1 // Count self
		for j := range n.peers {
			if n.peers[j] == n.address {
				continue
			}
			if n.matchIndex[j] >= i {
				matchCount++
			}
		}
		if matchCount > len(n.peers)/2 && n.log[i].Term == n.term {
			n.commitIndex = i
			n.applyToStateMachine()
			break
		}
	}
}

func (n *Node) findFirstIndexOfTerm(term int) int {
	for i := len(n.log) - 1; i >= 0; i-- {
		if n.log[i].Term == term {
			for j := i; j >= 0; j-- {
				if n.log[j].Term != term {
					return j + 1
				}
			}
			return 0
		}
	}
	return -1
}

///////////////////////////////////////////////////////////////////////////
// RPC Types
///////////////////////////////////////////////////////////////////////////

type RequestVoteArgs struct {
	Term         int
	CandidateID  string
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictTerm  int
	ConflictIndex int
}

///////////////////////////////////////////////////////////////////////////
// RPC send methods (over HTTP)
///////////////////////////////////////////////////////////////////////////

func (n *Node) sendRequestVote(peer string, args RequestVoteArgs) RequestVoteReply {
	var reply RequestVoteReply
	data, _ := json.Marshal(args)
	req, _ := http.NewRequest("POST", "http://"+peer+"/raft/requestvote", bytes.NewReader(data))
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return reply
	}
	defer resp.Body.Close()
	json.NewDecoder(resp.Body).Decode(&reply)
	return reply
}

func (n *Node) sendAppendEntries(peerIndex int, peer string, term int) {
	n.mu.Lock()
	if n.state != Leader || n.term != term {
		n.mu.Unlock()
		return
	}

	prevLogIndex := n.nextIndex[peerIndex] - 1
	var prevLogTerm int
	if prevLogIndex >= 0 && prevLogIndex < len(n.log) {
		prevLogTerm = n.log[prevLogIndex].Term
	} else {
		prevLogTerm = -1
	}

	toSend := []LogEntry{}
	if n.nextIndex[peerIndex] < len(n.log) {
		toSend = n.log[n.nextIndex[peerIndex]:]
	}

	args := AppendEntriesArgs{
		Term:         n.term,
		LeaderID:     n.address,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      toSend,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	if len(toSend) > 0 {
		log.Printf("Sending %d entries to %s", len(toSend), peer)
	}

	data, _ := json.Marshal(args)
	req, _ := http.NewRequest("POST", "http://"+peer+"/raft/appendentries", bytes.NewReader(data))
	resp, err := n.httpClient.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	var reply AppendEntriesReply
	json.NewDecoder(resp.Body).Decode(&reply)

	n.mu.Lock()
	defer n.mu.Unlock()
	if reply.Term > n.term {
		n.convertToFollower(reply.Term)
		return
	}
	if n.state != Leader || n.term != term {
		return
	}
	if reply.Success {
		n.nextIndex[peerIndex] = args.PrevLogIndex + len(toSend) + 1
		n.matchIndex[peerIndex] = n.nextIndex[peerIndex] - 1

		if len(toSend) > 0 {
			log.Printf("Updating commitIndex, peer: %s, matchIndex: %d", peer, n.matchIndex[peerIndex])
		}
		n.updateCommitIndex()
	} else {
		log.Printf("AppendEntries to %s failed, trying to reconcile, reply: %v", peer, reply)
		if reply.ConflictTerm != -1 {
			index := n.findFirstIndexOfTerm(reply.ConflictTerm)
			if index >= 0 {
				n.nextIndex[peerIndex] = index
			} else {
				n.nextIndex[peerIndex] = reply.ConflictIndex
			}
		} else {
			n.nextIndex[peerIndex] = reply.ConflictIndex
		}
		go n.sendAppendEntries(peerIndex, peer, term)
	}
}

///////////////////////////////////////////////////////////////////////////
// RPC handlers (HTTP handlers)
///////////////////////////////////////////////////////////////////////////

func (n *Node) handleRequestVote(w http.ResponseWriter, r *http.Request) {
	var args RequestVoteArgs
	json.NewDecoder(r.Body).Decode(&args)

	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.term}
	if args.Term > n.term {
		n.convertToFollower(args.Term)
	}
	if args.Term < n.term {
		reply.VoteGranted = false
	} else {
		lastLogIndex := len(n.log) - 1
		var lastLogTerm int
		if lastLogIndex >= 0 {
			lastLogTerm = n.log[lastLogIndex].Term
		} else {
			lastLogTerm = -1
		}
		upToDate := (args.LastLogTerm > lastLogTerm) ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
		if (n.votedFor == nil || *n.votedFor == args.CandidateID) && upToDate {
			n.votedFor = &args.CandidateID
			reply.VoteGranted = true
			n.resetElectionTimer()
		}
	}
	json.NewEncoder(w).Encode(reply)
}

func (n *Node) handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var args AppendEntriesArgs
	json.NewDecoder(r.Body).Decode(&args)

	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.term}
	if args.Term > n.term {
		n.convertToFollower(args.Term)
	}
	if args.Term < n.term {
		reply.Success = false
		json.NewEncoder(w).Encode(reply)
		return
	}
	// Valid leader or same term
	n.resetElectionTimer()
	n.leaderID = &args.LeaderID
	n.state = Follower

	// Check if log contains an entry at PrevLogIndex with PrevLogTerm
	if args.PrevLogIndex >= 0 {
		if args.PrevLogIndex >= len(n.log) {
			// Log doesn't contain an entry at PrevLogIndex
			reply.Success = false
			reply.ConflictIndex = len(n.log)
			reply.ConflictTerm = -1
			json.NewEncoder(w).Encode(reply)
			return
		}
		if n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			// Conflict in terms
			conflictingTerm := n.log[args.PrevLogIndex].Term
			reply.Success = false
			reply.ConflictTerm = conflictingTerm
			// Find the first index of the conflicting term
			conflictIndex := args.PrevLogIndex
			for conflictIndex > 0 && n.log[conflictIndex-1].Term == conflictingTerm {
				conflictIndex--
			}
			reply.ConflictIndex = conflictIndex
			json.NewEncoder(w).Encode(reply)
			return
		}
	}

	// Append new entries, overwriting conflicts
	index := args.PrevLogIndex + 1
	for i := 0; i < len(args.Entries); i++ {
		if index+i < len(n.log) {
			if n.log[index+i].Term != args.Entries[i].Term {
				// Delete the existing entry and all that follow it
				log.Printf("Deleting conflicting entry at index %d", index+i)
				n.log = n.log[:index+i]
				log.Printf("Appending %d entries instead", len(args.Entries[i:]))
				n.log = append(n.log, args.Entries[i:]...)
				break
			}
			// Entries are the same, no action needed
		} else {
			// Append any new entries not in the log
			log.Printf("Appending %d new entries", len(args.Entries[i:]))
			n.log = append(n.log, args.Entries[i:]...)
			break
		}
	}

	// Update commitIndex
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, len(n.log)-1)
		n.applyToStateMachine()
	}

	reply.Success = true
	json.NewEncoder(w).Encode(reply)
}

///////////////////////////////////////////////////////////////////////////
// HTTP Handlers for CRUD
///////////////////////////////////////////////////////////////////////////

func (n *Node) chooseReplicaForRead() string {
	// Choose a random replica that is not the leader
	var replicas []string
	for _, p := range n.peers {
		if p != n.address {
			replicas = append(replicas, p)
		}
	}
	if len(replicas) == 0 {
		return n.address
	}
	return replicas[randGenerator.Intn(len(replicas))]
}

func (n *Node) redirectToLeader(w http.ResponseWriter, r *http.Request) bool {
	n.mu.Lock()
	isLeader := (n.state == Leader)
	leaderID := n.leaderID
	n.mu.Unlock()

	if !isLeader || leaderID == nil || *leaderID != n.address {
		if leaderID != nil {
			http.Redirect(w, r, "http://"+*leaderID+"/resources", http.StatusFound)
			return true
		} else {
			http.Error(w, "no leader", http.StatusServiceUnavailable)
			return true
		}
	}
	return false
}

func (n *Node) handleCreate(w http.ResponseWriter, r *http.Request) {
	var content map[string]string
	if err := json.NewDecoder(r.Body).Decode(&content); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if n.redirectToLeader(w, r) {
		return
	}

	n.mu.Lock()
	cmd := n.store.GetCreateCommand(content)
	res, err := n.leaderAppendCommand(cmd)
	if err != nil {
		http.Error(w, "failed to apply", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(res))
}

func (n *Node) handlePut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	var content map[string]string
	if err := json.NewDecoder(r.Body).Decode(&content); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if n.redirectToLeader(w, r) {
		return
	}

	n.mu.Lock()
	cmd := n.store.GetPutCommand(id, content)
	res, err := n.leaderAppendCommand(cmd)
	if err != nil {
		http.Error(w, "failed to apply", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(res))
}

func (n *Node) handlePatch(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	var patchRequest struct {
		ExpectedContent map[string]string `json:"expected_content"`
		NewContent      map[string]string `json:"new_content"`
	}
	if err := json.NewDecoder(r.Body).Decode(&patchRequest); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	if n.redirectToLeader(w, r) {
		return
	}

	n.store.casMu.Lock()
	defer n.store.casMu.Unlock()

	n.mu.Lock()
	cmd := n.store.GetPatchCommand(id, patchRequest.ExpectedContent, patchRequest.NewContent)
	if cmd == nil {
		n.mu.Unlock()
		http.Error(w, "conflict", http.StatusConflict)
		return
	}

	res, err := n.leaderAppendCommand(cmd)
	if err != nil {
		http.Error(w, "failed to apply", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(res))
}

func (n *Node) handleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	if n.redirectToLeader(w, r) {
		return
	}

	n.mu.Lock()
	cmd := n.store.GetDeleteCommand(id)
	res, err := n.leaderAppendCommand(cmd)
	if err != nil {
		http.Error(w, "failed to apply", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(res))
}

func (n *Node) handleGet(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	n.mu.Lock()

	if n.state == Leader {
		replica := n.chooseReplicaForRead()
		n.mu.Unlock()
		http.Redirect(w, r, "http://"+replica+"/resources/"+id, http.StatusFound)
		return
	}

	res, ok := n.store.Get(id)
	n.mu.Unlock()
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

///////////////////////////////////////////////////////////////////////////
// main
///////////////////////////////////////////////////////////////////////////

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/raft") {
			log.Printf("Received %s request for %s from %s", r.Method, r.RequestURI, r.RemoteAddr)
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	// Command line arguments
	nodeID := flag.Int("id", 0, "Node ID")
	address := flag.String("address", "localhost:5030", "Node address")
	peersStr := flag.String("peers", "localhost:5030,localhost:5031,localhost:5032", "Comma-separated list of peer addresses")
	flag.Parse()

	peers := strings.Split(*peersStr, ",")
	node := NewNode(*nodeID, *address, peers)

	r := mux.NewRouter()

	// Raft endpoints
	r.HandleFunc("/raft/requestvote", node.handleRequestVote).Methods("POST")
	r.HandleFunc("/raft/appendentries", node.handleAppendEntries).Methods("POST")

	// CRUD endpoints
	r.HandleFunc("/resources", node.handleCreate).Methods("POST")
	r.HandleFunc("/resources/{id}", node.handlePut).Methods("PUT")
	r.HandleFunc("/resources/{id}", node.handlePatch).Methods("PATCH")
	r.HandleFunc("/resources/{id}", node.handleDelete).Methods("DELETE")
	r.HandleFunc("/resources/{id}", node.handleGet).Methods("GET")

	r.Use(loggingMiddleware)

	srv := &http.Server{
		Addr:    *address,
		Handler: r,
	}

	go node.run()

	log.Printf("Node listening on %s", srv.Addr)
	if err := srv.ListenAndServe(); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
		log.Printf("Server error: %v", err)
	}
}
