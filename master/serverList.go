package master

import (
	"container/heap"
)

// An Item is something we manage in a priority queue.
type Server struct {
	Server    string // The value of the item; arbitrary.
	NumberOfChunks int    // The priority of the item in the queue.
	// The index is needed by update and is maintained by the heap.Interface methods.
	index int // The index of the item in the heap.
}

// A PriorityQueue implements heap.Interface and holds Items.
type ServerList []*Server

func (pq ServerList) Len() int { return len(pq) }

func (pq ServerList) Less(i, j int) bool {
	// We want Pop to give us the highest, not lowest, priority so we use greater than here.
	return pq[i].NumberOfChunks < pq[j].NumberOfChunks
}

func (pq ServerList) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *ServerList) Push(x any) {
	n := len(*pq)
	item := x.(*Server)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *ServerList) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil  // don't stop the GC from reclaiming the item eventually
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// update modifies the server address and the number of chunks of a Server in the list.
// It also reorders the heap to maintain the correct priority order.
func (pq *ServerList) update(server *Server, newNumberOfChunks int) {

	server.NumberOfChunks = newNumberOfChunks

	// Reorder the heap based on the updated number of chunks
	heap.Fix(pq, server.index)
}