package master

import (
	"container/heap"
	"testing"
)

func TestPriorityQueueInitialization(t *testing.T) {
	pq := &ServerList{}
	heap.Init(pq)

	if pq.Len() != 0 {
		t.Errorf("Expected empty priority queue, got length %d", pq.Len())
	}
}

func TestPriorityQueuePushAndPop(t *testing.T) {
	pq := &ServerList{}
	heap.Init(pq)

	// Add servers with different chunk counts
	servers := []*Server{
		{server: "server1", NumberOfChunks: 5},
		{server: "server2", NumberOfChunks: 10},
		{server: "server3", NumberOfChunks: 3},
	}

	for _, server := range servers {
		heap.Push(pq, server)
	}

	// Check length
	if pq.Len() != 3 {
		t.Errorf("Expected priority queue length of 3, got %d", pq.Len())
	}

	// Since we're using a min heap (based on the Less function), 
	// we should get items in ascending order of NumberOfChunks
	expectedOrder := []string{"server3", "server1", "server2"}
	
	for i, expected := range expectedOrder {
		item := heap.Pop(pq).(*Server)
		if item.server != expected {
			t.Errorf("Pop %d: expected server %s, got %s", i, expected, item.server)
		}
	}

	// Queue should be empty now
	if pq.Len() != 0 {
		t.Errorf("Expected empty priority queue after pops, got length %d", pq.Len())
	}
}

func TestPriorityQueueUpdate(t *testing.T) {
	pq := &ServerList{}
	heap.Init(pq)

	// Add servers
	server1 := &Server{server: "server1", NumberOfChunks: 5}
	server2 := &Server{server: "server2", NumberOfChunks: 10}
	server3 := &Server{server: "server3", NumberOfChunks: 3}

	heap.Push(pq, server1)
	heap.Push(pq, server2)
	heap.Push(pq, server3)

	// Update server3 to have the least chunks
	pq.update(server2, 1)

	// First pop should now be server2
	item := heap.Pop(pq).(*Server)
	if item.server != "server2" || item.NumberOfChunks != 1 {
		t.Errorf("Expected server2 with 1 chunks, got %s with %d chunks", 
			item.server, item.NumberOfChunks)
	}

	// Second pop should be server3
	item = heap.Pop(pq).(*Server)
	if item.server != "server3" {
		t.Errorf("Expected server2, got %s", item.server)
	}

	// Update server1 before popping
	pq.update(server1, 20)
	
	// Last item should be server1 with updated value
	item = heap.Pop(pq).(*Server)
	if item.server != "server1" || item.NumberOfChunks != 20 {
		t.Errorf("Expected server1 with 20 chunks, got %s with %d chunks", 
			item.server, item.NumberOfChunks)
	}
}

func TestPriorityQueueEdgeCases(t *testing.T) {
	pq := &ServerList{}
	heap.Init(pq)

	// Test with identical priorities
	server1 := &Server{server: "server1", NumberOfChunks: 5}
	server2 := &Server{server: "server2", NumberOfChunks: 5}
	
	heap.Push(pq, server1)
	heap.Push(pq, server2)
	
	// The implementation should maintain stable ordering for equal priorities
	item1 := heap.Pop(pq).(*Server)
	item2 := heap.Pop(pq).(*Server)
	
	// We don't assert specific order here since heap doesn't guarantee stable sort
	// Just check both items were retrieved
	if (item1.server != "server1" && item1.server != "server2") ||
	   (item2.server != "server1" && item2.server != "server2") ||
	   (item1.server == item2.server) {
		t.Errorf("Unexpected items returned: %s and %s", item1.server, item2.server)
	}
}

func TestPriorityQueueStress(t *testing.T) {
	pq := &ServerList{}
	heap.Init(pq)

	// Add a large number of servers
	for i := 0; i < 100; i++ {
		heap.Push(pq, &Server{
			server:         "server" + string(rune(i+'0')),
			NumberOfChunks: i,
		})
	}

	// Pop them all and verify they come out in descending order of NumberOfChunks
	prev := 0 // arbitrary high value
	for pq.Len() > 0 {
		item := heap.Pop(pq).(*Server)
		if item.NumberOfChunks < prev {
			t.Errorf("Heap order violation: %d > %d", item.NumberOfChunks, prev)
		}
		prev = item.NumberOfChunks
	}
}

func TestPriorityQueueSequence(t *testing.T) {
	pq := &ServerList{}
	heap.Init(pq)

	// Sequence of operations
	server1 := &Server{server: "server1", NumberOfChunks: 5}
	server2 := &Server{server: "server2", NumberOfChunks: 10}
	
	heap.Push(pq, server1)
	heap.Push(pq, server2)
	
	// Top should be server1 with 5 chunks
	top := heap.Pop(pq).(*Server)
	if top.server != "server1" {
		t.Errorf("Expected server1, got %s", top.server)
	}
	
	// Push it back with fewer chunks
	top.NumberOfChunks = 2
	heap.Push(pq, top)
	
	// Update server1 to have more chunks
	pq.update(server1, 7)
	
	// Now server1 should be on top
	top = heap.Pop(pq).(*Server)
	if top.server != "server1" || top.NumberOfChunks != 7 {
		t.Errorf("Expected server1 with 7 chunks, got %s with %d chunks", 
			top.server, top.NumberOfChunks)
	}
}
