package lrucache

import (
	"container/list"
	"sync"
)

type Item struct {
	Key int64
	Value []byte
}

// LRUBufferCache implements a thread-safe Least Recently Used cache
type LRUBufferCache struct {
	capacity int
	items    map[int64]*list.Element
	lruList  *list.List
	mutex    sync.RWMutex
}

// NewLRUBufferCache creates a new LRU cache with the given capacity
func NewLRUBufferCache(capacity int) (*LRUBufferCache) {
	if(capacity<=0){
		return nil
	}
	return &LRUBufferCache{
		capacity: capacity,
		items:    make(map[int64]*list.Element),
		lruList:  list.New(),
	}
}

// Get retrieves an item from the cache by key
// Returns the value and a boolean indicating if the key was found
func (c *LRUBufferCache) Get(key int64) ([]byte, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if element, found := c.items[key]; found {
		// Move to front (most recently used)
		c.lruList.MoveToFront(element)
		return element.Value.(*Item).Value, true
	}
	return nil, false
}

// Put adds or updates an item in the cache
func (c *LRUBufferCache) Put(key int64, value []byte) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// If key exists, update value and move to front
	if element, found := c.items[key]; found {
		c.lruList.MoveToFront(element)
		element.Value.(*Item).Value = value
		return
	}

	// If we're at capacity, remove least recently used item
	if c.lruList.Len() >= c.capacity {
		c.evictOldest()
	}

	// Add new item to the front of the list
	element := c.lruList.PushFront(&Item{
		Key:   key,
		Value: value,
	})
	c.items[key] = element
}

// Remove explicitly removes an item from the cache
func (c *LRUBufferCache) Remove(key int64) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if element, found := c.items[key]; found {
		c.lruList.Remove(element)
		delete(c.items, key)
		return true
	}
	return false
}

// Peek retrieves an item's value without changing its position in the LRU list
func (c *LRUBufferCache) Peek(key int64) ([]byte, bool) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	if element, found := c.items[key]; found {
		return element.Value.(*Item).Value, true
	}
	return nil, false
}

// Contains checks if a key exists in the cache without changing its LRU position
func (c *LRUBufferCache) Contains(key int64) bool {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	_, found := c.items[key]
	return found
}

// evictOldest removes the least recently used item from the cache
func (c *LRUBufferCache) evictOldest() {
	if element := c.lruList.Back(); element != nil {
		item := element.Value.(*Item)
		delete(c.items, item.Key)
		c.lruList.Remove(element)
	}
}

// Clear removes all items from the cache
func (c *LRUBufferCache) Clear() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.lruList.Init()
	c.items = make(map[int64]*list.Element)
}

// Len returns the current number of items in the cache
func (c *LRUBufferCache) Len() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	return c.lruList.Len()
}

// Capacity returns the maximum capacity of the cache
func (c *LRUBufferCache) Capacity() int {
	return c.capacity
}

// GetKeys returns all keys in the cache in order of most to least recently used
func (c *LRUBufferCache) GetKeys() []int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	keys := make([]int64, 0, c.lruList.Len())
	for element := c.lruList.Front(); element != nil; element = element.Next() {
		keys = append(keys, element.Value.(*Item).Key)
	}
	return keys
}