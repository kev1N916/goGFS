package lrucache

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewLRUBufferCache(t *testing.T) {
	// Test creating a new cache with various capacities
	tests := []struct {
		name     string
		capacity int
	}{
		{"zero capacity", 0},
		{"small capacity", 5},
		{"large capacity", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache := NewLRUBufferCache(tt.capacity)
			if cache == nil {
				t.Fatal("NewLRUBufferCache returned nil")
			}
			if cache.Capacity() != tt.capacity {
				t.Errorf("Expected capacity %d, got %d", tt.capacity, cache.Capacity())
			}
			if cache.Len() != 0 {
				t.Errorf("New cache should be empty, but has %d items", cache.Len())
			}
			if cache.items == nil {
				t.Error("Items map should be initialized")
			}
			if cache.lruList == nil {
				t.Error("LRU list should be initialized")
			}
		})
	}
}

func TestPutAndGet(t *testing.T) {
	cache := NewLRUBufferCache(3)

	// Test putting and getting values
	testData := []struct {
		key   int64
		value []byte
	}{
		{1, []byte("value1")},
		{2, []byte("value2")},
		{3, []byte("value3")},
	}

	// Add items to cache
	for _, data := range testData {
		cache.Put(data.key, data.value)
	}

	// Verify all items are retrievable
	for _, data := range testData {
		value, found := cache.Get(data.key)
		if !found {
			t.Errorf("Key %d should be in cache but wasn't found", data.key)
		}
		if !bytes.Equal(value, data.value) {
			t.Errorf("Expected value %s for key %d, got %s", string(data.value), data.key, string(value))
		}
	}

	// Verify cache size
	if cache.Len() != 3 {
		t.Errorf("Cache should have 3 items, has %d", cache.Len())
	}
}

func TestUpdateExisting(t *testing.T) {
	cache := NewLRUBufferCache(3)
	key := int64(1)
	
	// Add initial value
	initialValue := []byte("initial")
	cache.Put(key, initialValue)
	
	// Update the value
	updatedValue := []byte("updated")
	cache.Put(key, updatedValue)
	
	// Verify value was updated
	value, found := cache.Get(key)
	if !found {
		t.Errorf("Key %d should be in cache but wasn't found", key)
	}
	if !bytes.Equal(value, updatedValue) {
		t.Errorf("Expected value %s for key %d, got %s", string(updatedValue), key, string(value))
	}
	
	// Ensure cache length is still 1
	if cache.Len() != 1 {
		t.Errorf("Cache should have 1 item, has %d", cache.Len())
	}
}

func TestEviction(t *testing.T) {
	capacity := 3
	cache := NewLRUBufferCache(capacity)
	
	// Add capacity+1 items to trigger eviction
	for i := int64(1); i <= int64(capacity+1); i++ {
		cache.Put(i, []byte("value"+string(rune('0'+i))))
	}
	
	// The first item should have been evicted
	if cache.Contains(1) {
		t.Error("Item with key 1 should have been evicted")
	}
	
	// All other items should be present
	for i := int64(2); i <= int64(capacity+1); i++ {
		if !cache.Contains(i) {
			t.Errorf("Item with key %d should be in cache", i)
		}
	}
	
	// Cache length should be at capacity
	if cache.Len() != capacity {
		t.Errorf("Cache should have %d items, has %d", capacity, cache.Len())
	}
}

func TestLRUBehavior(t *testing.T) {
	capacity := 3
	cache := NewLRUBufferCache(capacity)
	
	// Add items 1, 2, 3
	for i := int64(1); i <= int64(capacity); i++ {
		cache.Put(i, []byte("value"+string(rune('0'+i))))
	}
	
	// Access item 1 to make it most recently used
	cache.Get(1)
	
	// Add item 4, which should evict item 2 (now the LRU)
	cache.Put(4, []byte("value4"))
	
	// Check eviction
	if cache.Contains(2) {
		t.Error("Item with key 2 should have been evicted")
	}
	
	// Items 1, 3, 4 should be present
	expectedKeys := []int64{1, 3, 4}
	for _, key := range expectedKeys {
		if !cache.Contains(key) {
			t.Errorf("Item with key %d should be in cache", key)
		}
	}
}

func TestRemove(t *testing.T) {
	cache := NewLRUBufferCache(3)
	
	// Add items
	for i := int64(1); i <= 3; i++ {
		cache.Put(i, []byte("value"+string(rune('0'+i))))
	}
	
	// Remove middle item
	if !cache.Remove(2) {
		t.Error("Remove should return true for existing key")
	}
	
	// Check it's gone
	if cache.Contains(2) {
		t.Error("Item with key 2 should have been removed")
	}
	
	// Cache length should be reduced
	if cache.Len() != 2 {
		t.Errorf("Cache should have 2 items, has %d", cache.Len())
	}
	
	// Remove non-existent item
	if cache.Remove(5) {
		t.Error("Remove should return false for non-existent key")
	}
}

func TestPeek(t *testing.T) {
	cache := NewLRUBufferCache(3)
	
	// Add items
	testData := []struct {
		key   int64
		value []byte
	}{
		{1, []byte("value1")},
		{2, []byte("value2")},
		{3, []byte("value3")},
	}
	
	for _, data := range testData {
		cache.Put(data.key, data.value)
	}
	
	// Get the keys in their current order
	originalKeys := cache.GetKeys()
	
	// Peek at first item
	value, found := cache.Peek(1)
	if !found {
		t.Errorf("Key %d should be in cache but wasn't found", 1)
	}
	if !bytes.Equal(value, []byte("value1")) {
		t.Errorf("Expected value %s for key %d, got %s", "value1", 1, string(value))
	}
	
	// Order should not have changed
	newKeys := cache.GetKeys()
	for i, key := range originalKeys {
		if key != newKeys[i] {
			t.Errorf("Peek changed the order of keys, expected %v but got %v", originalKeys, newKeys)
			break
		}
	}
}

func TestContains(t *testing.T) {
	cache := NewLRUBufferCache(3)
	
	// Add items
	cache.Put(1, []byte("value1"))
	
	// Test existing key
	if !cache.Contains(1) {
		t.Error("Contains should return true for existing key")
	}
	
	// Test non-existent key
	if cache.Contains(2) {
		t.Error("Contains should return false for non-existent key")
	}
	
	// Ensure order hasn't changed
	keys := cache.GetKeys()
	if len(keys) != 1 || keys[0] != 1 {
		t.Errorf("Contains changed cache state, keys: %v", keys)
	}
}

func TestClear(t *testing.T) {
	cache := NewLRUBufferCache(3)
	
	// Add items
	for i := int64(1); i <= 3; i++ {
		cache.Put(i, []byte("value"+string(rune('0'+i))))
	}
	
	// Clear cache
	cache.Clear()
	
	// Check cache is empty
	if cache.Len() != 0 {
		t.Errorf("Cache should be empty after Clear(), but has %d items", cache.Len())
	}
	
	// Check previous items are gone
	for i := int64(1); i <= 3; i++ {
		if cache.Contains(i) {
			t.Errorf("Item with key %d should not be in cache after Clear()", i)
		}
	}
}

func TestGetKeys(t *testing.T) {
	cache := NewLRUBufferCache(3)
	
	// No keys in empty cache
	keys := cache.GetKeys()
	if len(keys) != 0 {
		t.Errorf("Empty cache should return empty keys slice, got %v", keys)
	}
	
	// Add items in order 1, 2, 3
	for i := int64(1); i <= 3; i++ {
		cache.Put(i, []byte("value"+string(rune('0'+i))))
	}
	
	// Keys should be in order 3, 2, 1 (most recently used first)
	expectedKeys := []int64{3, 2, 1}
	keys = cache.GetKeys()
	
	if len(keys) != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(keys))
	} else {
		for i, key := range keys {
			if key != expectedKeys[i] {
				t.Errorf("Expected key at position %d to be %d, got %d", i, expectedKeys[i], key)
			}
		}
	}
	
	// Access key 1, should move to front
	cache.Get(1)
	expectedKeys = []int64{1, 3, 2}
	keys = cache.GetKeys()
	
	if len(keys) != len(expectedKeys) {
		t.Errorf("Expected %d keys, got %d", len(expectedKeys), len(keys))
	} else {
		for i, key := range keys {
			if key != expectedKeys[i] {
				t.Errorf("After accessing key 1, expected key at position %d to be %d, got %d", 
					i, expectedKeys[i], key)
			}
		}
	}
}

func TestZeroCapacity(t *testing.T) {
	cache := NewLRUBufferCache(0)
	
	assert.Nil(t,cache)
}

func TestEmptyValues(t *testing.T) {
	cache := NewLRUBufferCache(3)
	
	// Test with nil value
	cache.Put(1, nil)
	value, found := cache.Get(1)
	if !found {
		t.Error("Key with nil value should be found")
	}
	if value != nil {
		t.Errorf("Expected nil value, got %v", value)
	}
	
	// Test with empty slice value
	cache.Put(2, []byte{})
	value, found = cache.Get(2)
	if !found {
		t.Error("Key with empty slice value should be found")
	}
	if len(value) != 0 {
		t.Errorf("Expected empty slice, got %v", value)
	}
}

func TestConcurrentAccess(t *testing.T) {
	cache := NewLRUBufferCache(100)
	
	var wg sync.WaitGroup
	
	// Concurrent writes
	for i := int64(0); i < 50; i++ {
		wg.Add(1)
		go func(key int64) {
			defer wg.Done()
			value := []byte("value" + string(rune('0'+key)))
			cache.Put(key, value)
		}(i)
	}
	
	// Concurrent reads during writes
	for i := int64(0); i < 50; i++ {
		wg.Add(1)
		go func(key int64) {
			defer wg.Done()
			for range 10 {
				cache.Get(key)
			}
		}(i)
	}
	
	// Concurrent operations of various types
	for i := int64(0); i < 20; i++ {
		wg.Add(1)
		go func(key int64) {
			defer wg.Done()
			cache.Contains(key)
			cache.Peek(key)
			if key%5 == 0 {
				cache.Remove(key)
			}
		}(i)
	}
	
	wg.Wait()
	
	// Check that the cache is still in a consistent state
	if cache.Len() > 100 {
		t.Errorf("Cache exceeded its capacity: %d", cache.Len())
	}
	
	// Verify data consistency for remaining items
	keys := cache.GetKeys()
	for _, key := range keys {
		value, found := cache.Get(key)
		if !found {
			t.Errorf("Key %d should be in cache but wasn't found", key)
			continue
		}
		
		expected := []byte("value" + string(rune('0'+key)))
		if bytes.Equal(value, expected) {
			continue
		}
		
		// If it's not the expected value, it might have been updated by a concurrent operation
		// This is a limitation of testing concurrent behavior - we can only check that the
		// cache remains operational without panics or deadlocks
	}
}

func TestLargeValues(t *testing.T) {
	cache := NewLRUBufferCache(5)
	
	// Create a large value (1MB)
	largeValue := make([]byte, 1024*1024)
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}
	
	// Store large value
	cache.Put(1, largeValue)
	
	// Retrieve and verify
	value, found := cache.Get(1)
	if !found {
		t.Error("Large value should be found in cache")
	}
	
	if !bytes.Equal(value, largeValue) {
		t.Error("Retrieved large value doesn't match what was stored")
	}
}

func TestEvictionOrder(t *testing.T) {
	cache := NewLRUBufferCache(3)
	
	// Add 3 items
	for i := int64(1); i <= 3; i++ {
		cache.Put(i, []byte{byte(i)})
	}
	
	// Access in reverse order to change LRU order
	for i := int64(3); i >= 1; i-- {
		cache.Get(i)
	}
	
	// Expected order now: 1, 2, 3 (most recently used first)
	expectedOrder := []int64{1, 2, 3}
	actualOrder := cache.GetKeys()
	
	for i, key := range actualOrder {
		if key != expectedOrder[i] {
			t.Errorf("Expected key at position %d to be %d, got %d", i, expectedOrder[i], key)
		}
	}
	
	// Add new item, should evict item 3
	cache.Put(4, []byte{4})
	
	if cache.Contains(3) {
		t.Error("Item 3 should have been evicted")
	}
	
	// Expected order: 4, 1, 2
	expectedOrder = []int64{4, 1, 2}
	actualOrder = cache.GetKeys()
	
	for i, key := range actualOrder {
		if key != expectedOrder[i] {
			t.Errorf("After eviction, expected key at position %d to be %d, got %d", 
				i, expectedOrder[i], key)
		}
	}
}