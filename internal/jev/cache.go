package jev

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

// Cache deduplicates identical state evaluations within one CLI run: 301
// timeouts sharing a signature cost one Jev call, not 301. It is intentionally
// in-memory only — never persisted, so threshold tuning stays reproducible.
type Cache struct {
	mu   sync.Mutex
	seen map[string]*Response
	hits int
}

// NewCache returns an empty per-run cache.
func NewCache() *Cache {
	return &Cache{seen: make(map[string]*Response)}
}

// Key returns the stable cache key for a state string.
func Key(state string) string {
	h := sha256.Sum256([]byte(state))
	return hex.EncodeToString(h[:])[:16]
}

// Get returns the cached response for state, if any.
func (c *Cache) Get(state string) (*Response, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	r, ok := c.seen[Key(state)]
	return r, ok
}

// Put stores resp for state.
func (c *Cache) Put(state string, resp *Response) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen == nil {
		c.seen = make(map[string]*Response)
	}
	c.seen[Key(state)] = resp
}

// Hit records a cache hit for stats display (dry-run previews show
// "Jev calls: N (cached M)").
func (c *Cache) Hit() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hits++
}

// Hits returns the number of cache hits recorded.
func (c *Cache) Hits() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits
}

// Size returns the number of cached entries.
func (c *Cache) Size() int {
	if c == nil {
		return 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.seen)
}
