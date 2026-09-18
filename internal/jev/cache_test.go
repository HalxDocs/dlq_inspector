package jev

import (
	"testing"
)

func TestCacheRoundTrip(t *testing.T) {
	c := NewCache()
	if _, ok := c.Get("s"); ok {
		t.Fatal("empty cache hit")
	}
	c.Put("s", &Response{Model: "jev-1.13.0"})
	got, ok := c.Get("s")
	if !ok || got.Model != "jev-1.13.0" {
		t.Fatalf("want cached response, got %+v %v", got, ok)
	}
	if c.Size() != 1 {
		t.Errorf("size = %d, want 1", c.Size())
	}
	c.Hit()
	if c.Hits() != 1 {
		t.Errorf("hits = %d, want 1", c.Hits())
	}
}

func TestCacheNilSafe(t *testing.T) {
	var c *Cache
	if _, ok := c.Get("s"); ok {
		t.Fatal("nil cache hit")
	}
	c.Put("s", &Response{})
	c.Hit()
	if c.Hits() != 0 || c.Size() != 0 {
		t.Fatal("nil cache not safe")
	}
}

func TestKeyStable(t *testing.T) {
	if Key("a") != Key("a") || Key("a") == Key("b") {
		t.Fatal("cache keys not stable/unique")
	}
}
