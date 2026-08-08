package broker

import (
	"fmt"
	"sort"
)

// Factory constructs a new adapter instance.
type Factory func() Broker

var registry = map[string]Factory{}

// Register adds an adapter factory under the given broker name. Adapters call
// this from their package init function.
func Register(name string, f Factory) {
	registry[name] = f
}

// New returns a fresh adapter instance for the named broker.
func New(name string) (Broker, error) {
	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown broker %q (registered: %v)", name, Registered())
	}
	return f(), nil
}

// Registered lists the registered broker names, sorted.
func Registered() []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
