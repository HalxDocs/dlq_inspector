package command

import (
	"context"
	"fmt"
	"time"

	"github.com/HalxDocs/dlq_inspector/internal/broker"
	_ "github.com/HalxDocs/dlq_inspector/internal/broker/rabbitmq" // register adapter factories
	"github.com/HalxDocs/dlq_inspector/internal/config"
)

// brokerFactory is the seam commands use to construct adapters. Tests override
// it with an in-memory fake so command behavior is testable without a broker.
var brokerFactory = func(name string) (broker.Broker, error) {
	return broker.New(name)
}

// openBroker resolves the active profile and returns a connected broker plus
// the profile itself. Callers must defer b.Close().
func openBroker(ctx context.Context, opts *GlobalOptions) (broker.Broker, *config.Profile, error) {
	cfg, err := config.Load(opts.ConfigPath)
	if err != nil {
		return nil, nil, err
	}
	profile, err := cfg.Profile(opts.Profile)
	if err != nil {
		return nil, nil, err
	}
	url, err := profile.ResolveURL()
	if err != nil {
		return nil, nil, err
	}
	b, err := brokerFactory(profile.Broker)
	if err != nil {
		return nil, nil, err
	}
	if err := b.Connect(ctx, broker.ConnectionConfig{
		Broker:        profile.Broker,
		URL:           url,
		Queue:         profile.DefaultQueue,
		ManagementURL: profile.ManagementURL,
	}); err != nil {
		return nil, nil, err
	}
	return b, profile, nil
}

// commandContext applies a connect/operation timeout to a command's context.
func commandContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, 15*time.Second)
}

// resolveQueue picks the queue from the command argument, falling back to the
// profile's default_queue.
func resolveQueue(args []string, defaultQueue string) (string, error) {
	if len(args) == 1 {
		return args[0], nil
	}
	if defaultQueue != "" {
		return defaultQueue, nil
	}
	return "", fmt.Errorf("no queue given and the profile has no default_queue set")
}
