// Package jev is an optional, opt-in classifier backend built on TypeSafe AI's
// Jev System One model: unstructured state in, typed probabilistic decisions
// out. It never mutates queues, never overrides policy, and never breaks
// offline use — every caller falls back to the rule-based classifier when
// Jev is unavailable or unsure.
package jev

// DefaultEndpoint is the TypeSafe System One evaluation endpoint.
const DefaultEndpoint = "https://api.typesafe.ai/v1/systemone"

// DefaultModel is the stable alias callers use unless a version is pinned.
// Threshold-tuned workflows should pin an explicit version (e.g. jev-1.13.0)
// instead of floating on this alias.
const DefaultModel = "jev-latest"

// APIKeyEnv is the only place the client reads a secret from. The key is
// never stored in config.yaml or a connection profile.
const APIKeyEnv = "TYPESAFE_API_KEY"

// DefaultTimeoutSeconds bounds one evaluation call. Jev typically answers in
// 70-500ms; anything slower is treated as unavailable and the caller falls
// back to rules.
const DefaultTimeoutSeconds = 10
