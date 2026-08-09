package message

import (
	"encoding/json"
	"strings"
)

// Redactor masks configured sensitive payload fields (emails, card numbers,
// SSNs, ...) before payloads are rendered or exported. Redaction is on by
// default; only an explicit --show-sensitive flag disables it.
type Redactor struct {
	// Fields are dotted payload paths to mask, e.g. "customer.email".
	Fields []string
	// Mask is the replacement text; defaults to "[REDACTED]".
	Mask string
}

func (r Redactor) mask() string {
	if r.Mask == "" {
		return "[REDACTED]"
	}
	return r.Mask
}

// Apply replaces the leaf value at each configured field path with the mask.
// Non-JSON payloads and missing fields pass through unchanged.
func (r Redactor) Apply(payload []byte) []byte {
	if len(r.Fields) == 0 {
		return payload
	}
	var doc any
	if err := json.Unmarshal(payload, &doc); err != nil {
		return payload
	}
	for _, path := range r.Fields {
		redactPath(doc, strings.Split(path, "."), r.mask())
	}
	out, err := json.Marshal(doc)
	if err != nil {
		return payload
	}
	return out
}

// redactPath walks parts through the document, masking the leaf value.
func redactPath(doc any, parts []string, mask string) {
	if len(parts) == 0 {
		return
	}
	obj, ok := doc.(map[string]any)
	if !ok {
		return
	}
	key := parts[0]
	val, ok := obj[key]
	if !ok {
		return
	}
	if len(parts) == 1 {
		obj[key] = mask
		return
	}
	redactPath(val, parts[1:], mask)
}
