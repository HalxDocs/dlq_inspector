package message

import (
	"bytes"
	"encoding/json"
)

// PrettyJSON returns the payload re-indented as JSON. Callers fall back to the
// raw bytes when it returns an error (non-JSON payload).
func PrettyJSON(payload []byte) (string, error) {
	var buf bytes.Buffer
	if err := json.Indent(&buf, payload, "", "  "); err != nil {
		return "", err
	}
	return buf.String(), nil
}
