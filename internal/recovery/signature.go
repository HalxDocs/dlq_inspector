package recovery

import (
	"regexp"
	"strings"
)

// Error-signature normalization turns raw failure strings into stable
// signatures by collapsing dynamic values — IPs, ports, UUIDs, timestamps,
// and bare numbers — into placeholders. Two messages that failed for the same
// reason usually differ only in those values ("timeout connecting to
// 10.0.4.5:6432" vs "timeout connecting to 10.0.4.9:6432"); after
// normalization they share one signature and cluster into one failure group.
//
// Order matters: specific shapes (timestamps, UUIDs, IPs, ports) are replaced
// before the generic number collapse, so "10.0.4.5:6432" becomes
// "{ip}:{port}", never "{n}.{n}.{n}.{n}:{n}". ISO timestamps run before the
// IPv6 rule because "14:22:31" (a clock time) is indistinguishable from
// colon-separated hex — so the IPv6 rule only matches unambiguous shapes
// (containing "::", or at least four hex groups).

var (
	isoTsRE = regexp.MustCompile(`\b\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?\b`)
	epochRE = regexp.MustCompile(`\b(?:1[5-9]\d{8}|1[6-9]\d{11})\b`) // epoch seconds / millis since ~2000
	uuidRE  = regexp.MustCompile(`\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b`)
	ipv4RE  = regexp.MustCompile(`\b\d{1,3}(?:\.\d{1,3}){3}\b`)
	// The compressed form keeps "::" intact: a trailing-colon group would
	// consume the first of the two colons in "2001:db8::1".
	ipv6CmpRE  = regexp.MustCompile(`\b(?:[0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){0,6}::[0-9a-fA-F]{1,4}|::[0-9a-fA-F]{1,4})\b`)
	ipv6FullRE = regexp.MustCompile(`\b[0-9a-fA-F]{1,4}(?::[0-9a-fA-F]{1,4}){3,7}\b`) // >= 4 groups, unambiguous vs clock times
	portRE     = regexp.MustCompile(`:\d{2,5}\b`)
	numberRE   = regexp.MustCompile(`\b\d+(?:\.\d+)?[a-zA-Z%]*\b`)
	spaceRE    = regexp.MustCompile(`\s+`)
	bracesRE   = regexp.MustCompile(`\{\w+\}`)
	punctRE    = regexp.MustCompile(`[^\w\s]`)
)

// noFailureSignature is the signature assigned to messages that carry no
// failure reason at all — a distinct group from every real failure.
const noFailureSignature = "(no failure reason)"

// NormalizeSignature collapses the dynamic values in raw into a stable
// signature suitable for clustering. It is case-preserving (only values are
// collapsed, not words) so distinct error kinds stay distinct.
func NormalizeSignature(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return noFailureSignature
	}
	s = isoTsRE.ReplaceAllString(s, "{ts}")
	s = epochRE.ReplaceAllString(s, "{ts}")
	s = uuidRE.ReplaceAllString(s, "{uuid}")
	s = ipv4RE.ReplaceAllString(s, "{ip}")
	s = ipv6CmpRE.ReplaceAllString(s, "{ip}")
	s = ipv6FullRE.ReplaceAllString(s, "{ip}")
	s = portRE.ReplaceAllString(s, ":{port}")
	s = numberRE.ReplaceAllString(s, "{n}")
	s = spaceRE.ReplaceAllString(s, " ")
	return strings.TrimSpace(s)
}

// groupLabel derives a short human-readable label from a normalized
// signature, e.g. "timeout connecting to {ip}:{port}" -> "Timeout connecting
// to". It is used for the group's display name; the full signature stays
// available on the group for exact matching.
func groupLabel(signature string) string {
	if signature == "" || signature == noFailureSignature {
		return "Unknown failure"
	}
	s := bracesRE.ReplaceAllString(signature, " ")
	s = punctRE.ReplaceAllString(s, " ")
	words := strings.Fields(s)
	if len(words) == 0 {
		return "Unknown failure"
	}
	if len(words) > 3 {
		words = words[:3]
	}
	label := strings.Join(words, " ")
	return strings.ToUpper(label[:1]) + label[1:]
}
