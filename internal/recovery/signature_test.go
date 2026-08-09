package recovery

import "testing"

func TestNormalizeSignature(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "collapses IPs and ports",
			raw:  "timeout connecting to 10.0.4.5:6432 after 30000ms",
			want: "timeout connecting to {ip}:{port} after {n}",
		},
		{
			name: "same failure different hosts share a signature",
			raw:  "timeout connecting to 10.0.4.9:6432 after 30000ms",
			want: "timeout connecting to {ip}:{port} after {n}",
		},
		{
			name: "collapses UUIDs",
			raw:  "order 3f2b1c8e-9a44-4b0d-9f51-1a2b3c4d5e6f rejected",
			want: "order {uuid} rejected",
		},
		{
			name: "collapses ISO timestamps",
			raw:  "deadline exceeded at 2026-08-09T14:22:31Z",
			want: "deadline exceeded at {ts}",
		},
		{
			name: "collapses epoch seconds",
			raw:  "expired at 1754250000",
			want: "expired at {ts}",
		},
		{
			name: "collapses bare numbers",
			raw:  "payment for order 4821 failed with code 7",
			want: "payment for order {n} failed with code {n}",
		},
		{
			name: "collapses IPv6",
			raw:  "no route to 2001:db8::1",
			want: "no route to {ip}",
		},
		{
			name: "preserves error kind words",
			raw:  "validation failed: customer_id is required",
			want: "validation failed: customer_id is required",
		},
		{
			name: "empty becomes the no-failure sentinel",
			raw:  "   ",
			want: noFailureSignature,
		},
		{
			name: "does not mangle plain text",
			raw:  "connection refused by broker",
			want: "connection refused by broker",
		},
		{
			name: "collapses whitespace runs",
			raw:  "error   with    many    spaces",
			want: "error with many spaces",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := NormalizeSignature(tc.raw); got != tc.want {
				t.Errorf("NormalizeSignature(%q) = %q, want %q", tc.raw, got, tc.want)
			}
		})
	}
}

func TestGroupLabel(t *testing.T) {
	cases := []struct {
		signature string
		want      string
	}{
		{"timeout connecting to {ip}:{port} after {n}", "Timeout connecting to"},
		{"validation failed: customer_id is required", "Validation failed customer_id"},
		{"", "Unknown failure"},
		{"(no failure reason)", "Unknown failure"},
		{"payment declined", "Payment declined"},
	}
	for _, tc := range cases {
		if got := groupLabel(tc.signature); got != tc.want {
			t.Errorf("groupLabel(%q) = %q, want %q", tc.signature, got, tc.want)
		}
	}
}
