package recovery

import (
	"testing"

	"github.com/HalxDocs/dlq_inspector/internal/message"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		msg  message.Message
		want Classification
	}{
		{
			name: "timeout is replayable",
			msg:  message.Message{ID: "m1", FailureReason: "timeout connecting to 10.0.4.5:6432"},
			want: Replayable,
		},
		{
			name: "5xx is replayable",
			msg:  message.Message{ID: "m2", FailureReason: "payment service returned 503"},
			want: Replayable,
		},
		{
			name: "connection refused is replayable",
			msg:  message.Message{ID: "m3", FailureReason: "connection refused by broker"},
			want: Replayable,
		},
		{
			name: "validation needs a fix",
			msg:  message.Message{ID: "m4", FailureReason: "validation failed: customer_id is required"},
			want: RequiresFix,
		},
		{
			name: "4xx needs a fix",
			msg:  message.Message{ID: "m5", FailureReason: "order service returned 422"},
			want: RequiresFix,
		},
		{
			name: "not found needs a fix",
			msg:  message.Message{ID: "m6", FailureReason: "account not found"},
			want: RequiresFix,
		},
		{
			name: "duplicate must not replay",
			msg:  message.Message{ID: "m7", FailureReason: "duplicate event already processed"},
			want: DoNotReplay,
		},
		{
			name: "idempotency hit must not replay",
			msg:  message.Message{ID: "m8", FailureReason: "idempotency key already seen"},
			want: DoNotReplay,
		},
		{
			name: "no failure reason is investigate",
			msg:  message.Message{ID: "m9"},
			want: Investigate,
		},
		{
			name: "unknown failure text is investigate",
			msg:  message.Message{ID: "m10", FailureReason: "the flux capacitor demagnetized"},
			want: Investigate,
		},
		{
			name: "conflicting signals are investigate",
			msg:  message.Message{ID: "m11", FailureReason: "invalid connection refused"},
			want: Investigate,
		},
		{
			name: "high retries on transient-looking failure are investigate",
			msg:  message.Message{ID: "m12", FailureReason: "timeout reaching warehouse", RetryCount: 9},
			want: Investigate,
		},
		{
			name: "low retries on transient failure stay replayable",
			msg:  message.Message{ID: "m13", FailureReason: "timeout reaching warehouse", RetryCount: 1},
			want: Replayable,
		},
		{
			name: "duplicate wins over transient",
			msg:  message.Message{ID: "m14", FailureReason: "duplicate event timeout"},
			want: DoNotReplay,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := Classify(&tc.msg)
			if got.Classification != tc.want {
				t.Errorf("Classify(%q) = %s (%s), want %s", tc.msg.FailureReason, got.Classification, got.Reason, tc.want)
			}
			if got.Confidence <= 0 || got.Confidence > 1 {
				t.Errorf("confidence %v out of range", got.Confidence)
			}
			if got.MessageID != tc.msg.ID {
				t.Errorf("result MessageID = %q, want %q", got.MessageID, tc.msg.ID)
			}
		})
	}
}

func TestClassifyInvestigateIsDefault(t *testing.T) {
	// The honest-default contract: anything ambiguous lands on INVESTIGATE,
	// never on REPLAYABLE by assumption.
	for _, reason := range []string{"", "mystery failure", "error code 0xDEAD"} {
		got := Classify(&message.Message{FailureReason: reason})
		if got.Classification != Investigate {
			t.Errorf("reason %q: got %s, want INVESTIGATE", reason, got.Classification)
		}
	}
}

func TestClassifyReasonIsPopulated(t *testing.T) {
	for _, reason := range []string{"timeout", "invalid input", "already processed", ""} {
		got := Classify(&message.Message{FailureReason: reason})
		if got.Reason == "" {
			t.Errorf("reason %q: empty classification reason", reason)
		}
	}
}
