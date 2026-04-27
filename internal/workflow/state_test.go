package workflow

import "testing"

func TestTransition_ValidPaths(t *testing.T) {
	cases := []struct {
		from, to State
	}{
		{StatePending, StateRunning},
		{StatePending, StateCancelled},
		{StatePending, StateSkipped},
		{StateRunning, StateSucceeded},
		{StateRunning, StateFailed},
		{StateRunning, StateCancelled},
	}
	for _, tc := range cases {
		if _, err := transition(tc.from, tc.to); err != nil {
			t.Errorf("%s -> %s rejected: %v", tc.from, tc.to, err)
		}
	}
}

func TestTransition_InvalidPaths(t *testing.T) {
	cases := []struct {
		from, to State
	}{
		{StateSucceeded, StateRunning},
		{StateFailed, StateRunning},
		{StateCancelled, StateSucceeded},
		{StateSkipped, StateRunning},
		{StateRunning, StatePending},
		{StatePending, StateSucceeded}, // cannot skip Running
	}
	for _, tc := range cases {
		if _, err := transition(tc.from, tc.to); err == nil {
			t.Errorf("%s -> %s allowed but should be rejected", tc.from, tc.to)
		}
	}
}

func TestTransition_UnknownState(t *testing.T) {
	if _, err := transition(State("garbage"), StateRunning); err == nil {
		t.Fatal("expected error for unknown state")
	}
}
