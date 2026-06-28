package main

import (
	"context"
	"testing"
	"time"
)

func TestWithCancelPreservesParentDeadline(t *testing.T) {
	t.Parallel()

	parent, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	child, childCancel := context.WithCancel(parent)
	defer childCancel()

	got, ok := child.Deadline()
	if !ok {
		t.Fatal("child deadline missing")
	}
	want, _ := parent.Deadline()
	if !got.Equal(want) {
		t.Fatalf("deadline = %v, want %v", got, want)
	}
}
