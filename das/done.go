package das

import (
	"context"
	"fmt"
)

type done struct {
	name     string
	finished chan struct{}
}

func newDone(name string) done {
	return done{
		name:     name,
		finished: make(chan struct{}),
	}
}

func (sm *done) indicateDone() {
	close(sm.finished)
}

func (sm *done) wait(ctx context.Context) error {
	select {
	case <-sm.finished:
	case <-ctx.Done():
		return fmt.Errorf("%v stuck: %w", sm.name, ctx.Err())
	}
	return nil
}
