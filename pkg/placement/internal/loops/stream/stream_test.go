/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stream

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/dapr/pkg/placement/internal/loops"
	v1pb "github.com/dapr/dapr/pkg/proto/placement/v1"
	"github.com/dapr/kit/events/loop/fake"
)

type fakeChannel struct {
	v1pb.Placement_ReportDaprStatusServer
	sendErr error
}

func (f *fakeChannel) Send(*v1pb.PlacementOrder) error { return f.sendErr }

type testStream struct {
	*stream
	enqueued     []loops.EventNamespace
	cancelCalls  int
	cancelCauses []error
}

func newTestStream(t *testing.T, sendErr error) *testStream {
	t.Helper()

	ts := new(testStream)
	ts.stream = &stream{
		idx:     7,
		ns:      "default",
		addr:    "10.0.0.1:1234",
		channel: &fakeChannel{sendErr: sendErr},
		nsLoop: fake.New[loops.EventNamespace]().
			WithEnqueue(func(e loops.EventNamespace) { ts.enqueued = append(ts.enqueued, e) }),
		cancel: func(cause error) {
			ts.cancelCalls++
			ts.cancelCauses = append(ts.cancelCauses, cause)
		},
	}

	return ts
}

// A send failure must cancel the stream context so recvLoop unwinds and
// reports the close exactly once. Enqueueing ConnCloseStream here as well
// would double-count the disconnect in namespaces.handleCloseStream.
func TestHandleSendFailureCancelsWithoutEnqueueingClose(t *testing.T) {
	ts := newTestStream(t, errors.New("send failed"))

	require.NoError(t, ts.Handle(t.Context(), &loops.DisseminateLock{Version: 1}))

	assert.Empty(t, ts.enqueued)
	require.Equal(t, 1, ts.cancelCalls)
	require.Error(t, ts.cancelCauses[0])
	assert.Equal(t, "send failed", ts.cancelCauses[0].Error())
}

func TestHandleSuccessLeavesStreamOpen(t *testing.T) {
	ts := newTestStream(t, nil)

	require.NoError(t, ts.Handle(t.Context(), &loops.DisseminateLock{Version: 1}))

	assert.Empty(t, ts.enqueued)
	assert.Zero(t, ts.cancelCalls)
}
