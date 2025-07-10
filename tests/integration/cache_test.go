// Copyright 2025 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"go.etcd.io/etcd/api/v3/mvccpb"
	cache "go.etcd.io/etcd/cache/v3"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/integration"
)

func TestCacheWatch(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	client := clus.Client(0)

	c, err := cache.New(client, "/foo", cache.WithHistoryWindowSize(32))
	if err != nil {
		t.Fatalf("New(...): %v", err)
	}
	t.Cleanup(c.Close)
	if err := c.WaitReady(t.Context()); err != nil {
		t.Fatalf("cache not ready: %v", err)
	}
	testWatch(t, client.KV, c)
}

func TestWatch(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	client := clus.Client(0)

	testWatch(t, client.KV, client.Watcher)
}

func testWatch(t *testing.T, kv clientv3.KV, watcher Watcher) {
	ctx := t.Context()
	event1PutFooA := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/a"),
			Value:          []byte("1"),
			CreateRevision: 2,
			ModRevision:    2,
			Version:        1,
		},
	}
	event2PutFooB := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/b"),
			Value:          []byte("2"),
			CreateRevision: 3,
			ModRevision:    3,
			Version:        1,
		},
	}
	event3DeleteFooA := &clientv3.Event{
		Type: clientv3.EventTypeDelete,
		Kv: &mvccpb.KeyValue{
			Key:         []byte("/foo/a"),
			ModRevision: 4,
		},
	}
	event4PutFooA := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/a"),
			Value:          []byte("3"),
			CreateRevision: 5,
			ModRevision:    5,
			Version:        1,
		},
	}
	event5DeleteFooB := &clientv3.Event{
		Type: clientv3.EventTypeDelete,
		Kv: &mvccpb.KeyValue{
			Key:         []byte("/foo/b"),
			ModRevision: 5,
		},
	}
	event6PutFooC := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/c"),
			Value:          []byte("x"),
			CreateRevision: 6,
			ModRevision:    6,
			Version:        1,
		},
	}
	event7PutFooBar := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/bar"),
			Value:          []byte("y"),
			CreateRevision: 7,
			ModRevision:    7,
			Version:        1,
		},
	}
	event8PutFooBaz := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/foo/baz"),
			Value:          []byte("z"),
			CreateRevision: 8,
			ModRevision:    8,
			Version:        1,
		},
	}
	event9PutYoo := &clientv3.Event{
		Type: clientv3.EventTypePut,
		Kv: &mvccpb.KeyValue{
			Key:            []byte("/yoo"),
			Value:          []byte("z"),
			CreateRevision: 9,
			ModRevision:    9,
			Version:        1,
		},
	}

	tcs := []struct {
		name           string
		key            string
		opts           []clientv3.OpOption
		wantEvents     []*clientv3.Event
		expectCanceled bool
	}{
		{
			name:       "Watch single key existing /foo/c",
			key:        "/foo/c",
			opts:       nil,
			wantEvents: []*clientv3.Event{event6PutFooC},
		},
		{
			name:       "Watch single key non‑existent /doesnotexist",
			key:        "/doesnotexist",
			opts:       nil,
			wantEvents: nil,
		},
		{
			name:       "Watch range empty",
			key:        "",
			opts:       []clientv3.OpOption{clientv3.WithRange("")},
			wantEvents: nil,
		},
		{
			name:       "Watch range [/foo/a, /foo/b)",
			key:        "/foo/a",
			opts:       []clientv3.OpOption{clientv3.WithRange("/foo/b")},
			wantEvents: []*clientv3.Event{event1PutFooA, event3DeleteFooA, event4PutFooA},
		},
		{
			name:           "Watch invalid range [/foo/a, /foo/a)",
			key:            "/foo/a",
			opts:           []clientv3.OpOption{clientv3.WithRange("/foo/a")},
			wantEvents:     nil,
			expectCanceled: true,
		},
		{
			name:           "Watch invalid range [/foo/b, /foo/a)",
			key:            "/foo/b",
			opts:           []clientv3.OpOption{clientv3.WithRange("/foo/a")},
			wantEvents:     nil,
			expectCanceled: true,
		},
		{
			name:       "[/foo/b, /foo/c)",
			key:        "/foo/b",
			opts:       []clientv3.OpOption{clientv3.WithRange("/foo/c")},
			wantEvents: []*clientv3.Event{event2PutFooB, event5DeleteFooB, event7PutFooBar, event8PutFooBaz},
		},
		{
			name:           "Watch range [/foo/c, /z)",
			key:            "/foo/c",
			opts:           []clientv3.OpOption{clientv3.WithRange("/z")},
			wantEvents:     []*clientv3.Event{event6PutFooC, event9PutYoo},
			expectCanceled: true,
		},
		{
			name:       "Watch with prefix /foo/b",
			key:        "/foo/b",
			opts:       []clientv3.OpOption{clientv3.WithPrefix()},
			wantEvents: []*clientv3.Event{event2PutFooB, event5DeleteFooB, event7PutFooBar, event8PutFooBaz},
		},
		{
			name:       "Watch with prefix non-existent /doesnotexist",
			key:        "/doesnotexist",
			opts:       []clientv3.OpOption{clientv3.WithPrefix()},
			wantEvents: nil,
		},
		{
			name:       "Watch with prefix empty string",
			key:        "",
			opts:       []clientv3.OpOption{clientv3.WithPrefix()},
			wantEvents: []*clientv3.Event{event1PutFooA, event2PutFooB, event3DeleteFooA, event4PutFooA, event5DeleteFooB, event6PutFooC, event7PutFooBar, event8PutFooBaz, event9PutYoo},
		},
		{
			name:       "Watch from key /foo/b",
			key:        "/foo/b",
			opts:       []clientv3.OpOption{clientv3.WithFromKey()},
			wantEvents: []*clientv3.Event{event2PutFooB, event5DeleteFooB, event6PutFooC, event7PutFooBar, event8PutFooBaz, event9PutYoo},
		},
		{
			name:       "Watch from empty key",
			key:        "",
			opts:       []clientv3.OpOption{clientv3.WithFromKey()},
			wantEvents: []*clientv3.Event{event1PutFooA, event2PutFooB, event3DeleteFooA, event4PutFooA, event5DeleteFooB, event6PutFooC, event7PutFooBar, event8PutFooBaz, event9PutYoo},
		},
		{
			name:       "Watch from non-existent key /doesnotexist",
			key:        "/doesnotexist",
			opts:       []clientv3.OpOption{clientv3.WithFromKey()},
			wantEvents: []*clientv3.Event{event1PutFooA, event2PutFooB, event3DeleteFooA, event4PutFooA, event5DeleteFooB, event6PutFooC, event7PutFooBar, event8PutFooBaz, event9PutYoo},
		},
	}

	t.Log("Open test watchers")
	watches := make([]clientv3.WatchChan, len(tcs))
	for i, tc := range tcs {
		watches[i] = watcher.Watch(ctx, tc.key, append(tc.opts, clientv3.WithCreatedNotify())...)
	}
	t.Log("Setup data")
	if _, err := kv.Put(ctx, string(event1PutFooA.Kv.Key), string(event1PutFooA.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Put(ctx, string(event2PutFooB.Kv.Key), string(event2PutFooB.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Delete(ctx, string(event3DeleteFooA.Kv.Key)); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := kv.Txn(ctx).Then(clientv3.OpPut(string(event4PutFooA.Kv.Key), string(event4PutFooA.Kv.Value)), clientv3.OpDelete(string(event5DeleteFooB.Kv.Key))).Commit(); err != nil {
		t.Fatalf("Txn: %v", err)
	}
	if _, err := kv.Put(ctx, string(event6PutFooC.Kv.Key), string(event6PutFooC.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Put(ctx, string(event7PutFooBar.Kv.Key), string(event7PutFooBar.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Put(ctx, string(event8PutFooBaz.Kv.Key), string(event8PutFooBaz.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if _, err := kv.Put(ctx, string(event9PutYoo.Kv.Key), string(event9PutYoo.Kv.Value)); err != nil {
		t.Fatalf("Put: %v", err)
	}
	t.Log("Validate")
	for i, tc := range tcs {
		i, tc := i, tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resp, ok := <-watches[i]
			if !ok {
				t.Fatalf("unexpected closed watch")
			}
			if resp.Canceled {
				if tc.expectCanceled {
					// invalid range or cache-only “unsupported” watch
					return
				}
				t.Fatalf("unexpected canceled watch: %+v", resp)
			}
			// We ignore the first WatchResponse which is the CreatedNotify (len(resp.Events) == 0) and read the real payload that follows.
			events, _ := readEvents(watches[i])
			if diff := cmp.Diff(tc.wantEvents, events); diff != "" {
				t.Errorf("unexpected events (-want +got):\n%s", diff)
			}
		})
	}
}

func TestCacheLaggingWatcher(t *testing.T) {
	const prefix = "/test/"
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	client := clus.Client(0)

	tests := []struct {
		name                string
		window              int
		eventCount          int
		wantExactEventCount int
		wantAtMaxEventCount int
		wantClosed          bool
	}{
		{
			name:                "all event fit",
			window:              10,
			eventCount:          9,
			wantExactEventCount: 9,
			wantClosed:          false,
		},
		{
			name:                "events fill window",
			window:              10,
			eventCount:          10,
			wantExactEventCount: 10,
			wantClosed:          false,
		},
		{
			name:                "event fill pipeline",
			window:              10,
			eventCount:          11,
			wantExactEventCount: 11,
			wantClosed:          false,
		},
		{
			name:                "pipeline overflow",
			window:              10,
			eventCount:          12,
			wantAtMaxEventCount: 1, // Either 0 or 1.
			wantClosed:          true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := cache.New(
				client, prefix,
				cache.WithHistoryWindowSize(tt.window),
				cache.WithPerWatcherBufferSize(0),
				cache.WithResyncInterval(10*time.Millisecond),
			)
			if err != nil {
				t.Fatalf("New(...): %v", err)
			}
			defer c.Close()

			if err := c.WaitReady(t.Context()); err != nil {
				t.Fatalf("cache not ready: %v", err)
			}
			ch := c.Watch(t.Context(), prefix, clientv3.WithPrefix())

			generateEvents(t, client, prefix, tt.eventCount)
			gotEvents, ok := readEvents(ch)
			closed := !ok

			if tt.wantExactEventCount != 0 && tt.wantExactEventCount != len(gotEvents) {
				t.Errorf("gotEvents=%v, wantEvents=%v", len(gotEvents), tt.wantExactEventCount)
			}
			if tt.wantAtMaxEventCount != 0 && len(gotEvents) > tt.wantAtMaxEventCount {
				t.Errorf("gotEvents=%v, wantEvents<%v", len(gotEvents), tt.wantAtMaxEventCount)
			}
			if closed != tt.wantClosed {
				t.Errorf("closed=%v, wantClosed=%v", closed, tt.wantClosed)
			}
		})
	}
}

func TestCacheRejectsUnsupportedWatch(t *testing.T) {
	integration.BeforeTest(t)
	clus := integration.NewCluster(t, &integration.ClusterConfig{Size: 1})
	t.Cleanup(func() { clus.Terminate(t) })
	client := clus.Client(0)

	ctx := t.Context()

	c, err := cache.New(client, "")
	if err != nil {
		t.Fatalf("New(...): %v", err)
	}
	t.Cleanup(c.Close)
	if err := c.WaitReady(ctx); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name string
		key  string
		opts []clientv3.OpOption
	}{
		{
			name: "non_zero_start_revision",
			key:  "",
			opts: []clientv3.OpOption{clientv3.WithPrefix(), clientv3.WithRev(123)},
		},
		{
			name: "subprefix_watch",
			key:  "foo/",
			opts: []clientv3.OpOption{clientv3.WithPrefix()},
		},
		{
			name: "single_key_watch",
			key:  "foo/bar",
			opts: nil, // exact-key watch (no WithPrefix)
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			watchCh := c.Watch(ctx, tt.key, tt.opts...)
			resp, ok := <-watchCh

			if !ok || !resp.Canceled {
				t.Errorf("expected canceled response, got %#v (closed=%v)", resp, !ok)
			}
		})
	}
}

func generateEvents(t *testing.T, client *clientv3.Client, prefix string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		if _, err := client.Put(t.Context(), key, fmt.Sprintf("%d", i)); err != nil {
			t.Fatalf("Put(%q): %v", key, err)
		}
	}
}

type Watcher interface {
	Watch(ctx context.Context, key string, opts ...clientv3.OpOption) clientv3.WatchChan
}

func readEvents(watch clientv3.WatchChan) (events []*clientv3.Event, ok bool) {
	deadline := time.After(time.Second)
	for {
		select {
		case resp, ok := <-watch:
			if !ok {
				return events, false
			}
			events = append(events, resp.Events...)
		case <-deadline:
			return events, true
		case <-time.After(100 * time.Millisecond):
			return events, true
		}
	}
}
