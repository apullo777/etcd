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

package cache

import (
	"fmt"
	"sync"

	"github.com/google/btree"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var ErrNotReady = fmt.Errorf("cache: store not ready")

type store struct {
	mu        sync.RWMutex
	tree      *btree.BTree
	degree    int
	latestRev int64
}

func newStore(degree int) *store {
	return &store{
		tree:   btree.New(degree),
		degree: degree,
	}
}

type kvItem struct {
	key string
	kv  *mvccpb.KeyValue
}

func newKVItem(kv *mvccpb.KeyValue) *kvItem {
	return &kvItem{key: string(kv.Key), kv: kv}
}

func (a *kvItem) Less(b btree.Item) bool {
	return a.key < b.(*kvItem).key
}

func (s *store) Get(startKey, endKey []byte) ([]*mvccpb.KeyValue, int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.latestRev == 0 {
		return nil, 0, ErrNotReady
	}

	var out []*mvccpb.KeyValue
	switch {
	case len(endKey) == 0:
		if item := s.tree.Get(probeItemFromBytes(startKey)); item != nil {
			out = append(out, item.(*kvItem).kv)
		}

	case isPrefixScan(endKey):
		s.tree.AscendGreaterOrEqual(probeItemFromBytes(startKey), func(item btree.Item) bool {
			out = append(out, item.(*kvItem).kv)
			return true
		})

	default:
		s.tree.AscendRange(probeItemFromBytes(startKey), probeItemFromBytes(endKey), func(item btree.Item) bool {
			out = append(out, item.(*kvItem).kv)
			return true
		})
	}
	return out, s.latestRev, nil
}

func (s *store) Restore(kvs []*mvccpb.KeyValue, rev int64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.tree = btree.New(s.degree)
	for _, kv := range kvs {
		s.tree.ReplaceOrInsert(newKVItem(kv))
	}
	s.latestRev = rev
}

func (s *store) Apply(events []*clientv3.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, ev := range events {
		if ev.Kv.ModRevision < s.latestRev {
			return fmt.Errorf("cache: stale event batch (rev %d < latest %d)", ev.Kv.ModRevision, s.latestRev)
		}
	}

	for _, ev := range events {
		switch ev.Type {
		case clientv3.EventTypeDelete:
			s.tree.Delete(&kvItem{key: string(ev.Kv.Key)})
		case clientv3.EventTypePut:
			s.tree.ReplaceOrInsert(newKVItem(ev.Kv))
		}
		if ev.Kv.ModRevision > s.latestRev {
			s.latestRev = ev.Kv.ModRevision
		}
	}
	return nil
}

func (s *store) LatestRev() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.latestRev
}

// isPrefixScan detects endKey=={0} semantics
func isPrefixScan(endKey []byte) bool {
	return len(endKey) == 1 && endKey[0] == 0
}

func probeItemFromBytes(b []byte) *kvItem { return &kvItem{key: string(b)} }
