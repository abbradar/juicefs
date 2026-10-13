//go:build !noredis
// +build !noredis

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

// needs 127.0.0.1:6380 replicating 127.0.0.1:6379
func TestRedisReadReplica(t *testing.T) { // skip mutate
	if os.Getenv("SKIP_NON_CORE") == "true" {
		t.Skipf("skip non-core test")
	}
	const addr = "127.0.0.1:6379/13?read-replica=127.0.0.1:6380"
	m, err := newRedisMeta("redis", addr, testConfig())
	require.NoError(t, err)
	r := m.(*redisMeta)
	defer m.Shutdown()
	ctx := Background()

	waitUsable := func(usable bool) {
		for i := 0; i < 100 && r.replica.usable.Load() != usable; i++ {
			time.Sleep(time.Millisecond * 100)
			r.replica.check()
		}
		require.Equal(t, usable, r.replica.usable.Load(), "usable replica")
	}

	// writes must still wait for the replica after the counter is reset with the database
	require.NoError(t, r.rdb.Set(ctx, r.replica.fenceKey, 1<<40, 0).Err())
	waitUsable(true)
	require.NoError(t, m.Reset())
	require.NoError(t, m.Init(testFormat(), true))

	t.Run("read your writes", func(t *testing.T) {
		waitUsable(true)
		require.Equal(t, redis.UniversalClient(r.replica.cli), r.reader(ctx))
		var inode Ino
		var attr Attr
		for i := 0; i < 200; i++ {
			name := fmt.Sprintf("d%d", i)
			require.Equal(t, 0, int(m.Mkdir(ctx, RootInode, name, 0755, 0, 0, &inode, &attr)))
			require.NoError(t, r.replica.cli.HGet(ctx, r.entryKey(RootInode), name).Err(), "replica has %s", name)
		}
	})

	t.Run("lagging replica", func(t *testing.T) {
		admin := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6380"})
		defer admin.Close()
		require.NoError(t, admin.SlaveOf(ctx, "no", "one").Err())
		defer admin.SlaveOf(ctx, "127.0.0.1", "6379")
		waitUsable(false)
		require.Equal(t, redis.UniversalClient(r.rdb), r.reader(ctx))

		var inode Ino
		var attr Attr
		start := time.Now()
		require.Equal(t, 0, int(m.Mkdir(ctx, RootInode, "lagging", 0755, 0, 0, &inode, &attr)))
		require.Less(t, time.Since(start), r.replica.timeout, "don't wait for a replica which is not used")
		require.Equal(t, 0, int(m.Lookup(ctx, RootInode, "lagging", &inode, &attr, false)))

		require.NoError(t, admin.SlaveOf(ctx, "127.0.0.1", "6379").Err())
		waitUsable(true)
		require.NoError(t, r.replica.cli.HGet(ctx, r.entryKey(RootInode), "lagging").Err())
	})

	t.Run("stale fence", func(t *testing.T) {
		waitUsable(true)
		timeout := r.replica.timeout
		r.replica.timeout = time.Millisecond * 300
		defer func() { r.replica.timeout = timeout }()
		admin := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6380"})
		defer admin.Close()
		defer admin.SlaveOf(ctx, "127.0.0.1", "6379")
		// a fence read before the counter was reset
		r.replica.mu.Lock()
		r.replica.applied = 1 << 40
		r.replica.mu.Unlock()
		require.NoError(t, admin.SlaveOf(ctx, "no", "one").Err())

		var inode Ino
		var attr Attr
		require.Equal(t, 0, int(m.Mkdir(ctx, RootInode, "stale", 0755, 0, 0, &inode, &attr)))
		require.Equal(t, 0, int(m.Lookup(ctx, RootInode, "stale", &inode, &attr, false)))
		require.NoError(t, admin.SlaveOf(ctx, "127.0.0.1", "6379").Err())
		waitUsable(true)
	})

	t.Run("unknown outcome", func(t *testing.T) {
		waitUsable(true)
		fence := redis.NewIntCmd(ctx, "incr", r.replica.fenceKey)
		fence.SetErr(io.ErrUnexpectedEOF)
		r.replica.afterWrite(ctx, fence)
		require.False(t, r.replica.usable.Load())
		waitUsable(true)
	})

	t.Run("failed replica", func(t *testing.T) {
		waitUsable(true)
		admin := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6380"})
		defer admin.Close()
		stale, err := admin.ConfigGet(ctx, "replica-serve-stale-data").Result()
		require.NoError(t, err)
		require.NoError(t, admin.ConfigSet(ctx, "replica-serve-stale-data", "no").Err())
		defer admin.ConfigSet(ctx, "replica-serve-stale-data", stale["replica-serve-stale-data"])
		require.NoError(t, admin.SlaveOf(ctx, "127.0.0.1", "1").Err()) // nothing listens there
		defer admin.SlaveOf(ctx, "127.0.0.1", "6379")
		require.Error(t, admin.Get(ctx, r.inodeKey(RootInode)).Err(), "MASTERDOWN")

		// served by the primary
		require.NoError(t, r.replica.cli.Get(ctx, r.inodeKey(RootInode)).Err())
		require.False(t, r.replica.usable.Load())
		require.NoError(t, r.replica.cli.HScan(ctx, r.entryKey(RootInode), 0, "*", 10).Err())
		// the cursor is not valid for the primary
		require.Error(t, r.replica.cli.HScan(ctx, r.entryKey(RootInode), 1, "*", 10).Err())

		require.NoError(t, admin.SlaveOf(ctx, "127.0.0.1", "6379").Err())
		require.NoError(t, admin.ConfigSet(ctx, "replica-serve-stale-data", stale["replica-serve-stale-data"]).Err())
		waitUsable(true)
	})

	t.Run("concurrent", func(t *testing.T) {
		waitUsable(true)
		timeout := r.replica.timeout
		r.replica.timeout = time.Millisecond * 300
		defer func() { r.replica.timeout = timeout }()
		admin := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6380"})
		defer admin.Close()
		defer admin.SlaveOf(ctx, "127.0.0.1", "6379")

		done := make(chan struct{})
		var chaos sync.WaitGroup
		chaos.Add(1)
		go func() {
			defer chaos.Done()
			for i := 0; ; i++ {
				select {
				case <-done:
					return
				case <-time.After(time.Millisecond * 150):
				}
				switch i % 4 {
				case 0:
					_ = admin.SlaveOf(ctx, "no", "one").Err()
				case 1:
					_ = admin.SlaveOf(ctx, "127.0.0.1", "6379").Err()
				case 2:
					_ = admin.ScriptFlush(ctx).Err()
				}
				r.replica.check()
			}
		}()

		var parent Ino
		var attr Attr
		require.Equal(t, 0, int(m.Mkdir(ctx, RootInode, "concurrent", 0755, 0, 0, &parent, &attr)))
		var workers sync.WaitGroup
		var created atomic.Int64
		deadline := time.Now().Add(time.Second * 4)
		for w := 0; w < 8; w++ {
			workers.Add(1)
			go func(w int) {
				defer workers.Done()
				var inode, found Ino
				var attr Attr
				for i := 0; time.Now().Before(deadline); i++ {
					name := fmt.Sprintf("f%d-%d", w, i)
					if st := m.Create(ctx, parent, name, 0644, 0, 0, &inode, &attr); st != 0 {
						t.Errorf("create %s: %s", name, st)
						return
					}
					created.Add(1)
					if st := m.Lookup(ctx, parent, name, &found, &attr, false); st != 0 || found != inode {
						t.Errorf("lookup %s after create: %s, inode %d != %d", name, st, found, inode)
					}
					if st := m.GetAttr(ctx, inode, &attr); st != 0 {
						t.Errorf("getattr %s after create: %s", name, st)
					}
				}
			}(w)
		}
		workers.Wait()
		close(done)
		chaos.Wait()
		require.NoError(t, admin.SlaveOf(ctx, "127.0.0.1", "6379").Err())
		waitUsable(true)
		var entries []*Entry
		require.Equal(t, 0, int(m.Readdir(ctx, parent, 0, &entries)))
		require.Len(t, entries, 2+int(created.Load()))
		t.Logf("created %d files", created.Load())
	})

	t.Run("lost scripts", func(t *testing.T) {
		waitUsable(true)
		r.shaLookup, err = r.loadScript(scriptLookup)
		require.NoError(t, err)
		require.NoError(t, r.replica.cli.ScriptFlush(ctx).Err())
		var inode Ino
		var attr Attr
		require.Equal(t, 0, int(m.Lookup(ctx, RootInode, "d1", &inode, &attr, false)))
		require.NotEmpty(t, r.shaLookup)
	})

	m2, err := newRedisMeta("redis", addr, testConfig())
	require.NoError(t, err)
	defer m2.Shutdown()
	testMeta(t, m2)
}
