//go:build !noredis

package meta

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/push"
	"github.com/stretchr/testify/require"
)

func mockRedisCSCMeta(t *testing.T) *redisMeta {
	m, err := newRedisMeta("redis", "127.0.0.1:6379/10?client-cache=true", testConfig())
	require.NoError(t, err, "failed to create redis meta")
	require.Equal(t, "redis", m.Name(), "meta name should be redis")
	return m.(*redisMeta)
}

func TestRedisCache(t *testing.T) {
	ctx := context.Background()
	m := mockRedisCSCMeta(t)
	_ = m.rdb.FlushAll(ctx)
	defer m.Shutdown()
	defer m.cache.close()

	var err error
	t.Run("invalidation handling", func(t *testing.T) {
		cache := m.cache
		ino := Ino(100)
		attr := &Attr{Typ: TypeFile, Mode: 0644}
		cache.inodeCache.Add(ino, attr.Marshal())
		if _, ok := cache.inodeCache.Get(ino); !ok {
			t.Fatal("inode should be in cache")
		}

		err = m.rdb.Set(ctx, m.inodeKey(ino), m.marshal(&Attr{Mode: 0755}), 0).Err()
		require.NoError(t, err, "failed to set key %d", ino)
		dumIno := Ino(101)
		err = m.rdb.Set(ctx, m.inodeKey(dumIno), m.marshal(&Attr{Mode: 0755}), 0).Err()
		require.NoError(t, err, "failed to set key %d", dumIno)
		time.Sleep(3 * time.Second)
		if _, ok := cache.inodeCache.Get(Ino(100)); ok {
			t.Fatal("inode should be invalidated and removed from cache")
		}

		cache.entryCache.Add(cache.entryName(101, "file"), &cachedEntry{})
		m.rdb.HSet(ctx, m.entryKey(100), "file", "content").Err()
	})
	t.Run("cache expiration", func(t *testing.T) {
		shortExpiry := 50 * time.Millisecond
		cache := newRedisCache("jfs", 1000, shortExpiry, 0)
		attr := &Attr{Typ: TypeFile, Mode: 0644}
		cache.inodeCache.Add(Ino(102), attr.Marshal())
		time.Sleep(3 * shortExpiry)
		if _, ok := cache.inodeCache.Get(Ino(102)); ok {
			t.Fatal("inode should be expired")
		}
	})

	t.Run("inode hook", func(t *testing.T) {
		cache := m.cache
		ino := Ino(103)
		attr := &Attr{Typ: TypeFile, Length: 10}
		cache.inodeCache.Add(ino, attr.Marshal())

		data, err := m.rdb.Get(ctx, m.inodeKey(ino)).Bytes()
		require.NoError(t, err, "failed to get inode")
		attr2 := &Attr{}
		attr2.Unmarshal(data)
		attr2.Full = false
		require.Equal(t, *attr, *attr2)

		attr3 := &Attr{Typ: TypeFile, Length: 20}
		err = m.rdb.Set(ctx, m.inodeKey(ino), attr3.Marshal(), 0).Err()
		require.NoError(t, err)
		_, ok := cache.inodeCache.Get(ino)
		require.False(t, ok)
	})

	t.Run("entry hook", func(t *testing.T) {
		cache := m.cache
		ino := Ino(104)
		gen := cache.entryTerm(ino)
		name1, name2 := cache.entryName(ino, "f1"), cache.entryName(ino, "f2")
		cache.entryCache.Add(name1, &cachedEntry{term: gen})
		cache.entryCache.Add(name2, &cachedEntry{term: gen})

		err := m.rdb.HSet(ctx, m.entryKey(ino), "f1", "c1", "f2", "c2").Err()
		require.NoError(t, err)

		_, ok := cache.entryCache.Get(name1)
		require.False(t, ok)
		_, ok = cache.entryCache.Get(name2)
		require.False(t, ok)

		cache.entryCache.Add(name1, &cachedEntry{term: gen})
		cache.entryCache.Add(name2, &cachedEntry{term: gen})
		err = m.rdb.HDel(ctx, m.entryKey(ino), "f1", "f2").Err()
		require.NoError(t, err)

		_, ok = cache.entryCache.Get(name1)
		require.False(t, ok)
		_, ok = cache.entryCache.Get(name2)
		require.False(t, ok)
	})

	t.Run("entry generation invalidation", func(t *testing.T) {
		cache := m.cache
		parent := Ino(105)
		name := cache.entryName(parent, "f")
		gen := cache.entryTerm(parent)
		cache.entryCache.Add(name, &cachedEntry{ino: 106, term: gen})

		err := cache.HandlePushNotification(ctx, push.NotificationHandlerContext{}, []interface{}{"invalidate", []interface{}{m.entryKey(parent)}})
		require.NoError(t, err)
		require.Equal(t, gen+1, cache.entryTerm(parent))

		entry, ok := cache.entryCache.Get(name)
		require.True(t, ok)
		require.NotEqual(t, cache.entryTerm(parent), entry.term)
	})

	t.Run("entry term expires after idle", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, 10*time.Millisecond, 0)
		parent := Ino(106)

		require.Equal(t, uint64(1), cache.bumpEntryTerm(parent))
		time.Sleep(11 * cache.expiry)
		require.Equal(t, uint64(0), cache.entryTerm(parent))
	})

	t.Run("entry term refreshes on access", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, 10*time.Millisecond, 0)
		parent := Ino(107)

		require.Equal(t, uint64(1), cache.bumpEntryTerm(parent))
		time.Sleep(6 * cache.expiry)
		require.Equal(t, uint64(1), cache.entryTerm(parent))
		time.Sleep(6 * cache.expiry)
		require.Equal(t, uint64(1), cache.entryTerm(parent))
	})

	t.Run("entry mark refill requires current generation", func(t *testing.T) {
		cache := m.cache
		parent := Ino(107)
		name := cache.entryName(parent, "f")
		gen := cache.entryTerm(parent)
		cache.entryCache.Add(name, &cachedEntry{term: gen})

		cache.bumpEntryTerm(parent)
		added, _ := cache.entryCache.AddIf(name, &cachedEntry{ino: 108, term: gen}, func(oldEntry *cachedEntry, exists bool) bool {
			return exists && oldEntry.isMark() && oldEntry.term == gen && cache.entryTerm(parent) == gen
		})
		require.False(t, added)

		entry, ok := cache.entryCache.Get(name)
		require.True(t, ok)
		require.True(t, entry.isMark())
	})

	t.Run("entry mark refill requires existing mark", func(t *testing.T) {
		cache := m.cache
		parent := Ino(109)
		name := cache.entryName(parent, "f")
		gen := cache.entryTerm(parent)
		cache.entryCache.Add(name, &cachedEntry{term: gen})
		cache.entryCache.Remove(name)

		added, _ := cache.entryCache.AddIf(name, &cachedEntry{ino: 110, term: gen}, func(oldEntry *cachedEntry, exists bool) bool {
			return exists && oldEntry.isMark() && oldEntry.term == gen && cache.entryTerm(parent) == gen
		})
		require.False(t, added)
		_, ok := cache.entryCache.Get(name)
		require.False(t, ok)
	})

	t.Run("stale entry is not used while entry term is active", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, 20*time.Millisecond, 0)
		parent := Ino(110)
		name := cache.entryName(parent, "f")
		cache.entryCache.Add(name, &cachedEntry{ino: 111, term: cache.entryTerm(parent)})

		cache.bumpEntryTerm(parent)
		entry, ok := cache.entryCache.Get(name)
		require.True(t, ok)
		require.False(t, entry.term >= cache.entryTerm(parent) && !entry.isMark())
	})

	t.Run("old entry expires before idle entry term", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, 10*time.Millisecond, 0)
		parent := Ino(112)
		name := cache.entryName(parent, "f")
		cache.entryCache.Add(name, &cachedEntry{ino: 113, term: cache.entryTerm(parent)})
		cache.bumpEntryTerm(parent)

		time.Sleep(2 * cache.expiry)
		_, ok := cache.entryCache.Get(name)
		require.False(t, ok)
		require.Equal(t, uint64(1), cache.entryTerm(parent))
	})

	t.Run("active entry term outlives repeated stale entry checks", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, 10*time.Millisecond, 0)
		parent := Ino(114)
		name := cache.entryName(parent, "f")
		cache.entryCache.Add(name, &cachedEntry{ino: 115, term: cache.entryTerm(parent)})
		cache.bumpEntryTerm(parent)

		for range 3 {
			time.Sleep(4 * cache.expiry)
			entry, ok := cache.entryCache.Get(name)
			if ok {
				require.False(t, entry.term >= cache.entryTerm(parent) && !entry.isMark())
			} else {
				require.Equal(t, uint64(1), cache.entryTerm(parent))
			}
		}
		require.Equal(t, uint64(1), cache.entryTerm(parent))
	})

	t.Run("concurrent stale refills cannot overwrite newer mark", func(t *testing.T) {
		cache := newRedisCache("jfs", 1000, time.Second, 0)
		parent := Ino(116)
		name := cache.entryName(parent, "f")
		oldTerm := cache.entryTerm(parent)
		cache.entryCache.Add(name, &cachedEntry{term: oldTerm})
		newTerm := cache.bumpEntryTerm(parent)
		cache.entryCache.Add(name, &cachedEntry{term: newTerm})

		var wg sync.WaitGroup
		for i := 0; i < 8; i++ {
			wg.Add(1)
			go func(ino Ino) {
				defer wg.Done()
				cache.entryCache.AddIf(name, &cachedEntry{ino: ino, term: oldTerm}, func(oldEntry *cachedEntry, exists bool) bool {
					return exists && oldEntry.isMark() && oldEntry.term == oldTerm && cache.entryTerm(parent) == oldTerm
				})
			}(Ino(117 + i))
		}
		wg.Wait()

		entry, ok := cache.entryCache.Get(name)
		require.True(t, ok)
		require.True(t, entry.isMark())
		require.Equal(t, newTerm, entry.term)
	})

	t.Run("stale reply does not fill a newer mark", func(t *testing.T) {
		cache := newRedisCache("", 1000, time.Minute, 0)
		get1, get2 := redis.NewStringCmd(ctx, "get", "i160"), redis.NewStringCmd(ctx, "get", "i160")
		_, mark1 := cache.beforeProcess(get1)
		cache.inodeCache.Remove(160) // invalidated while the first read is in flight
		_, mark2 := cache.beforeProcess(get2)
		get1.SetVal("old")
		cache.afterProcess(get1, mark1)
		data, _ := cache.inodeCache.Get(160)
		require.Empty(t, data, "the stale reply is dropped")
		get2.SetVal("new")
		cache.afterProcess(get2, mark2)
		data, ok := cache.inodeCache.Get(160)
		require.True(t, ok)
		require.Equal(t, "new", string(data))
	})

	t.Run("own transactions drop what they wrote", func(t *testing.T) {
		cache := newRedisCache("", 1000, time.Minute, 0)
		cmds := []redis.Cmder{
			redis.NewStatusCmd(ctx, "multi"),
			redis.NewStatusCmd(ctx, "set", "i161", "new"),
			redis.NewIntCmd(ctx, "hdel", "d163", "f"),
			redis.NewIntCmd(ctx, "del", "i162"),
			redis.NewSliceCmd(ctx, "exec"),
		}
		fill := func() {
			cache.inodeCache.Add(161, []byte("old"))
			cache.inodeCache.Add(162, []byte("old"))
			cache.entryCache.Add(cache.entryName(163, "f"), &cachedEntry{ino: 161})
		}
		cached := func() int {
			n := 0
			for _, ino := range []Ino{161, 162} {
				if _, ok := cache.inodeCache.Get(ino); ok {
					n++
				}
			}
			if _, ok := cache.entryCache.Get(cache.entryName(163, "f")); ok {
				n++
			}
			return n
		}

		fill()
		for _, cmd := range cmds {
			cmd.SetErr(redis.TxFailedErr)
		}
		_ = cache.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return redis.TxFailedErr })(ctx, cmds)
		require.Equal(t, 3, cached(), "an aborted transaction writes nothing")

		for _, cmd := range cmds {
			cmd.SetErr(nil)
		}
		require.NoError(t, cache.ProcessPipelineHook(func(context.Context, []redis.Cmder) error { return nil })(ctx, cmds))
		require.Equal(t, 0, cached())
	})

	t.Run("lookup sees attributes changed by another client", func(t *testing.T) {
		w, err := newRedisMeta("redis", "127.0.0.1:6379/10", testConfig())
		require.NoError(t, err)
		defer w.Shutdown()
		require.NoError(t, w.Init(testFormat(), true))
		_, err = m.Load(true)
		require.NoError(t, err)

		var ino, found Ino
		var attr Attr
		require.Equal(t, 0, int(w.Create(Background(), RootInode, "appended", 0644, 0, 0, &ino, &attr)))
		require.Equal(t, 0, int(m.Lookup(Background(), RootInode, "appended", &found, &attr, false)))
		_, ok := m.cache.entryCache.Get(m.cache.entryName(RootInode, "appended"))
		require.True(t, ok, "the entry is cached")

		require.Equal(t, 0, int(w.Truncate(Background(), ino, 0, 4096, &attr, false)))
		require.Eventually(t, func() bool {
			var a Attr
			return m.Lookup(Background(), RootInode, "appended", &found, &a, false) == 0 && a.Length == 4096
		}, 3*time.Second, 50*time.Millisecond)
		_, ok = m.cache.entryCache.Get(m.cache.entryName(RootInode, "appended"))
		require.True(t, ok, "the entry is still cached")
	})

	raw := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379", DB: 10})
	defer raw.Close()

	t.Run("invalidations under heavy writes", func(t *testing.T) {
		w, err := newRedisMeta("redis", "127.0.0.1:6379/10", testConfig())
		require.NoError(t, err)
		defer w.Shutdown()
		require.NoError(t, w.Init(testFormat(), true))
		_, err = m.Load(true)
		require.NoError(t, err)

		mctx := Background()
		var dir Ino
		var attr Attr
		require.Equal(t, 0, int(w.Mkdir(mctx, RootInode, "stress", 0755, 0, 0, &dir, &attr)))
		inodes := make([]Ino, 200)
		for i := range inodes {
			require.Equal(t, 0, int(w.Create(mctx, dir, fmt.Sprintf("f%d", i), 0644, 0, 0, &inodes[i], &attr)))
		}

		stop := make(chan struct{})
		var wg sync.WaitGroup
		loop := func(f func()) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
						f()
					}
				}
			}()
		}
		for range 4 {
			loop(func() { // readers fill the cache
				i := rand.Intn(len(inodes))
				var ino Ino
				var a Attr
				m.Lookup(mctx, dir, fmt.Sprintf("f%d", i), &ino, &a, false)
				m.GetAttr(mctx, inodes[i], &a)
			})
			loop(func() { // another client changes the files
				var a Attr
				w.Truncate(mctx, inodes[rand.Intn(len(inodes))], 0, uint64(rand.Intn(1<<20)), &a, false)
			})
		}
		loop(func() { // many more invalidations for other inodes
			pipe := raw.Pipeline()
			for j := 0; j < 1000; j++ {
				pipe.Set(ctx, fmt.Sprintf("i%d", 1<<40+j), j, 0)
			}
			_, _ = pipe.Exec(ctx)
		})
		time.Sleep(3 * time.Second)
		close(stop)
		wg.Wait()

		require.Eventually(t, func() bool {
			for i, ino := range inodes {
				var want, got, looked Attr
				var found Ino
				if w.GetAttr(mctx, ino, &want) != 0 || m.GetAttr(mctx, ino, &got) != 0 ||
					m.Lookup(mctx, dir, fmt.Sprintf("f%d", i), &found, &looked, false) != 0 {
					return false
				}
				if found != ino || got.Length != want.Length || looked.Length != want.Length {
					return false
				}
			}
			return true
		}, 3*time.Second, 100*time.Millisecond)
		require.NotZero(t, m.cache.inodeCache.Len(), "the cache is used")
	})

	t.Run("tracking survives reconnects", func(t *testing.T) {
		subscriber := func() (id string, tracking bool) {
			list, _ := raw.ClientList(ctx).Result()
			for _, l := range strings.Split(strings.TrimSpace(list), "\n") {
				fields := strings.Fields(l)
				if !slices.Contains(fields, "sub=1") {
					continue
				}
				for _, f := range fields {
					if v, ok := strings.CutPrefix(f, "id="); ok {
						id = v
					} else if v, ok := strings.CutPrefix(f, "flags="); ok {
						tracking = strings.Contains(v, "t")
					}
				}
			}
			return
		}
		id, tracking := subscriber()
		require.True(t, tracking)
		ino := Ino(140)
		key := m.inodeKey(ino)
		require.NoError(t, raw.Set(ctx, key, "v1", 0).Err())
		// the invalidation of the SET above may drop the first read
		require.Eventually(t, func() bool {
			require.NoError(t, m.rdb.Get(ctx, key).Err())
			_, ok := m.cache.inodeCache.Get(ino)
			return ok
		}, 3*time.Second, 50*time.Millisecond)

		require.NoError(t, raw.Do(ctx, "CLIENT", "KILL", "ID", id).Err())
		require.Eventually(t, func() bool {
			newID, tracking := subscriber()
			return newID != "" && newID != id && tracking
		}, 10*time.Second, 100*time.Millisecond)
		// invalidations may have been missed while it was disconnected
		_, ok := m.cache.inodeCache.Get(ino)
		require.False(t, ok)

		require.NoError(t, m.rdb.Get(ctx, key).Err())
		require.NoError(t, raw.Set(ctx, key, "v2", 0).Err())
		require.Eventually(t, func() bool {
			v, err := m.rdb.Get(ctx, key).Result()
			return err == nil && v == "v2"
		}, 3*time.Second, 50*time.Millisecond)
	})

	// last: it flushes the database
	t.Run("flush clears the cache", func(t *testing.T) {
		key := m.inodeKey(150)
		require.NoError(t, raw.Set(ctx, key, "v", 0).Err())
		require.NoError(t, m.rdb.Get(ctx, key).Err())
		m.cache.entryCache.Add(m.cache.entryName(150, "f"), &cachedEntry{ino: 151})
		require.NotZero(t, m.cache.inodeCache.Len())

		require.NoError(t, raw.FlushDB(ctx).Err())
		require.Eventually(t, func() bool {
			return m.cache.inodeCache.Len() == 0 && m.cache.entryCache.Len() == 0
		}, 3*time.Second, 50*time.Millisecond)
	})
}
