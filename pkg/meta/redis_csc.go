//go:build !noredis
// +build !noredis

/*
 * JuiceFS, Copyright 2025 Juicedata, Inc.
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
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

// cachedEntry maps a name to its inode, the attributes are in the inode cache
type cachedEntry struct {
	ino  Ino
	term uint64
}

func (e *cachedEntry) isMark() bool {
	return e.ino == 0
}

// redisCache support bcast mode client-side cache
// cache attrs and entries only, chunks are already cached in OpenCache
type redisCache struct {
	cli          *redis.Client
	sub          *redis.Client // only for the subscription, which receives the invalidations
	prefix       string
	cap          int
	expiry       time.Duration
	preload      int
	subscription *redis.PubSub

	inodeCache *expirable.LRU[Ino, []byte]
	entryCache *expirable.LRU[string, *cachedEntry]
	entryTerms *expirable.LRU[Ino, uint64]
}

func newRedisCache(prefix string, cap int, expiry time.Duration, preload int) *redisCache {
	logger.Infof("Initializing Redis client-side cache with size %d and expiry %+v", cap, expiry)
	return &redisCache{
		prefix:     prefix,
		cap:        cap,
		expiry:     expiry,
		preload:    preload,
		inodeCache: expirable.NewLRU[Ino, []byte](cap, nil, expiry),
		entryCache: expirable.NewLRU[string, *cachedEntry](cap, nil, expiry),
		entryTerms: expirable.NewLRU[Ino, uint64](0, nil, 10*expiry), // no size limit, cleanup only based on expiry
	}
}

func (c *redisCache) purge() {
	c.inodeCache.Purge()
	c.entryCache.Purge()
	c.entryTerms.Purge()
}

func (c *redisCache) init(cli redis.UniversalClient) error {
	ctx := context.Background()
	var err error
	if rc, ok := cli.(*redis.Client); ok {
		c.cli = rc
	} else if cc, ok := cli.(*redis.ClusterClient); ok {
		// For cluster mode, we should get the master node for our key
		if c.cli, err = cc.MasterForKey(ctx, c.prefix); err != nil {
			return err
		}
	}
	// A client of its own, so that tracking is enabled on every connection of the subscription,
	// including the ones it reconnects with.
	opt := *c.cli.Options()
	opt.OnConnect = c.onTrackingConnect
	opt.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	c.sub = redis.NewClient(&opt)
	// handle PUSH notifications for invalidation in c.HandlePushNotification
	if err = c.sub.RegisterPushNotificationHandler("invalidate", c, true); err != nil {
		c.close()
		return err
	}
	// under the RESP3 protocol, "__redis__:invalidate" actually has no effect.
	// we use Pubsub channel to simplify connection management and receiving PUSH messages.
	c.subscription = c.sub.Subscribe(ctx, "__redis__:invalidate")
	_ = c.subscription.Channel()
	// handle client cmd to avoid race conditions
	c.cli.AddHook(c)
	return nil
}

const (
	keyTypOther = iota
	keyTypInode
	keyTypEntry
)

func (c *redisCache) parse(key string) int {
	if strings.HasPrefix(key, c.prefix+"i") {
		return keyTypInode
	}
	if strings.HasPrefix(key, c.prefix+"d") {
		return keyTypEntry
	}
	return keyTypOther
}

func (c *redisCache) entryName(parent Ino, name string) string {
	return fmt.Sprintf("%d%d%s", parent, os.PathSeparator, name)
}

func (c *redisCache) entryTerm(parent Ino) uint64 {
	term, ok := c.entryTerms.Get(parent)
	if !ok {
		return 0
	}
	c.entryTerms.Add(parent, term)
	return term
}

func (c *redisCache) bumpEntryTerm(parent Ino) uint64 {
	term, _ := c.entryTerms.Get(parent)
	c.entryTerms.Add(parent, term+1)
	return term + 1
}

func (c *redisCache) HandlePushNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if len(notification) != 2 || notification[0] == nil {
		return nil
	}
	if typ, ok := notification[0].(string); !ok || typ != "invalidate" {
		return nil
	}
	if notification[1] == nil { // all keys, e.g. after FLUSHDB
		c.purge()
		return nil
	}
	iKeys, ok := notification[1].([]interface{})
	if !ok {
		return nil
	}
	var key string
	for _, iKey := range iKeys {
		key, ok = iKey.(string)
		if !ok {
			continue
		}
		typ := c.parse(key)
		switch typ {
		case keyTypInode:
			inodeStr := key[len(c.prefix)+1:]
			inode, err := strconv.ParseUint(inodeStr, 10, 64)
			if err == nil {
				c.inodeCache.Remove(Ino(inode))
			}
		case keyTypEntry:
			parentStr := key[len(c.prefix)+1:]
			parent, err := strconv.ParseUint(parentStr, 10, 64)
			if err == nil {
				c.bumpEntryTerm(Ino(parent))
			}
		}
	}
	return nil
}

func (c *redisCache) DialHook(next redis.DialHook) redis.DialHook { return nil }

// beforeProcess returns false if the reply was served from the cache. For a read of an inode which is not cached,
// it returns the mark it placed: only this read may fill it, so that a reply older than an invalidation is dropped
// even if another read placed a new mark since then.
func (c *redisCache) beforeProcess(cmd redis.Cmder) (bool, []byte) {
	name, args := cmd.Name(), cmd.Args()
	var key string
	var ok bool
	if len(args) < 2 {
		return true, nil
	}
	if key, ok = args[1].(string); !ok {
		return true, nil
	}
	typ := c.parse(key)

	if name == "get" && typ == keyTypInode {
		num, err := strconv.ParseUint(key[len(c.prefix)+1:], 10, 64)
		if err == nil {
			inode := Ino(num)
			if data, ok := c.inodeCache.Get(inode); ok {
				if len(data) > 0 {
					rsp := cmd.(*redis.StringCmd)
					rsp.SetErr(nil)
					rsp.SetVal(bytesToString(data))
					return false, nil
				}
			}
			mark := make([]byte, 0, 1) // distinct from other marks
			if added, _ := c.inodeCache.AddIf(inode, mark, func(oldVal []byte, exists bool) bool {
				return !exists
			}); added {
				return true, mark
			}
			// request to Redis server
		}
	}
	return true, nil
}

func (c *redisCache) afterProcess(cmd redis.Cmder, mark []byte) {
	name, args := cmd.Name(), cmd.Args()
	var key string
	var ok bool
	if len(args) < 2 {
		return
	}
	if key, ok = args[1].(string); !ok {
		return
	}
	typ := c.parse(key)

	switch name {
	case "get":
		if typ == keyTypInode && mark != nil {
			if data, err := cmd.(*redis.StringCmd).Bytes(); err == nil {
				num, err := strconv.ParseUint(key[len(c.prefix)+1:], 10, 64)
				if err != nil {
					return
				}
				_, _ = c.inodeCache.AddIf(Ino(num), data, func(oldVal []byte, exists bool) bool {
					return exists && len(oldVal) == 0 && unsafe.SliceData(oldVal) == unsafe.SliceData(mark)
				})
			}
		}
	case "del":
		if cmd.Err() == nil {
			for _, arg := range args[1:] {
				if k, ok := arg.(string); ok && c.parse(k) == keyTypInode {
					if num, err := strconv.ParseUint(k[len(c.prefix)+1:], 10, 64); err == nil {
						_ = c.inodeCache.Remove(Ino(num))
					}
				}
			}
		}
	case "set":
		if typ == keyTypInode {
			if cmd.(*redis.StatusCmd).Err() == nil {
				if num, err := strconv.ParseUint(key[len(c.prefix)+1:], 10, 64); err == nil {
					_ = c.inodeCache.Remove(Ino(num))
				}
			}
		}
	case "hdel":
		if typ == keyTypEntry {
			if err := cmd.(*redis.IntCmd).Err(); err == nil {
				for i := 2; i < len(args); i++ {
					_ = c.entryCache.Remove(fmt.Sprintf("%s%d%s", key[len(c.prefix)+1:], os.PathSeparator, args[i]))
				}
			}
		}
	case "hset":
		if typ == keyTypEntry {
			if err := cmd.(*redis.IntCmd).Err(); err == nil {
				for i := 2; i < len(args); i += 2 {
					_ = c.entryCache.Remove(fmt.Sprintf("%s%d%s", key[len(c.prefix)+1:], os.PathSeparator, args[i]))
				}
			}
		}
	}
}

func (c *redisCache) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		send, mark := c.beforeProcess(cmd)
		if !send {
			return nil
		}
		err := next(ctx, cmd)
		c.afterProcess(cmd, mark)
		return err
	}
}

// ProcessPipelineHook drops what pipelines and transactions wrote, so that this client reads its own writes
// without waiting for the invalidations. Reads in pipelines are not cached.
func (c *redisCache) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			c.afterProcess(cmd, nil)
		}
		return err
	}
}

func (c *redisCache) close() {
	if c.subscription != nil {
		if err := c.subscription.Close(); err != nil {
			logger.Warnf("failed closing Redis cache subscription: %v", err)
		}
		c.subscription = nil
	}
	if c.sub != nil {
		_ = c.sub.Close()
		c.sub = nil
	}
	c.cli = nil
}

// onTrackingConnect enables tracking on a new connection of the subscription, and clears the caches,
// which may have missed invalidations while it was not connected.
func (c *redisCache) onTrackingConnect(ctx context.Context, cn *redis.Conn) error {
	if err := cn.Do(ctx, "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", c.prefix+"i", "PREFIX", c.prefix+"d").Err(); err != nil {
		logger.Warnf("Failed to enable Redis client-side caching on new connection: %v", err)
		return err
	}
	// after enabling tracking: the values read from now on get invalidated, and the reads in flight lose their marks
	c.purge()
	return nil
}

func (m *redisMeta) preloadCache() {
	if m.cache == nil {
		return
	}
	if m.cache.preload <= 0 {
		return
	}
	start := time.Now()
	ctx := Background()
	attr := &Attr{}
	if eno := m.doGetAttr(ctx, m.root, attr); eno != 0 {
		logger.Warnf("failed to get root inode %d attribute: %d", m.root, eno)
		return
	}

	term := m.cache.entryTerm(m.root)
	var entries []*Entry
	if eno := m.doReaddir(ctx, m.root, 0, &entries, m.cache.preload); eno != 0 {
		logger.Warnf("failed to read root %d directory: %d", m.root, eno)
		return
	}
	for _, entry := range entries {
		m.cache.entryCache.Add(m.cache.entryName(m.root, string(entry.Name)), &cachedEntry{ino: entry.Inode, term: term})
	}
	logger.Infof("preload %d inodes in %v", m.cache.inodeCache.Len(), time.Since(start))
}

func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
