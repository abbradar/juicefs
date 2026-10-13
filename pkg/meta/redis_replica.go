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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisReplica serves reads from a replica of the primary, usually a local one in a remote region.
//
// Every pipeline or transaction that writes to the primary also increments a fence counter, and doesn't
// return until the replica has the new value. Replication keeps the order of writes, so the replica
// then has all writes acknowledged to this client.
//
// While the replica is not available or lags more than maxLag, writes don't wait for it and reads go to
// the primary. When it's back, writes wait for it again, then the fence of the primary is loaded: it covers
// the writes acknowledged before, and reads go to the replica once it has this value.
//
// The counter is reset with the database (e.g. by FLUSHDB), so it's never assumed to only grow.
type redisReplica struct {
	cli      *redis.Client
	primary  redis.UniversalClient
	fenceKey string
	maxLag   time.Duration
	timeout  time.Duration // how long writes wait for the replica

	mu      sync.Mutex
	lagging bool   // the replica is not available or lags behind, writes don't wait for it
	dirty   uint64 // incremented when the replica may miss an acknowledged write
	clean   uint64 // the value of dirty covered by floor
	floor   int64  // the fence value the replica must have to serve reads
	applied int64  // the fence value last seen on the replica
	seen    uint64 // the sequence number of the read which returned applied
	reads   uint64 // the sequence number of the last read of the fence from the replica
	noRole  bool   // ROLE is not allowed, the lag can't be checked
	waiters map[chan struct{}]fenceWaiter
	polling bool
	offsets []offsetSample // replication offsets of the primary
	usable  atomic.Bool    // whether reads can go to the replica

	done      chan struct{}
	closeOnce sync.Once
}

// fenceWaiter waits for a read of the fence which started after the write, and returns at least n.
type fenceWaiter struct {
	n     int64
	after uint64
}

func (w fenceWaiter) done(r *redisReplica) bool {
	return r.seen > w.after && r.applied >= w.n
}

type offsetSample struct {
	time   time.Time
	offset int64
}

// commands that don't need a fence, the scripts of JuiceFS only read
var redisNoFenceCmds = map[string]bool{
	"multi": true, "exec": true, "discard": true, "watch": true, "unwatch": true,
	"hello": true, "auth": true, "select": true, "client": true, "ping": true, "echo": true, "readonly": true,
	"info": true, "role": true, "config": true, "cluster": true, "script": true, "evalsha": true,
	"get": true, "mget": true, "exists": true, "type": true, "scan": true, "dbsize": true,
	"hget": true, "hmget": true, "hgetall": true, "hkeys": true, "hvals": true, "hexists": true, "hlen": true, "hscan": true,
	"lrange": true, "llen": true, "lindex": true, "smembers": true, "sismember": true, "scard": true,
	"zrange": true, "zrangebyscore": true, "zscore": true, "zcard": true,
}

func newRedisReplica(cli *redis.Client, primary redis.UniversalClient, fenceKey string, maxLag, timeout time.Duration) *redisReplica {
	r := &redisReplica{
		cli:      cli,
		primary:  primary,
		fenceKey: fenceKey,
		maxLag:   maxLag,
		timeout:  timeout,
		lagging:  true,
		dirty:    1, // writes before this client started should be visible too
		waiters:  make(map[chan struct{}]fenceWaiter),
		done:     make(chan struct{}),
	}
	cli.AddHook((*replicaReadHook)(r))
	r.check()
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-r.done:
				return
			case <-ticker.C:
				r.check()
			}
		}
	}()
	return r
}

func (r *redisReplica) close() error {
	r.closeOnce.Do(func() { close(r.done) })
	return r.cli.Close()
}

// update must be called with mu held after changing the state.
func (r *redisReplica) update() {
	r.usable.Store(!r.lagging && r.dirty == r.clean && r.applied >= r.floor)
	for ch, w := range r.waiters {
		if w.done(r) || r.lagging {
			close(ch)
			delete(r.waiters, ch)
		}
	}
}

// setLagging must be called with mu held.
func (r *redisReplica) setLagging(lagging bool, reason string) {
	if lagging != r.lagging {
		if lagging {
			logger.Warnf("Replica %s %s, read from the primary", r.cli.Options().Addr, reason)
		} else {
			logger.Infof("Replica %s is available, read from it once it catches up", r.cli.Options().Addr)
		}
	}
	r.lagging = lagging
	r.update()
}

type replicaInternalKey struct{}

// replicaReadHook is added to the replica client: reads which fail on the replica are retried on the primary.
type replicaReadHook redisReplica

func (h *replicaReadHook) DialHook(next redis.DialHook) redis.DialHook { return nil }

func (h *replicaReadHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	r := (*redisReplica)(h)
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if ctx.Value(replicaInternalKey{}) != nil || !r.failed(err) || scanContinues(cmd) {
			return err
		}
		return r.primary.Process(ctx, cmd)
	}
}

// scanContinues returns whether cmd continues a scan, its cursor is only valid for the replica.
func scanContinues(cmd redis.Cmder) bool {
	args := cmd.Args()
	i := 2
	switch cmd.Name() {
	case "scan":
		i = 1
	case "hscan", "sscan", "zscan":
	default:
		return false
	}
	return len(args) > i && fmt.Sprint(args[i]) != "0"
}

func (h *replicaReadHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	r := (*redisReplica)(h)
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		if ctx.Value(replicaInternalKey{}) != nil || !r.failed(err) {
			return err
		}
		pipe := r.primary.Pipeline()
		for _, cmd := range cmds {
			_ = pipe.Process(ctx, cmd)
		}
		_, err = pipe.Exec(ctx)
		return err
	}
}

// failed returns whether err means that the replica can't serve reads, and stops using it until it's checked again.
func (r *redisReplica) failed(err error) bool {
	if err == nil || err == redis.Nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if _, ok := err.(redis.Error); ok {
		s := err.Error()
		if !strings.HasPrefix(s, "LOADING") && !strings.HasPrefix(s, "MASTERDOWN") {
			return false
		}
	}
	r.mu.Lock()
	r.setLagging(true, "failed: "+err.Error())
	r.mu.Unlock()
	return true
}

func (r *redisReplica) DialHook(next redis.DialHook) redis.DialHook { return nil }

// ProcessHook fences single writes. They can't carry the fence, so it's sent after them.
func (r *redisReplica) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if redisNoFenceCmds[cmd.Name()] || r.isFence(cmd) {
			return err
		}
		if _, ok := err.(redis.Error); err != nil && !ok {
			r.markDirty(err)
			return err
		}
		fence := redis.NewIntCmd(ctx, "incr", r.fenceKey)
		_ = r.primary.Process(ctx, fence)
		r.afterWrite(ctx, fence)
		return err
	}
}

func (r *redisReplica) isFence(cmd redis.Cmder) bool {
	args := cmd.Args()
	return len(args) == 2 && cmd.Name() == "incr" && args[1] == r.fenceKey
}

// ProcessPipelineHook adds the fence to pipelines and transactions with writes.
func (r *redisReplica) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		var write bool
		for _, cmd := range cmds {
			if !redisNoFenceCmds[cmd.Name()] {
				write = true
				break
			}
		}
		if !write {
			return next(ctx, cmds)
		}
		fence := redis.NewIntCmd(ctx, "incr", r.fenceKey)
		if n := len(cmds); cmds[0].Name() == "multi" && cmds[n-1].Name() == "exec" {
			cmds = append(cmds[:n-1:n-1], fence, cmds[n-1])
		} else {
			cmds = append(cmds[:n:n], fence)
		}
		err := next(ctx, cmds)
		r.afterWrite(ctx, fence)
		return err
	}
}

func (r *redisReplica) afterWrite(ctx context.Context, fence *redis.IntCmd) {
	n, err := fence.Result()
	if err == redis.TxFailedErr {
		return // nothing was written
	}
	if err != nil {
		r.markDirty(err)
	} else if !r.wait(ctx, n) {
		r.markDirty(nil)
	}
}

// markDirty records that the replica may miss an acknowledged write, err is the error of a write with unknown outcome.
func (r *redisReplica) markDirty(err error) {
	if err != nil {
		logger.Warnf("Unknown outcome of a write, read from the primary until the replica catches up: %s", err)
	}
	r.mu.Lock()
	r.dirty++
	r.update()
	r.mu.Unlock()
}

// wait returns whether the replica has the fence value n before timeout. Only the fence read after the write
// counts, a value read before could be from before a reset of the counter.
func (r *redisReplica) wait(ctx context.Context, n int64) bool {
	r.mu.Lock()
	if r.lagging {
		r.mu.Unlock()
		return false
	}
	w := fenceWaiter{n, r.reads}
	ch := make(chan struct{})
	r.waiters[ch] = w
	if !r.polling {
		r.polling = true
		go r.poll()
	}
	r.mu.Unlock()

	timer := time.NewTimer(r.timeout)
	defer timer.Stop()
	var timeout bool
	select {
	case <-ch:
	case <-timer.C:
		timeout = true
	case <-ctx.Done():
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.waiters, ch)
	if w.done(r) {
		return true
	}
	if timeout {
		r.setLagging(true, "didn't catch up in "+r.timeout.String())
	}
	return false
}

// readFence reads the fence from the replica and remembers it, unless a later read finished first.
func (r *redisReplica) readFence(ctx context.Context) error {
	r.mu.Lock()
	r.reads++
	seq := r.reads
	r.mu.Unlock()
	v, err := r.cli.Get(ctx, r.fenceKey).Int64()
	if err != nil && err != redis.Nil {
		return err
	}
	r.mu.Lock()
	if seq > r.seen {
		r.applied, r.seen = v, seq
		r.update()
	}
	r.mu.Unlock()
	return nil
}

// poll reads the fence from the replica while there are writes waiting for it.
func (r *redisReplica) poll() {
	ctx := context.WithValue(context.Background(), replicaInternalKey{}, true)
	for {
		err := r.readFence(ctx)
		r.mu.Lock()
		if len(r.waiters) == 0 {
			r.polling = false
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()
		if err != nil {
			time.Sleep(time.Millisecond * 100)
		} else {
			time.Sleep(time.Millisecond)
		}
	}
}

func isNoPermission(err error) bool {
	s := err.Error()
	return strings.HasPrefix(s, "NOPERM") || strings.Contains(s, "unknown command")
}

// check updates the state of the replica, and the fence it must have to serve reads.
func (r *redisReplica) check() {
	ctx, cancel := context.WithTimeout(context.WithValue(context.Background(), replicaInternalKey{}, true), time.Second*5)
	defer cancel()
	r.mu.Lock()
	noRole := r.noRole
	r.mu.Unlock()

	lagging := false
	reason := "lags behind"
	if !noRole {
		var err error
		if lagging, err = r.checkLag(ctx); err != nil && isNoPermission(err) {
			logger.Warnf("Can't check the lag of the replica: %s", err)
			noRole, lagging = true, false
		} else if err != nil {
			reason = "is not available: " + err.Error()
		}
	}
	rctx, rcancel := context.WithTimeout(ctx, time.Second)
	defer rcancel()
	if err := r.readFence(rctx); err != nil {
		lagging, reason = true, "is not available: "+err.Error()
	}

	r.mu.Lock()
	r.noRole = noRole
	r.setLagging(lagging, reason)
	dirty, behind := r.dirty, r.applied < r.floor
	reload := dirty != r.clean
	r.mu.Unlock()
	if lagging || !reload && !behind {
		return
	}

	// writes wait for the replica from now on, so the fence of the primary covers all writes acknowledged before
	v, err := r.primary.Get(ctx, r.fenceKey).Int64()
	if err != nil && err != redis.Nil {
		return
	}
	r.mu.Lock()
	if reload && r.dirty == dirty && !r.lagging {
		r.floor, r.clean = v, dirty
	} else if v < r.floor {
		r.floor = v // the counter was reset
	}
	r.update()
	r.mu.Unlock()
}

// checkLag returns whether the replica is disconnected, or doesn't have the offset the primary had maxLag ago.
func (r *redisReplica) checkLag(ctx context.Context) (bool, error) {
	now := time.Now()
	if res, err := r.primary.Do(ctx, "ROLE").Slice(); err == nil {
		if len(res) > 1 {
			if offset, ok := res[1].(int64); ok {
				r.mu.Lock()
				r.offsets = append(r.offsets, offsetSample{now, offset})
				// keep the newest sample which is older than maxLag
				i := 0
				for i+1 < len(r.offsets) && now.Sub(r.offsets[i+1].time) >= r.maxLag {
					i++
				}
				r.offsets = r.offsets[i:]
				r.mu.Unlock()
			}
		}
	} else if isNoPermission(err) {
		return false, err
	}
	// a primary which is not available is not checked, writes fail anyway

	rctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	res, err := r.cli.Do(rctx, "ROLE").Slice()
	if err != nil {
		return true, err
	}
	if len(res) < 5 || res[0] != "slave" || res[3] != "connected" {
		return true, nil
	}
	offset, ok := res[4].(int64)
	if !ok {
		return true, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.offsets) > 0 && offset < r.offsets[0].offset, nil
}
