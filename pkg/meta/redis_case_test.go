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
	"math/rand"
	"strings"
	"syscall"
	"testing"
	"unicode"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestRedisResolveCase(t *testing.T) {
	m, err := newRedisMeta("redis", "127.0.0.1:6379/10", testConfig())
	require.NoError(t, err)
	defer m.Shutdown()
	r := m.(*redisMeta)
	ctx := Background()

	fill := func(parent Ino, names []string) {
		require.NoError(t, r.rdb.Del(ctx, r.entryKey(parent)).Err())
		for i := 0; i < len(names); i += 1000 {
			_, err := r.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for j := i; j < i+1000 && j < len(names); j++ {
					pipe.HSet(ctx, r.entryKey(parent), names[j], r.packEntry(TypeFile, Ino(j+100)))
				}
				return nil
			})
			require.NoError(t, err)
		}
	}
	// the result must be the same as checking the whole listing in Go
	check := func(parent Ino, name string) {
		var entries []*Entry
		require.Equal(t, 0, int(r.doReaddir(ctx, parent, 0, &entries, -1)))
		var expected *Entry
		for _, e := range entries {
			if strings.EqualFold(name, string(e.Name)) {
				expected = e
				break
			}
		}
		e, err := r.doResolveCase(ctx, parent, name)
		require.NoError(t, err)
		require.Equal(t, expected, e, "resolve %q", name)
	}

	small := Ino(1 << 40)
	fill(small, []string{"foo", "Bar.TXT", "desk", "Kelvin", "ſecret", "Straße", "ПРИВЕТ", "中文.txt", "\xff\xfe", "dup", "DUP", "\u212aey-ſ", "a*b?[c]\\", "x\xffK", "y\xfe\u212a"})
	for _, name := range []string{
		"FOO", "bar.txt", "DESK", "deſk", "\u212aelvin", "kelvin", "SECRET", "Secret", "STRAẞE", "strasse",
		"привет", "中文.TXT", "\xfe\xff", "\ufffd\ufffd", "Dup", "KEY-S", "key-ſ", "A*B?[C]\\", "a*b?[c]", "X\xfe\u212a", "x\ufffdk", "Y\xffk", "missing", "fo", "fooo",
	} {
		check(small, name)
	}

	// random names from characters with tricky case folding
	rnd := rand.New(rand.NewSource(1))
	alphabet := []rune("aAkKsS-*?[]\\\u212a\u017f\u00df\u1e9e\u00e4\u00c4\u03c9\u03a9\u2126\u03c3\u03c2\u03a3\u0432\u0412\u1c80\u4e2d\ufffd")
	randomName := func() string {
		var b strings.Builder
		for i := rnd.Intn(5); i >= 0; i-- {
			if rnd.Intn(15) == 0 {
				b.WriteByte(0xfe) // invalid UTF-8
			} else {
				b.WriteRune(alphabet[rnd.Intn(len(alphabet))])
			}
		}
		return b.String()
	}
	randomCase := func(name string) string {
		var b strings.Builder
		for _, r := range name {
			for i := rnd.Intn(3); i > 0; i-- {
				r = unicode.SimpleFold(r)
			}
			b.WriteRune(r)
		}
		return b.String()
	}
	random := small + 2
	names := make([]string, 300)
	for i := range names {
		names[i] = randomName()
	}
	fill(random, names)
	for i := 0; i < 1000; i++ {
		check(random, randomCase(names[rnd.Intn(len(names))]))
		check(random, randomName())
	}

	// more entries than one HSCAN batch
	large := small + 1
	names = make([]string, 30000)
	for i := range names {
		names[i] = fmt.Sprintf("File-%05d", i)
	}
	names[12345] = "Needle-Ünïcode-Kſ"
	fill(large, names)
	for _, name := range []string{"FILE-00000", "file-29999", "NEEDLE-ÜNÏCODE-\u212aS", "needle-ünïcode-ks", "File-30000"} {
		check(large, name)
	}

	// servers without safe glob matching fall back to the listing
	r.safeGlob = false
	_, err = r.doResolveCase(ctx, small, "FOO")
	require.Equal(t, syscall.ENOTSUP, err)
	require.Equal(t, "foo", string(r.resolveCase(ctx, small, "FOO").Name))
	r.safeGlob = true

	require.NoError(t, r.rdb.Del(ctx, r.entryKey(small), r.entryKey(large), r.entryKey(random)).Err())
}
