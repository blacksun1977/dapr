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

package inflight

// 玄滩自定义放置策略的单体 + 集成测试。
//
// - 单体测试：纯 Go 逻辑(类型分发、配置解析)，无需任何外部依赖。
// - 集成测试：需要一个真实 Redis；通过环境变量 XT_TEST_REDIS_ADDR 注入地址，
//   未设置时自动 Skip。配套脚本用 docker 起一个 redis 再设置该变量运行。
//
// 测试与被测代码同包(inflight)，故可直接构造 Inflight、读写包内全局变量，
// 绕过 xuantanInit 的 sync.Once，对每个用例独立配置。

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/dapr/pkg/actors/api"
	"github.com/dapr/dapr/pkg/placement/hashing"
)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// xtRing 构造一个含给定 host 地址的一致性哈希环。
func xtRing(hosts ...string) *hashing.Consistent {
	vnc := hashing.NewVirtualNodesCache()
	lm := make(map[string]*hashing.Host, len(hosts))
	for idx, h := range hosts {
		lm[h] = hashing.NewHost(h, "app-"+strconv.Itoa(idx), 0, 7000)
	}
	return hashing.NewFromExisting(lm, 100, vnc)
}

// xtInflight 构造一个仅含单个 actorType -> ring 的 Inflight。
func xtInflight(actorType string, ring *hashing.Consistent) *Inflight {
	return &Inflight{
		hostname: "127.0.0.1",
		port:     "7000",
		hashTable: &hashing.ConsistentHashTables{
			Entries: map[string]*hashing.Consistent{actorType: ring},
		},
	}
}

// xtClearCache 清空进程级本地缓存(用例间隔离)。
func xtClearCache() {
	xuantanCache.Range(func(k, _ any) bool {
		xuantanCache.Delete(k)
		return true
	})
}

// xtExclude 返回 all 中去掉 target 后的列表(模拟某 host 下线)。
func xtExclude(target string, all ...string) []string {
	out := make([]string, 0, len(all))
	for _, h := range all {
		if h != target {
			out = append(out, h)
		}
	}
	return out
}

// xtRedis 连接 XT_TEST_REDIS_ADDR 指向的 Redis；未设置则 Skip。每次 FlushDB 清场。
func xtRedis(t *testing.T) redis.UniversalClient {
	t.Helper()
	addr := os.Getenv("XT_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("XT_TEST_REDIS_ADDR not set; skip redis integration test")
	}
	c := redis.NewUniversalClient(&redis.UniversalOptions{Addrs: []string{addr}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, c.Ping(ctx).Err(), "redis ping")
	require.NoError(t, c.FlushDB(ctx).Err(), "redis flushdb")
	return c
}

// xtSetup 把包内全局变量重置为可控的测试配置，并把 xuantanRDB 指向 c。
func xtSetup(c redis.UniversalClient) {
	xuantanRDB = c
	xuantanKeyPrefix = "xt:dapr:bind:"
	xuantanIdsPrefix = "xt:dapr:ids:"
	xuantanBindTTL = 3 * time.Hour
	xuantanRoomCapPerHost = 0
	xuantanTypeKinds = []xuantanTypeKind{{typ: "table", kind: xtKindTable}, {typ: "room", kind: xtKindRoom}}
	xtClearCache()
}

func xtReq(actorType, id string) *api.LookupActorRequest {
	return &api.LookupActorRequest{ActorType: actorType, ActorID: id}
}

// ---------------------------------------------------------------------------
// 单体测试(无需 Redis)
// ---------------------------------------------------------------------------

func TestXuantanKindOf(t *testing.T) {
	old := xuantanTypeKinds
	t.Cleanup(func() { xuantanTypeKinds = old })

	xuantanTypeKinds = []xuantanTypeKind{
		{typ: "table", kind: xtKindTable},
		{typ: "table_py", kind: xtKindTable},
		{typ: "room", kind: xtKindRoom},
	}
	assert.Equal(t, xtKindTable, xuantanKindOf("table"))
	assert.Equal(t, xtKindTable, xuantanKindOf("table_py"))
	assert.Equal(t, xtKindRoom, xuantanKindOf("room"))
	assert.Equal(t, 0, xuantanKindOf("user"))
	assert.Equal(t, 0, xuantanKindOf(""))
}

func TestXuantanAppendTypes(t *testing.T) {
	const env = "XT_TEST_APPEND_TYPES"

	t.Setenv(env, " a , b ,, c ")
	got := xuantanAppendTypes(nil, env, "def", xtKindTable)
	require.Len(t, got, 3)
	assert.Equal(t, xuantanTypeKind{typ: "a", kind: xtKindTable}, got[0])
	assert.Equal(t, xuantanTypeKind{typ: "b", kind: xtKindTable}, got[1])
	assert.Equal(t, xuantanTypeKind{typ: "c", kind: xtKindTable}, got[2])

	// 未设置环境变量时回退默认列表
	got2 := xuantanAppendTypes(nil, "XT_TEST_APPEND_TYPES_UNSET", "x,y", xtKindRoom)
	require.Len(t, got2, 2)
	assert.Equal(t, xuantanTypeKind{typ: "x", kind: xtKindRoom}, got2[0])
	assert.Equal(t, xuantanTypeKind{typ: "y", kind: xtKindRoom}, got2[1])

	// 追加语义：在已有切片后继续追加
	merged := xuantanAppendTypes(got2, env, "def", xtKindTable)
	require.Len(t, merged, 5)
}

func TestXuantanCacheStoreKind(t *testing.T) {
	xtClearCache()
	t.Cleanup(xtClearCache)

	xuantanCacheStore("room:1", "h:1", xtKindRoom)
	xuantanCacheStore("table:1", "h:2", xtKindTable)

	v, ok := xuantanCache.Load("room:1")
	require.True(t, ok)
	e := v.(xuantanCacheEntry)
	assert.Equal(t, "h:1", e.host)
	assert.Equal(t, xtKindRoom, e.kind)
	assert.WithinDuration(t, time.Now(), e.at, time.Second)
}

// resolveXuantan 分发器：未启用(xuantanRDB==nil)时整体旁路。
func TestXuantanResolveBypassWhenDisabled(t *testing.T) {
	oldRDB := xuantanRDB
	t.Cleanup(func() { xuantanRDB = oldRDB })
	xuantanRDB = nil

	in := xtInflight("table", xtRing("a:1", "b:1"))
	resp, handled, err := in.resolveXuantan(xtReq("table", "x"))
	assert.False(t, handled)
	assert.Nil(t, resp)
	assert.NoError(t, err)
}

// ---------------------------------------------------------------------------
// 集成测试(需要 Redis)
// ---------------------------------------------------------------------------

// table：首次哈希分配 -> 持久化 -> 粘性 -> host 失效后重分配。
func TestXuantanTableAssignStickyReassign(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	h1, h2, h3 := "10.0.0.1:7", "10.0.0.2:7", "10.0.0.3:7"
	in := xtInflight("table", xtRing(h1, h2, h3))
	req := xtReq("table", "100001")

	// 首次分配
	resp, handled, err := in.resolveXuantanTable(req)
	require.True(t, handled)
	require.NoError(t, err)
	require.NotNil(t, resp)
	first := resp.Address
	assert.Contains(t, []string{h1, h2, h3}, first)

	// 已持久化到 Redis
	got, err := c.Get(ctx, "xt:dapr:bind:table:100001").Result()
	require.NoError(t, err)
	assert.Equal(t, first, got)

	// 粘性：清本地缓存强制走 Redis，仍返回同一 host
	xtClearCache()
	resp2, _, err := in.resolveXuantanTable(req)
	require.NoError(t, err)
	assert.Equal(t, first, resp2.Address)

	// 杀掉被分配的 host -> 应重分配到其它存活 host，并 CAS 覆盖 Redis
	live := xtExclude(first, h1, h2, h3)
	in = xtInflight("table", xtRing(live...))
	xtClearCache()
	resp3, _, err := in.resolveXuantanTable(req)
	require.NoError(t, err)
	require.NotNil(t, resp3)
	assert.NotEqual(t, first, resp3.Address)
	assert.Contains(t, live, resp3.Address)

	got2, err := c.Get(ctx, "xt:dapr:bind:table:100001").Result()
	require.NoError(t, err)
	assert.Equal(t, resp3.Address, got2)
}

// table：并发首次分配收敛到同一 host，Redis 仅一个权威值。
func TestXuantanTableConcurrentConverge(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	hosts := []string{"a:1", "b:1", "c:1", "d:1"}
	const n = 32
	results := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			in := xtInflight("table", xtRing(hosts...))
			r, _, err := in.resolveXuantanTable(xtReq("table", "casid"))
			if err == nil && r != nil {
				results[idx] = r.Address
			}
		}(i)
	}
	wg.Wait()

	for i, r := range results {
		assert.NotEmpty(t, r, "goroutine %d", i)
		assert.Equal(t, results[0], r, "goroutine %d diverged", i)
	}
	got, err := c.Get(ctx, "xt:dapr:bind:table:casid").Result()
	require.NoError(t, err)
	assert.Equal(t, results[0], got)
}

// room：N 个 room 在 M 个 host 上均衡分布(最大-最小 <= 1)。
func TestXuantanRoomBalanced(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	hosts := []string{"r1:9", "r2:9", "r3:9"}
	in := xtInflight("room", xtRing(hosts...))

	const n = 30
	for i := 0; i < n; i++ {
		require.NoError(t, c.SAdd(ctx, "xt:dapr:ids:room", strconv.Itoa(1000+i)).Err())
	}

	counts := map[string]int{}
	for i := 0; i < n; i++ {
		resp, handled, err := in.resolveXuantanRoom(xtReq("room", strconv.Itoa(1000+i)))
		require.True(t, handled)
		require.NoError(t, err)
		require.NotNil(t, resp)
		counts[resp.Address]++
	}

	mn, mx := n, 0
	for _, h := range hosts {
		ct := counts[h]
		if ct < mn {
			mn = ct
		}
		if ct > mx {
			mx = ct
		}
	}
	assert.LessOrEqualf(t, mx-mn, 1, "imbalanced: %+v", counts)
	assert.Equal(t, n, counts[hosts[0]]+counts[hosts[1]]+counts[hosts[2]])
}

// room：容量上限生效，超额返回可重试错误。
func TestXuantanRoomCapacity(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)
	xuantanRoomCapPerHost = 2 // 2 host * cap2 = 4 槽

	hosts := []string{"c1:9", "c2:9"}
	in := xtInflight("room", xtRing(hosts...))
	for i := 1; i <= 5; i++ {
		require.NoError(t, c.SAdd(ctx, "xt:dapr:ids:room", strconv.Itoa(i)).Err())
	}

	for i := 1; i <= 4; i++ {
		resp, handled, err := in.resolveXuantanRoom(xtReq("room", strconv.Itoa(i)))
		require.Truef(t, handled, "room %d", i)
		require.NoErrorf(t, err, "room %d", i)
		require.NotNilf(t, resp, "room %d", i)
	}

	// 第 5 个：全满
	resp, handled, err := in.resolveXuantanRoom(xtReq("room", "5"))
	assert.True(t, handled)
	assert.Nil(t, resp)
	assert.Error(t, err)
}

// room：统计时清理不在 ids set 的无效绑定。
func TestXuantanRoomInvalidCleanup(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	hosts := []string{"i1:9", "i2:9"}
	in := xtInflight("room", xtRing(hosts...))

	// 预置一个不在 ids set 的陈旧字段
	require.NoError(t, c.HSet(ctx, "xt:dapr:bind:room", "99999", "deadhost:9").Err())
	// 一个有效 room
	require.NoError(t, c.SAdd(ctx, "xt:dapr:ids:room", "1").Err())

	resp, handled, err := in.resolveXuantanRoom(xtReq("room", "1"))
	require.True(t, handled)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// 陈旧字段应已被 HDEL
	ex, err := c.HExists(ctx, "xt:dapr:bind:room", "99999").Result()
	require.NoError(t, err)
	assert.False(t, ex, "stale binding should be cleaned")
}

// Redis 故障：不降级回原生哈希，返回 handled=true + 可重试错误。
func TestXuantanRedisFailureNoFallback(t *testing.T) {
	if os.Getenv("XT_TEST_REDIS_ADDR") == "" {
		t.Skip("XT_TEST_REDIS_ADDR not set; skip redis integration test")
	}
	// 指向一个无人监听的地址，制造 Redis 故障。
	bad := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs:       []string{"127.0.0.1:1"},
		DialTimeout: 200 * time.Millisecond,
	})
	defer bad.Close()
	xtSetup(bad)

	// table：GET 失败 -> handled=true + 错误，且不返回地址(未降级)。
	inT := xtInflight("table", xtRing("a:1", "b:1"))
	resp, handled, err := inT.resolveXuantanTable(xtReq("table", "x"))
	assert.True(t, handled, "must take over, not fall back to hashing")
	assert.Nil(t, resp)
	assert.Error(t, err)

	// room：脚本执行失败 -> handled=true + 错误。
	inR := xtInflight("room", xtRing("r1:1", "r2:1"))
	resp2, handled2, err2 := inR.resolveXuantanRoom(xtReq("room", "y"))
	assert.True(t, handled2, "must take over, not fall back to hashing")
	assert.Nil(t, resp2)
	assert.Error(t, err2)
}

// room：绑定的 host 失效后，重分配到存活 host。
func TestXuantanRoomDeadHostReassign(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	require.NoError(t, c.SAdd(ctx, "xt:dapr:ids:room", "7").Err())
	// 绑定到一个不在 ring 中的(已死)host
	require.NoError(t, c.HSet(ctx, "xt:dapr:bind:room", "7", "gone:9").Err())

	live := []string{"live1:9", "live2:9"}
	in := xtInflight("room", xtRing(live...))

	resp, handled, err := in.resolveXuantanRoom(xtReq("room", "7"))
	require.True(t, handled)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Contains(t, live, resp.Address)

	got, err := c.HGet(ctx, "xt:dapr:bind:room", "7").Result()
	require.NoError(t, err)
	assert.Equal(t, resp.Address, got)
}
