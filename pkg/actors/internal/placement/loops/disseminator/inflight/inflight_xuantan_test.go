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
	xuantanTypeKinds = []xuantanTypeKind{{typ: "table", kind: xtKindTable}, {typ: "room", kind: xtKindRoom}}
	xtClearCache()
	xuantanDrainCache.Store(nil) // 清进程内 draining 缓存，避免跨用例复用陈旧快照
}

func xtReq(actorType, id string) *api.LookupActorRequest {
	return &api.LookupActorRequest{ActorType: actorType, ActorID: id}
}

// xtMarkDraining 把给定 host 写入排空索引 ZSET(score=now+10m，模拟其进入 block-shutdown)。
func xtMarkDraining(t *testing.T, c redis.UniversalClient, hosts ...string) {
	t.Helper()
	exp := float64(time.Now().Add(10 * time.Minute).UnixMilli())
	for _, h := range hosts {
		require.NoError(t, c.ZAdd(context.Background(), xuantanDrainingKey, redis.Z{Score: exp, Member: h}).Err())
	}
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
	got := xuantanAppendTypes(nil, []string{" a ", " b ", "", " c "}, []string{"def"}, xtKindTable)
	require.Len(t, got, 3)
	assert.Equal(t, xuantanTypeKind{typ: "a", kind: xtKindTable}, got[0])
	assert.Equal(t, xuantanTypeKind{typ: "b", kind: xtKindTable}, got[1])
	assert.Equal(t, xuantanTypeKind{typ: "c", kind: xtKindTable}, got[2])

	// 配置为空时回退默认列表
	got2 := xuantanAppendTypes(nil, nil, []string{"x", "y"}, xtKindRoom)
	require.Len(t, got2, 2)
	assert.Equal(t, xuantanTypeKind{typ: "x", kind: xtKindRoom}, got2[0])
	assert.Equal(t, xuantanTypeKind{typ: "y", kind: xtKindRoom}, got2[1])

	// 追加语义：在已有切片后继续追加
	merged := xuantanAppendTypes(got2, []string{" a ", " b ", "", " c "}, []string{"def"}, xtKindTable)
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

// table：预标记有效 -> 首次哈希分配 -> 持久化 -> 粘性 -> host 失效后拒绝(不迁移)。
func TestXuantanTableAssignStickyReject(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	h1, h2, h3 := "10.0.0.1:7", "10.0.0.2:7", "10.0.0.3:7"
	in := xtInflight("table", xtRing(h1, h2, h3))
	req := xtReq("table", "100001")

	// 业务预标记：绑定 key 置为有效标记 "1"(模拟 MarkTableValid)。
	require.NoError(t, c.Set(ctx, "xt:dapr:bind:table:100001", xuantanTableValidMark, 0).Err())

	// 首次分配：门禁通过(值为 "1") -> CAS 分配 host。
	resp, handled, err := in.resolveXuantanTable(req)
	require.True(t, handled)
	require.NoError(t, err)
	require.NotNil(t, resp)
	first := resp.Address
	assert.Contains(t, []string{h1, h2, h3}, first)

	// 已持久化到 Redis(值由 "1" 变为 host)
	got, err := c.Get(ctx, "xt:dapr:bind:table:100001").Result()
	require.NoError(t, err)
	assert.Equal(t, first, got)

	// 粘性：清本地缓存强制走 Redis，仍返回同一 host
	xtClearCache()
	resp2, _, err := in.resolveXuantanTable(req)
	require.NoError(t, err)
	assert.Equal(t, first, resp2.Address)

	// 杀掉被分配的 host -> 牌桌绝不迁移：值为失效 host(非 "1")即视为无效，报错，不重分配。
	live := xtExclude(first, h1, h2, h3)
	in = xtInflight("table", xtRing(live...))
	xtClearCache()
	resp3, handled3, err3 := in.resolveXuantanTable(req)
	assert.True(t, handled3)
	assert.Error(t, err3)
	assert.Nil(t, resp3)

	// Redis 中的绑定不被改写(仍指向失效 host)。
	got2, err := c.Get(ctx, "xt:dapr:bind:table:100001").Result()
	require.NoError(t, err)
	assert.Equal(t, first, got2)
}

// table：未预标记(绑定 key 不存在)的 tableId -> 有效性门禁直接拒绝，且不建立任何绑定。
func TestXuantanTableNotMarkedRejected(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	in := xtInflight("table", xtRing("a:1", "b:1"))

	// 不预标记 tableId "555"：应被门禁直接拒绝。
	resp, handled, err := in.resolveXuantanTable(xtReq("table", "555"))
	require.True(t, handled)
	require.Error(t, err)
	require.Nil(t, resp)

	// 无效 tableId 不应建立绑定。
	ex, err := c.Exists(ctx, "xt:dapr:bind:table:555").Result()
	require.NoError(t, err)
	assert.EqualValues(t, 0, ex, "no binding should be created for un-marked table")
}

// table：并发首次分配收敛到同一 host，Redis 仅一个权威值。
func TestXuantanTableConcurrentConverge(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	// 业务预标记 tableId "casid" 为有效，才允许并发分配。
	require.NoError(t, c.Set(ctx, "xt:dapr:bind:table:casid", xuantanTableValidMark, 0).Err())

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

// room：roomId 不在有效集合内 -> 有效性门禁直接拒绝(handled=true + 错误)，且不建立任何绑定。
func TestXuantanRoomNotInIdsRejected(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	hosts := []string{"n1:9", "n2:9"}
	in := xtInflight("room", xtRing(hosts...))

	// 不把 roomId "42" 加入 xt:dapr:ids:room：应被门禁直接拒绝。
	resp, handled, err := in.resolveXuantanRoom(xtReq("room", "42"))
	require.True(t, handled)
	require.Error(t, err)
	require.Nil(t, resp)

	// 无效 roomId 不应建立绑定。
	ex, err := c.HExists(ctx, "xt:dapr:bind:room", "42").Result()
	require.NoError(t, err)
	assert.False(t, ex, "no binding should be created for invalid room")
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

// ---------------------------------------------------------------------------
// 排空(draining)：新分配剔除正在排空的 host，粘性/既有绑定不受影响，空候选回退。
// ---------------------------------------------------------------------------

// table：首次分配时，若哈希首选 host 正在排空，则改选到非排空存活 host。
func TestXuantanTableAvoidsDrainingHost(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	h1, h2, h3 := "10.0.0.1:7", "10.0.0.2:7", "10.0.0.3:7"
	in := xtInflight("table", xtRing(h1, h2, h3))
	req := xtReq("table", "200001")

	require.NoError(t, c.Set(ctx, "xt:dapr:bind:table:200001", xuantanTableValidMark, 0).Err())

	// 把哈希首选 host 标记为排空，分配应避开它。
	pref, err := in.hashTable.Entries["table"].GetHost("200001")
	require.NoError(t, err)
	xtMarkDraining(t, c, pref.Name)

	resp, handled, err := in.resolveXuantanTable(req)
	require.True(t, handled)
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.NotEqual(t, pref.Name, resp.Address, "should avoid draining host")
	assert.Contains(t, []string{h1, h2, h3}, resp.Address)
}

// table：全部 host 都在排空(=全体服务下线) -> 拒绝分配新桌，报可重试错误，且不写绑定。
func TestXuantanTableAllDrainingRejected(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	h1, h2 := "10.0.1.1:7", "10.0.1.2:7"
	in := xtInflight("table", xtRing(h1, h2))
	require.NoError(t, c.Set(ctx, "xt:dapr:bind:table:200002", xuantanTableValidMark, 0).Err())
	xtMarkDraining(t, c, h1, h2)

	resp, handled, err := in.resolveXuantanTable(xtReq("table", "200002"))
	require.True(t, handled)
	require.Error(t, err)
	require.Nil(t, resp)

	// 拒绝分配时不得把有效标记覆盖为 host（绑定仍为 "1"）。
	got, err := c.Get(ctx, "xt:dapr:bind:table:200002").Result()
	require.NoError(t, err)
	assert.Equal(t, xuantanTableValidMark, got, "must not bind to a draining host")
}

// room：正在排空的 host 上的既有 room 保持粘性(不迁移)，新 room 一律落到非排空 host。
func TestXuantanRoomAvoidsDrainingKeepsSticky(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	h1, h2 := "rd1:9", "rd2:9"
	in := xtInflight("room", xtRing(h1, h2))

	// room 500 已绑定到 h1；把 h1 标记排空后仍应粘在 h1(不迁移)。
	require.NoError(t, c.SAdd(ctx, "xt:dapr:ids:room", "500").Err())
	require.NoError(t, c.HSet(ctx, "xt:dapr:bind:room", "500", h1).Err())
	xtMarkDraining(t, c, h1)

	resp, _, err := in.resolveXuantanRoom(xtReq("room", "500"))
	require.NoError(t, err)
	assert.Equal(t, h1, resp.Address, "existing room stays on draining host (sticky)")

	// 新建 room 一律避开排空的 h1，落到 h2。
	for i := 501; i <= 506; i++ {
		id := strconv.Itoa(i)
		require.NoError(t, c.SAdd(ctx, "xt:dapr:ids:room", id).Err())
		r, _, err := in.resolveXuantanRoom(xtReq("room", id))
		require.NoError(t, err)
		assert.Equalf(t, h2, r.Address, "new room %s must avoid draining host", id)
	}
}

// room：全部 host 都在排空(=全体服务下线) -> 拒绝分配新 room，报可重试错误，且不建绑定。
func TestXuantanRoomAllDrainingRejected(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	h1, h2 := "ra1:9", "ra2:9"
	in := xtInflight("room", xtRing(h1, h2))
	require.NoError(t, c.SAdd(ctx, "xt:dapr:ids:room", "900").Err())
	xtMarkDraining(t, c, h1, h2)

	resp, handled, err := in.resolveXuantanRoom(xtReq("room", "900"))
	require.True(t, handled)
	require.Error(t, err)
	require.Nil(t, resp)

	// 拒绝分配时不得建立绑定。
	ex, err := c.HExists(ctx, "xt:dapr:bind:room", "900").Result()
	require.NoError(t, err)
	assert.False(t, ex, "must not bind a room to a draining host")
}

// MarkSelfDraining：写入自身 host.Name 到排空 ZSET，且能被 xuantanDrainingHosts 读回。
func TestXuantanMarkSelfDraining(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	oldSelf := xuantanSelfHost
	t.Cleanup(func() { xuantanSelfHost = oldSelf })
	xuantanSetSelfHost("10.9.9.9", "7000")

	require.NoError(t, MarkSelfDraining(ctx, 5*time.Minute))
	set := xuantanDrainingHosts(ctx)
	_, ok := set["10.9.9.9:7000"]
	assert.True(t, ok, "self host should be in draining set")
}

// 排空缓存(优化A)：首次读回填缓存；TTL 内即便 Redis 变了也返回旧快照（可容忍的 ≤TTL 陈旧）；
// 缓存重置后再读拿到最新。
func TestXuantanDrainCacheTTL(t *testing.T) {
	ctx := context.Background()
	c := xtRedis(t)
	defer c.Close()
	xtSetup(c)

	// 空集合也缓存：首读回 Redis 得空，写入快照。
	got := xuantanDrainingHostsCached(ctx)
	assert.Empty(t, got)

	// Redis 里新增一个 draining host，但 TTL 内缓存仍为旧（空）快照。
	xtMarkDraining(t, c, "h:1")
	assert.Empty(t, xuantanDrainingHostsCached(ctx), "within TTL should serve cached (stale) snapshot")

	// 重置缓存后再读 -> 拿到最新。
	xuantanDrainCache.Store(nil)
	got = xuantanDrainingHostsCached(ctx)
	_, ok := got["h:1"]
	assert.True(t, ok, "after cache reset should reflect latest draining set")
}
