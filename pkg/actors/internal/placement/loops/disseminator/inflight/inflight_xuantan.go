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

// 玄滩(xuantan)自定义 actor 放置策略。
//
// 背景：Dapr 原生放置是一致性哈希——成员一变(扩容/滚动更新/崩溃)就按哈希
// 重排，已激活的 actor 可能被迁走。对"牌桌(table)"这类对局中绝不能迁移、且
// 一局可达数分钟的场景不可接受。
//
// 入口 resolveXuantan 按 actorType 分发到具体子策略(两个 actorType 集合，逗号分隔)：
//   - KEY_XT_PLACEMENT_STICKY_TYPE_TABLE 集合 -> resolveXuantanTable，牌桌粘性(本文件实现)；
//   - KEY_XT_PLACEMENT_STICKY_TYPE_ROOM  集合 -> resolveXuantanRoom，房间策略(最少负载+容量感知)。
//
// 牌桌(table)策略对受管 actor 类型改为：
//
//	① 首次分配：用 Dapr 现有的一致性哈希选一个 host；
//	② 持久化：把 actorID -> host(地址) 原子写入 Redis，TTL 3 小时；
//	③ 粘性：只要绑定的 host 仍在当前成员表(hash ring)里(存活)，就一直返回它，不重新分配；
//	④ 失效重分配：仅当绑定的 host 从成员表消失(失效)时，才用哈希重新选一个 host
//	   并以 CAS 方式原子替换 Redis 中的绑定。
//
// 性能：热路径用进程内本地缓存(i.xuantanCache) + ring 本地判活兜底——命中且 host
// 存活时零 Redis；仅在"本地未缓存 / 缓存过期 / 绑定 host 失效"时才访问 Redis。
//
// TTL：Redis 绑定与本地缓存条目均为 3 小时，且【不做命中续期】——TTL 仅用于自动
// 清理。牌桌一局 ~5 分钟，远小于 TTL，绝不会在对局期间过期(约束:actor 存活 < TTL)。
//
// 单激活保证：Redis 绑定是全集群唯一权威；首次写用 SET NX、失效替换用
// "仅当旧值匹配才覆盖"的 Lua CAS，确保并发的多个 daprd 收敛到同一个 host。
// 本地缓存每次都用 ring 复核存活，host 一旦失效即弃用缓存回到 Redis，不破坏单激活。
// 由于"存活键绝不重分配"，正常的扩容/滚动更新不会触发迁移。
//
// 启用方式：设置环境变量 KEY_XT_PLACEMENT_REDIS_ADDR 即开启；未设置时本策略
// 完全旁路(handled=false)，回退到 Dapr 原生哈希，行为与官方一致。
//
// 容错取向：一旦受管(已启用且命中策略类型)，Redis 故障不降级回原生哈希，而是返回
// ErrActorNoAddress 让上层重试——降级会破坏 table 粘性与 room 均衡/容量约束。

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/dapr/dapr/pkg/actors/api"
	"github.com/dapr/dapr/pkg/actors/internal/placement/loops"
	"github.com/dapr/dapr/pkg/messages"
	"github.com/dapr/dapr/pkg/placement/hashing"
)

// 策略种类(kind)，用于在单张映射表里区分 actorType 归属哪个子策略。
const (
	xtKindTable = 1 // 牌桌粘性策略
	xtKindRoom  = 2 // 房间策略(待实现)
)

const (
	// xuantanRedisOpTimeout 单次 Redis 操作超时；resolve 在热路径上，超时即异常
	xuantanRedisOpTimeout = 2 * time.Second

	// xuantanCacheGCInterval 本地缓存过期清理周期。
	// resolve 已对单条做惰性失效(命中时判 TTL/判活)，此处只为回收"再也不会被访问"
	// 的死条目(对局结束后不再有消息的牌桌)，避免本进程内长期堆积。
	xuantanCacheGCInterval = time.Minute

	// defaultXuantanBindTTL 牌桌 actorID->host 绑定在 Redis 与本地缓存的默认存活时间。
	// 一局通常 ~5 分钟，3 小时给足冗余；不做命中续期，TTL 仅用于自动清理。
	// 可用 KEY_XT_PLACEMENT_BIND_TTL 覆盖(Go duration 格式，如 "3h"、"90m")。
	defaultXuantanBindTTL = 3 * time.Hour

	defaultXuantanKeyPrefix = "xt:dapr:bind:"
	// defaultXuantanIdsPrefix room 有效 roomId 集合(SET)的 key 前缀，完整 key 为
	// <prefix><actorType>，由外部业务进程维护(增删 roomId)。
	defaultXuantanIdsPrefix = "xt:dapr:ids:"

	// defaultXuantanTableType / defaultXuantanRoomType 各策略默认绑定的 actorType 列表
	// (逗号分隔)；环境变量未设置时使用。
	defaultXuantanTableType = "table_py,table"
	defaultXuantanRoomType  = "room_py,room"

	envXuantanRedisAddr   = "KEY_XT_PLACEMENT_REDIS_ADDR"     // 逗号分隔，多地址即集群/哨兵
	envXuantanRedisPasswd = "KEY_XT_PLACEMENT_REDIS_PASSWORD" //nolint:gosec
	envXuantanRedisDB     = "KEY_XT_PLACEMENT_REDIS_DB"
	envXuantanKeyPrefix   = "KEY_XT_PLACEMENT_KEY_PREFIX"
	envXuantanIdsPrefix   = "KEY_XT_PLACEMENT_IDS_PREFIX" // room 有效 id 集合(SET) key 前缀
	envXuantanBindTTL     = "KEY_XT_PLACEMENT_BIND_TTL"   // Go duration 格式，如 "3h"、"90m"

	// 各策略绑定的 actorType 集合(逗号分隔列表)：留空则该策略不接管任何类型(关闭)。
	envXuantanTableType = "KEY_XT_PLACEMENT_STICKY_TYPE_TABLE" // 牌桌粘性策略，逗号分隔
	envXuantanRoomType  = "KEY_XT_PLACEMENT_STICKY_TYPE_ROOM"  // 房间策略，逗号分隔

	// envXuantanRoomCap 每个 host 的 room 容量上限(= pypy worker 槽数)；0/未设=不限。
	envXuantanRoomCap = "KEY_XT_PLACEMENT_ROOM_CAP_PER_HOST"
)

var (
	xuantanOnce      sync.Once
	xuantanRDB       redis.UniversalClient
	xuantanKeyPrefix string
	xuantanIdsPrefix string

	// xuantanTypeKinds 是 actorType -> 策略种类(kind) 的映射表，在 xuantanInit 中初始化。
	// 规模极小(table 至多 3、room 至多 4)，故用切片线性扫描：相比 map 省去哈希/分配，
	// 短字符串少量比较反而更快、更省内存。空表表示无任何受管类型(整体旁路)。
	xuantanTypeKinds []xuantanTypeKind

	// xuantanRoomCapPerHost 每 host 的 room 容量上限；0=不限。由 xuantanInit 读环境变量。
	xuantanRoomCapPerHost int

	// xuantanBindTTL 实际生效的绑定 TTL，在 xuantanInit 中由环境变量初始化，
	// 缺省/非法时回退 defaultXuantanBindTTL。
	xuantanBindTTL = defaultXuantanBindTTL

	// xuantanCache 进程级本地绑定缓存：key "type:id" -> xuantanCacheEntry。
	// 命中且 host 仍在 ring(存活) 时直接返回，避免热路径每条消息打 Redis。
	// sync.Map 零值即可用，并发安全；与 xuantanRDB 等同为进程级全局配置。
	xuantanCache sync.Map
)

// xuantanTypeKind 把一个 actorType 关联到其策略种类(xtKind*)。
type xuantanTypeKind struct {
	typ  string
	kind int
}

// xuantanCacheEntry 本地绑定缓存条目。
//   - table: at 记录写入时刻，条目在 xuantanBindTTL 后由 GC 自动失效(与 Redis TTL 对齐)，
//     避免已结束牌桌的条目无限堆积；
//   - room : 无 TTL，GC 不按时间清理(全量 room <1000，规模有界)，仅靠 ring 判活惰性淘汰。
type xuantanCacheEntry struct {
	host string
	kind int
	at   time.Time
}

// xuantanBindScript 原子地"创建或按旧值 CAS 替换"绑定，并返回最终生效的 host 地址。
//
//	KEYS[1] = 绑定 key
//	ARGV[1] = expectedOld（调用方读到的旧值；新建时传 ""）
//	ARGV[2] = newHost（哈希选出的新 host 地址）
//	ARGV[3] = ttl 秒
//
// 语义：
//   - key 不存在            -> 写入 newHost（等价 SET NX），返回 newHost；
//   - 当前值 == expectedOld  -> 覆盖为 newHost（失效重分配的 CAS），返回 newHost；
//   - 否则（已被他人改写）    -> 不动，返回当前值，让调用方采用既有权威绑定。
var xuantanBindScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur == false then
    redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
    return ARGV[2]
end
if cur == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
    return ARGV[2]
end
return cur
`)

// xuantanRoomBindScript 房间(room)的"最少负载 + 容量感知 + 粘性"原子分配。
// 全量 room 数很小(<1000)，整张绑定 hash 一次 HGETALL 在脚本内统计即可，无需独立计数器。
//
//	KEYS[1] = room 绑定 hash    xt:dapr:bind:<actorType>(field=<roomId>, value=hostAddr，无 TTL)
//	KEYS[2] = 有效 roomId 集合   xt:dapr:ids:<actorType>(SET，由外部业务维护)
//	ARGV[1] = field(本次 room 的 roomId)
//	ARGV[2] = capPerHost(每 host 容量上限；<=0 表示不限)
//	ARGV[3..] = 当前存活 host 地址列表(daprd 从 ring 读出)
//
// 语义：
//   - 已有绑定且其 host 仍存活            -> 直接返回该 host(粘性，绝不迁移)；
//   - 统计负载时顺带清理：bind 里的 roomId 若已不在 ids set(被业务删除) -> 立即 HDEL，
//     既释放容量又自清理无效绑定；死 host 上的有效 room 不计入负载(待其被请求时重分配)；
//   - 无绑定 / 绑定 host 已失效            -> 在"存活且未满容量"的 host 中选当前承载最少者
//     (平手按地址名定序)，HSET 覆盖写入并返回；
//   - 所有存活 host 均已满              -> 返回 ”，由上层报错重试。
var xuantanRoomBindScript = redis.NewScript(`
local field = ARGV[1]
local cap = tonumber(ARGV[2])
local alive = {}
for i = 3, #ARGV do alive[ARGV[i]] = 0 end

local cur = redis.call('HGET', KEYS[1], field)
if cur and alive[cur] ~= nil then
    return cur
end

local all = redis.call('HGETALL', KEYS[1])
for i = 1, #all, 2 do
    local rid = all[i]
    local h = all[i + 1]
    if redis.call('SISMEMBER', KEYS[2], rid) == 0 then
        redis.call('HDEL', KEYS[1], rid)
    elseif alive[h] ~= nil then
        alive[h] = alive[h] + 1
    end
end

local best, bestCount
for i = 3, #ARGV do
    local h = ARGV[i]
    local c = alive[h]
    if cap <= 0 or c < cap then
        if best == nil or c < bestCount or (c == bestCount and h < best) then
            best = h
            bestCount = c
        end
    end
end
if best == nil then return '' end
redis.call('HSET', KEYS[1], field, best)
return best
`)

// xuantanInit 惰性初始化 Redis 客户端与受管类型集合（进程内仅一次）。
// 未配置 KEY_XT_PLACEMENT_REDIS_ADDR 时 xuantanRDB 保持 nil，策略整体旁路。
func xuantanInit() {
	xuantanOnce.Do(func() {
		addr := strings.TrimSpace(os.Getenv(envXuantanRedisAddr))
		if addr == "" {
			log.Info("xuantan placement: disabled (no KEY_XT_PLACEMENT_REDIS_ADDR), using stock hashing")
			return
		}

		xuantanTypeKinds = nil
		xuantanTypeKinds = xuantanAppendTypes(xuantanTypeKinds, envXuantanTableType, defaultXuantanTableType, xtKindTable)
		xuantanTypeKinds = xuantanAppendTypes(xuantanTypeKinds, envXuantanRoomType, defaultXuantanRoomType, xtKindRoom)

		xuantanKeyPrefix = strings.TrimSpace(os.Getenv(envXuantanKeyPrefix))
		if xuantanKeyPrefix == "" {
			xuantanKeyPrefix = defaultXuantanKeyPrefix
		}

		xuantanIdsPrefix = strings.TrimSpace(os.Getenv(envXuantanIdsPrefix))
		if xuantanIdsPrefix == "" {
			xuantanIdsPrefix = defaultXuantanIdsPrefix
		}

		xuantanBindTTL = defaultXuantanBindTTL
		if v := strings.TrimSpace(os.Getenv(envXuantanBindTTL)); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				xuantanBindTTL = d
			} else {
				log.Warnf("xuantan placement: invalid %s=%q, fallback to %s", envXuantanBindTTL, v, defaultXuantanBindTTL)
			}
		}

		xuantanRoomCapPerHost = 0
		if v := strings.TrimSpace(os.Getenv(envXuantanRoomCap)); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n >= 0 {
				xuantanRoomCapPerHost = n
			} else {
				log.Warnf("xuantan placement: invalid %s=%q, fallback to unlimited", envXuantanRoomCap, v)
			}
		}

		db := 0
		if v := strings.TrimSpace(os.Getenv(envXuantanRedisDB)); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				db = n
			}
		}

		xuantanRDB = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:    strings.Split(addr, ","),
			Password: os.Getenv(envXuantanRedisPasswd),
			DB:       db,
		})
		go xuantanCacheGC()

		log.Infof("xuantan placement: enabled, types=%v, redis=%s db=%d ttl=%s roomCap=%d bindPrefix=%q idsPrefix=%q",
			xuantanTypeKinds, addr, db, xuantanBindTTL, xuantanRoomCapPerHost, xuantanKeyPrefix, xuantanIdsPrefix)
	})
}

// xuantanAppendTypes 解析逗号分隔的 actorType 列表(环境变量为空时用 def)，
// 以给定 kind 追加到映射表 dst 并返回。
func xuantanAppendTypes(dst []xuantanTypeKind, env, def string, kind int) []xuantanTypeKind {
	v := strings.TrimSpace(os.Getenv(env))
	if v == "" {
		v = def
	}
	for _, t := range strings.Split(v, ",") {
		if t = strings.TrimSpace(t); t != "" {
			dst = append(dst, xuantanTypeKind{typ: t, kind: kind})
		}
	}
	return dst
}

// xuantanKindOf 线性扫描映射表，返回 actorType 的策略种类(xtKind*)；未受管返回 0。
// 表很小(≤7 项)，线性扫描比 map 更省更快。
func xuantanKindOf(actorType string) int {
	for _, e := range xuantanTypeKinds {
		if e.typ == actorType {
			return e.kind
		}
	}
	return 0
}

// xuantanCacheGC 每 xuantanCacheGCInterval 扫描一次本地缓存，删除超过 TTL 的死条目。
// 仅在策略启用时由 xuantanInit 启动一次，随进程生命周期常驻。
func xuantanCacheGC() {
	t := time.NewTicker(xuantanCacheGCInterval)
	defer t.Stop()
	for range t.C {
		now := time.Now()
		xuantanCache.Range(func(k, v any) bool {
			// 仅按时间回收 table 条目；room 无 TTL，规模有界，靠 ring 判活惰性淘汰。
			if e, ok := v.(xuantanCacheEntry); ok && e.kind == xtKindTable && now.Sub(e.at) > xuantanBindTTL {
				xuantanCache.Delete(k)
			}
			return true
		})
	}
}

// resolveXuantan 是自定义放置策略的统一入口/分发器。返回 (resp, handled, err)：
//   - handled=false：本策略不接管该请求（未启用 / actorType 无对应策略），
//     调用方应继续走 Dapr 原生哈希 resolve；
//   - handled=true ：某子策略已给出结果（resp 或 err 之一有效）。
//     注意：Redis 故障也返回 handled=true + ErrActorNoAddress(可重试)，而非降级回原生哈希——
//     降级会破坏粘性(table 迁移)/均衡与容量(room)，故宁可重试也不回退。
//
// 按 actorType 分发到具体策略：table -> resolveXuantanTable；room -> resolveXuantanRoom。
func (i *Inflight) resolveXuantan(req *api.LookupActorRequest) (*api.LookupActorResponse, bool, error) {
	if xuantanRDB == nil {
		return nil, false, nil // 未启用
	}
	switch xuantanKindOf(req.ActorType) {
	case xtKindTable:
		return i.resolveXuantanTable(req)
	case xtKindRoom:
		return i.resolveXuantanRoom(req)
	default:
		return nil, false, nil // 非受管类型
	}
}

// resolveXuantanTable 牌桌(table)粘性放置策略：初次按哈希分配并写 Redis(TTL)，
// 之后只要绑定 host 仍存活就一直返回它，host 失效才用哈希 + CAS 重分配。详见文件头注释。
func (i *Inflight) resolveXuantanTable(req *api.LookupActorRequest) (*api.LookupActorResponse, bool, error) {
	ring, ok := i.hashTable.Entries[req.ActorType]
	if !ok {
		return nil, true, messages.ErrActorNoAddress.WithFormat(req.ActorKey())
	}

	cacheKey := req.ActorType + ":" + req.ActorID

	// ⓪ 本地缓存命中且 host 仍存活 -> 直接返回，热路径零 Redis。
	if v, ok := xuantanCache.Load(cacheKey); ok {
		if e, _ := v.(xuantanCacheEntry); e.host != "" && time.Since(e.at) <= xuantanBindTTL {
			if h, alive := i.xuantanRingHost(ring, e.host); alive {
				return i.xuantanResp(h), true, nil
			}
		}
		// 条目过期或绑定 host 已失效：清掉，回落 Redis 重取/重分配。
		xuantanCache.Delete(cacheKey)
	}

	ctx, cancel := context.WithTimeout(context.Background(), xuantanRedisOpTimeout)
	defer cancel()
	key := xuantanKeyPrefix + cacheKey

	// ① 读现有绑定：存活则粘住(不续期)，并回填本地缓存。
	prev, err := xuantanRDB.Get(ctx, key).Result()
	switch {
	case err == nil && prev != "":
		if h, alive := i.xuantanRingHost(ring, prev); alive {
			xuantanCacheStore(cacheKey, prev, xtKindTable)
			return i.xuantanResp(h), true, nil
		}
		// 绑定的 host 已失效 -> 走 ② 用 expectedOld=prev 做 CAS 重分配。
	case err != nil && !errors.Is(err, redis.Nil):
		// Redis 故障：不降级到原生哈希(会破坏粘性导致迁移)，返回可重试错误等待重试。
		msg := fmt.Sprintf("xuantan placement: %s %q get failed: %v", req.ActorKey(), key, err)
		log.Warn(msg)
		return nil, true, messages.ErrActorNoAddress.WithFormat(msg)
	default:
		prev = "" // 无绑定（redis.Nil 或空值）
	}

	// ② 用 Dapr 原生哈希选新 host。
	host, err := ring.GetHost(req.ActorID)
	if err != nil {
		return nil, true, err
	}

	// ③ 原子创建或 CAS 替换，拿到最终权威绑定。
	ttl := strconv.Itoa(int(xuantanBindTTL.Seconds()))
	res, err := xuantanBindScript.Run(ctx, xuantanRDB, []string{key}, prev, host.Name, ttl).Result()
	if err != nil {
		// Redis 故障：不降级，返回可重试错误等待重试。
		msg := fmt.Sprintf("xuantan placement: %s %q bind failed: %v", req.ActorKey(), key, err)
		log.Warn(msg)
		return nil, true, messages.ErrActorNoAddress.WithFormat(msg)
	}
	finalName, _ := res.(string)

	// ④ 采用权威绑定并回填本地缓存。
	if finalName == host.Name {
		xuantanCacheStore(cacheKey, finalName, xtKindTable)
		return i.xuantanResp(host), true, nil
	}
	if h, alive := i.xuantanRingHost(ring, finalName); alive {
		xuantanCacheStore(cacheKey, finalName, xtKindTable)
		return i.xuantanResp(h), true, nil
	}
	// 权威绑定指向的 host 也已失效（极少见的下发过渡态）：返回可重试错误，
	// 让上层 resiliency 重试，下一次会重新做 CAS 重分配。
	return nil, true, messages.ErrActorNoAddress.WithFormat(req.ActorKey())
}

// resolveXuantanRoom 房间(room)放置策略：最少负载 + 容量感知 + 粘性不迁移、无 TTL。
//
// 与 table 的差异：
//   - 选址不用一致性哈希(roomId 是 <1000 的稀疏小集合，哈希会失衡，且 1 room=1 pypy
//     worker，每 host 有硬容量上限)，改为在存活且未满的 host 中选"当前承载最少者"；
//   - 绑定无 TTL，常驻；清理靠 host 失效时的重分配，或外部 roomId owner 进程 HDEL；
//   - 扩容(新 host)不自动迁移既有 room，新容量仅承接后续(重)分配，随失效逐步均衡。
//
// 稳态同 table：本地缓存命中 + ring 判活，零 Redis；(重)分配仅在首次/绑定 host 失效时发生。
func (i *Inflight) resolveXuantanRoom(req *api.LookupActorRequest) (*api.LookupActorResponse, bool, error) {
	ring, ok := i.hashTable.Entries[req.ActorType]
	if !ok {
		return nil, true, messages.ErrActorNoAddress.WithFormat(req.ActorKey())
	}

	cacheKey := req.ActorType + ":" + req.ActorID

	// ⓪ 本地缓存命中且 host 仍存活 -> 直接返回(room 不按时间失效，仅 ring 判活)。
	if v, ok := xuantanCache.Load(cacheKey); ok {
		if e, _ := v.(xuantanCacheEntry); e.host != "" {
			if h, alive := i.xuantanRingHost(ring, e.host); alive {
				return i.xuantanResp(h), true, nil
			}
		}
		xuantanCache.Delete(cacheKey)
	}

	// 枚举当前存活 host 作为候选(负载均衡的候选集)。
	hosts := i.xuantanRingHosts(ring)
	if len(hosts) == 0 {
		return nil, true, messages.ErrActorNoAddress.WithFormat(req.ActorKey())
	}

	ctx, cancel := context.WithTimeout(context.Background(), xuantanRedisOpTimeout)
	defer cancel()
	hashKey := xuantanKeyPrefix + req.ActorType // 每 actorType 一张绑定 hash，无 TTL
	idsKey := xuantanIdsPrefix + req.ActorType  // 外部业务维护的有效 roomId 集合(SET)

	argv := make([]interface{}, 0, len(hosts)+2)
	argv = append(argv, req.ActorID, strconv.Itoa(xuantanRoomCapPerHost)) // field=roomId
	for _, h := range hosts {
		argv = append(argv, h)
	}
	res, err := xuantanRoomBindScript.Run(ctx, xuantanRDB, []string{hashKey, idsKey}, argv...).Result()
	if err != nil {
		// Redis 故障：不降级到原生哈希(哈希会失衡/超容)，返回可重试错误等待重试。
		msg := fmt.Sprintf("xuantan placement: %s room bind failed: %v", req.ActorKey(), err)
		log.Warn(msg)
		return nil, true, messages.ErrActorNoAddress.WithFormat(msg)
	}
	name, _ := res.(string)
	if name == "" {
		// 所有存活 host 均已满容量：报可重试错误，等待扩容/有 room 释放后重试。
		log.Warnf("xuantan placement: %s no host under cap=%d (alive=%d)", req.ActorKey(), xuantanRoomCapPerHost, len(hosts))
		return nil, true, messages.ErrActorNoAddress.WithFormat(req.ActorKey())
	}

	if h, alive := i.xuantanRingHost(ring, name); alive {
		xuantanCacheStore(cacheKey, name, xtKindRoom)
		return i.xuantanResp(h), true, nil
	}
	// 选中的 host 恰好在并发下发中失效：返回可重试错误，下次重选。
	return nil, true, messages.ErrActorNoAddress.WithFormat(req.ActorKey())
}

// xuantanRingHost 在指定 actorType 的一致性哈希环里按 host 地址查 *Host，
// 命中即代表该 host 仍是当前成员（存活）。借助 Consistent.ReadInternals 读 loadMap，
// 无需修改 hashing 包。
func (i *Inflight) xuantanRingHost(ring *hashing.Consistent, name string) (*hashing.Host, bool) {
	var found *hashing.Host
	ring.ReadInternals(func(_ map[uint64]string, _ []uint64, loadMap map[string]*hashing.Host, _ int64) {
		if h, ok := loadMap[name]; ok {
			found = h
		}
	})
	return found, found != nil
}

// xuantanRingHosts 枚举指定 actorType 一致性哈希环里当前所有存活 host 的地址。
// 供 room 策略作为负载均衡的候选集。
func (i *Inflight) xuantanRingHosts(ring *hashing.Consistent) []string {
	var hosts []string
	ring.ReadInternals(func(_ map[uint64]string, _ []uint64, loadMap map[string]*hashing.Host, _ int64) {
		hosts = make([]string, 0, len(loadMap))
		for name := range loadMap {
			hosts = append(hosts, name)
		}
	})
	return hosts
}

// xuantanResp 由 *Host 构造与原生 resolve 一致的查找响应（含本地性判定）。
func (i *Inflight) xuantanResp(h *hashing.Host) *api.LookupActorResponse {
	return &api.LookupActorResponse{
		Address: h.Name,
		AppID:   h.AppID,
		Local:   loops.IsActorLocal(h.Name, i.hostname, i.port),
	}
}

// xuantanCacheStore 写入/刷新本地绑定缓存。仅在确实从 Redis 取到/写入权威绑定后调用。
// kind 决定 GC 是否按 TTL 回收(见 xuantanCacheEntry)。
func xuantanCacheStore(cacheKey, host string, kind int) {
	xuantanCache.Store(cacheKey, xuantanCacheEntry{host: host, kind: kind, at: time.Now()})
}
