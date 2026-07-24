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
// 入口 resolveXuantan 按 actorType 分发到具体子策略(受管类型来自配置文件的两个列表)：
//   - dapr.sticky_type_battle 列表 -> resolveXuantanBattle，牌桌粘性(本文件实现)；
//   - dapr.sticky_type_match  列表 -> resolveXuantanMatch，房间策略(最少负载)。
//
// 牌桌(table)策略对受管 actor 类型改为：
//
//	⓪ 有效性门禁：分配前先看绑定 key 是否被业务预标记为有效——业务在预建牌桌实例前把
//	   绑定 key 置为有效标记 "1"(见 core.IManager.MarkBattleValid)；key 不存在(未预标记)
//	   即视为无效 tableId，直接返回异常，绝不分配 host。杜绝任意 tableId 凭空激活牌桌；
//	① 本地算候选：门禁与选址前先在本地用一致性哈希选一个候选 host cand；若 cand 正在排空
//	   (命中 xt:dapr:draining 进程缓存)则在非排空存活 host 中确定性改选(仅影响新分配)；
//	② 单脚本一趟：以「门禁 + 粘性 + CAS(仅当值仍为 '1' 才覆盖为 cand)」的 Lua 原子完成，
//	   (重)分配只 1 次 Redis 往返；存活判定在 daprd 侧对返回值本地做，脚本无需知道 ring；
//	③ 粘性：脚本返回既有绑定 host，只要它仍在当前成员表(hash ring)里(存活)，就一直返回它，不重新分配；
//	④ host 失效：绑定的 host 从成员表消失(失效)即视为无效(牌桌绝不迁移、亦不重分配到新 host)，
//	   返回异常——host 失效意味着这局牌桌已不可恢复。
//
// 性能：热路径用进程内本地缓存(i.xuantanCache) + ring 本地判活兜底——命中且 host
// 存活时零 Redis；仅在"本地未缓存 / 缓存过期 / 绑定 host 失效"时才访问 Redis，且(重)分配只 1 次往返。
//
// TTL：Redis 绑定与本地缓存条目均为 BIND_TTL(默认 15m)，且【不做命中续期】——TTL 仅用于自动
// 清理。牌桌一局 ~5 分钟，小于 TTL，正常对局不会过期(约束:actor 存活 < TTL；活跃牌桌可由业务幂等续期)。
//
// 单激活保证：Redis 绑定是全集群唯一权威；首次分配用"仅当值仍为有效标记 '1' 才覆盖"的
// Lua CAS，确保并发的多个 daprd 收敛到同一个 host。本地缓存每次都用 ring 复核存活，
// host 一旦失效即弃用缓存回到 Redis，不破坏单激活。由于"存活键绝不重分配"，正常的
// 扩容/滚动更新不会触发迁移。
//
// 启用方式：设置环境变量 KEY_XT_PLACEMENT_CONFIG 指向共享配置文件(YAML，含顶层
// dapr: 段)即开启；未设置该路径、或文件里 dapr.redis.addresses 为空时，本策略
// 完全旁路(handled=false)，回退到 Dapr 原生哈希，行为与官方一致。配置格式与业务侧
// core/etc DaprConfig 完全一致(两侧读同一份文件，保证同一 Redis / 同一 key)。
//
// 容错取向：一旦受管(已启用且命中策略类型)，Redis 故障不降级回原生哈希，而是返回
// ErrActorNoAddress 让上层重试——降级会破坏 table 粘性与 room 均衡。

import (
	"context"
	"fmt"
	"hash/fnv"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"gopkg.in/yaml.v3"

	"github.com/dapr/dapr/pkg/actors/api"
	"github.com/dapr/dapr/pkg/actors/internal/placement/loops"
	"github.com/dapr/dapr/pkg/messages"
	"github.com/dapr/dapr/pkg/placement/hashing"
)

// 策略种类(kind)，用于在单张映射表里区分 actorType 归属哪个子策略。
const (
	xtKindBattle = 1 // 牌桌粘性策略（resolveXuantanBattle）
	xtKindMatch  = 2 // 房间策略：最少负载 + 粘性不迁移（resolveXuantanMatch）
)

const (
	// xuantanRedisOpTimeout 单次 Redis 操作超时；resolve 在热路径上，超时即异常
	xuantanRedisOpTimeout = 3 * time.Second

	// xuantanCacheGCInterval 本地缓存过期清理周期。
	// resolve 已对单条做惰性失效(命中时判 TTL/判活)，此处只为回收"再也不会被访问"
	// 的死条目(对局结束后不再有消息的牌桌)，避免本进程内长期堆积。
	xuantanCacheGCInterval = time.Minute

	// defaultXuantanBindTTL 牌桌 actorID->host 绑定在 Redis 与本地缓存的默认存活时间。
	// 一局通常 ~5 分钟，15 分钟给足冗余；不做命中续期，TTL 仅用于自动清理。
	// 可用配置文件 dapr.bind_ttl 覆盖(Go duration 格式，如 "15m"、"1h")。
	defaultXuantanBindTTL = 15 * time.Minute

	defaultXuantanKeyPrefix = "xt:dapr:bind:"
	// defaultXuantanIdsPrefix room 有效 roomId 集合(SET)的 key 前缀，完整 key 为
	// <prefix><actorType>，由外部业务进程维护(增删 roomId)。
	defaultXuantanIdsPrefix = "xt:dapr:ids:"

	// envXuantanConfig 指向共享配置文件路径(YAML，含顶层 dapr: 段)；本策略的唯一环境变量，
	// 未设置=放置策略整体关闭。配置格式与业务侧 core/etc DaprConfig 完全一致(见
	// inflight_xuantan.md)，两侧读同一份文件，保证「同一 Redis、同一 key」。所有参数均来自该文件。
	envXuantanConfig = "KEY_XT_PLACEMENT_CONFIG"

	// xuantanNotValid 是绑定脚本在「目标 id 无效」时返回的哨兵值，供 Go 侧识别并转成「无效」异常：
	//   - room：roomId 不在有效集合(ids set)         (xuantanMatchBindScript)；
	//   - table：tableId 未被业务预标记(绑定 key 不存在) (xuantanBindScript)。
	// 含前导 \0，不可能与真实 host 地址(ip:port)冲突；须与两段 Lua 脚本里的字面量一致。
	xuantanNotValid = "\x00xt-not-member"

	// xuantanBattleValidMark 是「有效牌桌」预标记值：业务在预建牌桌实例前把绑定 key 置为该值
	// (见 core.IManager.MarkBattleValid)，daprd 放置策略据此放行首次 host 分配(CAS 标记->host)；
	// 绑定 key 不存在(未预标记)即视为无效 tableId，直接拒绝。须与业务侧 tableValidMark 一致。
	xuantanBattleValidMark = "1"

	// defaultXuantanDrainingKey 排空索引 ZSET 的 key：member=host.Name(ring 地址)，score=过期时刻(ms)。
	// daprd 进入 block-shutdown 时把自身 host.Name 写入(MarkSelfDraining)，放置策略在(重)分配路径据此
	// 把「正在排空、即将退出」的 host 从候选剔除——只拦新分配，不动既有绑定/粘性；host 离开 ring 后靠 score 过期自清。
	defaultXuantanDrainingKey = "xt:dapr:draining"

	// defaultXuantanDrainTTL 排空自标记成员的默认存活时长(MarkSelfDraining 未给 ttl 时回退)。
	// 生产应传 blockShutdownDuration+余量，确保覆盖整个 block-shutdown 窗口，避免窗口内标记过期而本 host 重新被选中。
	defaultXuantanDrainTTL = 30 * time.Minute

	// xuantanDrainCacheTTL 排空集合进程内缓存的有效期：分配路径据此缓存 draining 视图，
	// 避免建桌高峰下每次分配都读一次 ZSET。取值越小越新鲜、越大越省 Redis；draining 是分钟级事件，1s 足够。
	xuantanDrainCacheTTL = time.Second
)

// defaultXuantanBattleType / defaultXuantanMatchType 各策略默认绑定的 actorType 列表；
// 配置文件对应字段为空时使用。须与业务侧 core/actor 默认一致。
var (
	defaultXuantanBattleType = []string{"battle_py", "battle"}
	defaultXuantanMatchType  = []string{"match_py", "match"}
)

// xuantanConfig 共享配置文件顶层 dapr: 段的解析视图；yaml key 须与业务侧 core/etc
// DaprConfig 完全一致(见 inflight_xuantan.md)。缺省值在 xuantanInit 里回落，与业务侧对齐。
type xuantanConfig struct {
	Redis            xuantanRedisConfig `yaml:"redis"`              // Redis 连接(格式同业务侧 infra.RedisConfig)
	KeyPrefix        string             `yaml:"key_prefix"`         // 绑定 key 前缀，默认 xt:dapr:bind:
	IdsPrefix        string             `yaml:"ids_prefix"`         // room 有效 id 集合(SET)前缀，默认 xt:dapr:ids:
	BindTTL          string             `yaml:"bind_ttl"`           // table 绑定 TTL(Go duration)，默认 15m
	StickyTypeBattle []string           `yaml:"sticky_type_battle"` // 走 battle(牌桌) 策略的 actorType 列表
	StickyTypeMatch  []string           `yaml:"sticky_type_match"`  // 走 match(房间) 策略的 actorType 列表
}

// xuantanRedisConfig 与业务侧 core/infra.RedisConfig 的 yaml 键完全一致，保证两侧共读同一份配置文件
// 的 dapr.redis 段。addresses 单条=单节点、多条=Cluster；空=放置整体关闭。
type xuantanRedisConfig struct {
	Addresses    []string `yaml:"addresses"`
	Username     string   `yaml:"username"`
	Password     string   `yaml:"password"` // nolint:gosec
	DB           int      `yaml:"db"`
	DialTimeout  string   `yaml:"dial_timeout"`  // Go duration，空缺省 2s
	ReadTimeout  string   `yaml:"read_timeout"`  // Go duration，空缺省 3s
	WriteTimeout string   `yaml:"write_timeout"` // Go duration，空缺省 3s
	PoolSize     int      `yaml:"pool_size"`
	MinIdleConns int      `yaml:"min_idle_conns"`
}

// loadXuantanConfig 读取共享配置文件并解出顶层 dapr: 段。
func loadXuantanConfig(path string) (xuantanConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return xuantanConfig{}, err
	}
	var aux struct {
		Dapr xuantanConfig `yaml:"dapr"`
	}
	if err := yaml.Unmarshal(data, &aux); err != nil {
		return xuantanConfig{}, err
	}
	return aux.Dapr, nil
}

var (
	xuantanOnce      sync.Once
	xuantanRDB       redis.UniversalClient
	xuantanKeyPrefix string
	xuantanIdsPrefix string

	// xuantanCacheGCStop 关停本地缓存 GC goroutine 的信号：xuantanInit 启动 GC，
	// MarkSelfDraining（进入 block-shutdown 优雅退出）经 xuantanStopCacheGC 关闭它，令 GC 及时收尾。
	xuantanCacheGCStop     = make(chan struct{})
	xuantanCacheGCStopOnce sync.Once

	// xuantanDrainingKey 排空索引 ZSET 的实际 key（xuantanInit 时置为 defaultXuantanDrainingKey）。
	xuantanDrainingKey = defaultXuantanDrainingKey

	// xuantanSelfHost 本 daprd 在 ring 中的 Host.Name（= net.JoinHostPort(hostname, port)），
	// 由 inflight.New 经 xuantanSetSelfHost 写入；进程内仅一个 disseminator Inflight，故用全局单值。
	// 供 MarkSelfDraining 在优雅退出时把「自己」写入排空索引。
	xuantanSelfHost string

	// xuantanTypeKinds 是 actorType -> 策略种类(kind) 的映射表，在 xuantanInit 中初始化。
	// 规模极小(table 至多 3、room 至多 4)，故用切片线性扫描：相比 map 省去哈希/分配，
	// 短字符串少量比较反而更快、更省内存。空表表示无任何受管类型(整体旁路)。
	xuantanTypeKinds []xuantanTypeKind

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

// xuantanBindScript 一趟原子完成「有效性门禁 + 粘性 + 有效标记 CAS 分配」，返回最终生效值。
// 调用方不再先做独立 GET：本地算好候选 cand 后只调本脚本一次，(重)分配从 2 次往返降到 1 次；
// 对返回值的「存活判定」由 daprd 侧本地做（脚本无需知道 ring）。
//
//	KEYS[1] = 绑定 key（xt:dapr:bind:<type>:<tableId>）
//	ARGV[1] = 有效标记 "1"（业务 MarkBattleValid 预写的待分配标记，作为 CAS 的 expectedOld）
//	ARGV[2] = cand（daprd 本地按哈希+排空过滤选出的候选 host 地址）
//	ARGV[3] = ttl 秒
//
// 语义（有效性门禁：牌桌必须先被业务预标记为有效才能分配 host）：
//   - key 不存在      -> 未预标记(或标记已过期)，返回哨兵 xuantanNotValid，上层报「无效 tableId」，绝不写入绑定；
//   - 当前值 == "1"   -> 已预标记待分配，CAS 覆盖为 cand，返回 cand（= 本次分配）；
//   - 否则（已是某 host 或脏值）-> 不动，原样返回当前值，由 daprd 判活：存活=粘性，失效=无效(绝不迁移)。
//
// 注意：不再"key 不存在即 SET NX 建绑定"——那会给未经业务预建的任意 tableId 分配 host。
var xuantanBindScript = redis.NewScript(`
local cur = redis.call('GET', KEYS[1])
if cur == false then
    return '\0xt-not-member'
end
if cur == ARGV[1] then
    redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
    return ARGV[2]
end
return cur
`)

// xuantanMatchBindScript 房间(room)的"最少负载 + 粘性"原子分配。
// 全量 room 数很小(<1000)，整张绑定 hash 一次 HGETALL 在脚本内统计即可，无需独立计数器。
//
//	KEYS[1] = room 绑定 hash    xt:dapr:bind:<actorType>(field=<roomId>, value=hostAddr，无 TTL)
//	KEYS[2] = 有效 roomId 集合   xt:dapr:ids:<actorType>(SET，由外部业务维护)
//	ARGV[1] = field(本次 room 的 roomId)
//	ARGV[2] = D(正在排空的 host 个数)
//	ARGV[3 .. 2+D]     = 正在排空的 host 地址列表(即将退出，选址时剔除，但仍在 ring 内故仍算存活)
//	ARGV[3+D .. #ARGV] = 当前全部存活 host 地址列表(daprd 从 ring 读出，含正在排空者)
//
// 语义：
//   - 有效性门禁(先于一切)：field 不在 ids set(无效 roomId) -> 返回哨兵 xuantanNotValid，
//     不建绑定；仅在此(重)分配路径校验，本地缓存命中路径不进本脚本、不校验；
//   - 已有绑定且其 host 仍存活(含正在排空者) -> 直接返回该 host(粘性，绝不迁移；排空 host 在离开 ring 前继续承载既有 room)；
//   - 统计负载时顺带清理：bind 里的 roomId 若已不在 ids set(被业务删除) -> 立即 HDEL，
//     既释放容量又自清理无效绑定；死 host 上的有效 room 不计入负载(待其被请求时重分配)；
//   - 无绑定 / 绑定 host 已失效 -> 在「非排空」存活 host 中选当前承载最少者(平手按地址名定序)，HSET 覆盖写入并返回；
//   - 无「非排空」存活 host（全在排空=全体服务下线）或无存活 host -> 返回 ”，由上层报错重试(绝不硬塞到正在排空的 host)。
var xuantanMatchBindScript = redis.NewScript(`
local field = ARGV[1]
if redis.call('SISMEMBER', KEYS[2], field) == 0 then
    return '\0xt-not-member'
end

local dcount = tonumber(ARGV[2])
local draining = {}
for i = 3, 2 + dcount do draining[ARGV[i]] = true end

local firstAlive = 3 + dcount
local alive = {}
for i = firstAlive, #ARGV do alive[ARGV[i]] = 0 end

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

local best, bestCount      -- 仅在「非排空」存活 host 里选承载最少者
for i = firstAlive, #ARGV do
    local h = ARGV[i]
    if not draining[h] then
        local c = alive[h]
        if best == nil or c < bestCount or (c == bestCount and h < best) then
            best = h
            bestCount = c
        end
    end
end
if best == nil then return '' end   -- 无「非排空」存活 host(全在排空=全体下线) -> 上层报错，不分配
redis.call('HSET', KEYS[1], field, best)
return best
`)

// xuantanInit 惰性初始化 Redis 客户端与受管类型集合（进程内仅一次）。
// 未配置 KEY_XT_PLACEMENT_CONFIG（或文件里 dapr.redis.addresses 为空 / 读取解析失败）时 xuantanRDB 保持 nil，
// 策略整体旁路，回退 Dapr 原生哈希。
func xuantanInit() {
	xuantanOnce.Do(func() {
		path := strings.TrimSpace(os.Getenv(envXuantanConfig))
		if path == "" {
			log.Info("xuantan placement: disabled (no KEY_XT_PLACEMENT_CONFIG), using stock hashing")
			return
		}
		cfg, err := loadXuantanConfig(path)
		if err != nil {
			log.Warnf("xuantan placement: load config %q failed: %v, using stock hashing", path, err)
			return
		}
		addrs := xuantanTrimNonEmpty(cfg.Redis.Addresses)
		if len(addrs) == 0 {
			log.Info("xuantan placement: disabled (dapr.redis.addresses empty), using stock hashing")
			return
		}

		xuantanTypeKinds = nil
		xuantanTypeKinds = xuantanAppendTypes(xuantanTypeKinds, cfg.StickyTypeBattle, defaultXuantanBattleType, xtKindBattle)
		xuantanTypeKinds = xuantanAppendTypes(xuantanTypeKinds, cfg.StickyTypeMatch, defaultXuantanMatchType, xtKindMatch)

		xuantanKeyPrefix = strings.TrimSpace(cfg.KeyPrefix)
		if xuantanKeyPrefix == "" {
			xuantanKeyPrefix = defaultXuantanKeyPrefix
		}

		xuantanIdsPrefix = strings.TrimSpace(cfg.IdsPrefix)
		if xuantanIdsPrefix == "" {
			xuantanIdsPrefix = defaultXuantanIdsPrefix
		}

		xuantanBindTTL = defaultXuantanBindTTL
		if v := strings.TrimSpace(cfg.BindTTL); v != "" {
			if d, err := time.ParseDuration(v); err == nil && d > 0 {
				xuantanBindTTL = d
			} else {
				log.Warnf("xuantan placement: invalid bind_ttl=%q, fallback to %s", v, defaultXuantanBindTTL)
			}
		}

		// 密码为空或仍是未渲染的 ${REDIS_PASSWORD} 占位符时，回退读环境变量（与业务侧 infra 一致）。
		password := xuantanResolveSecret(cfg.Redis.Password, "REDIS_PASSWORD")

		xuantanRDB = redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:        addrs,
			Username:     cfg.Redis.Username,
			Password:     password,
			DB:           cfg.Redis.DB,
			DialTimeout:  xuantanParseDur(cfg.Redis.DialTimeout, 2*time.Second),
			ReadTimeout:  xuantanParseDur(cfg.Redis.ReadTimeout, 3*time.Second),
			WriteTimeout: xuantanParseDur(cfg.Redis.WriteTimeout, 3*time.Second),
			PoolSize:     cfg.Redis.PoolSize,
			MinIdleConns: cfg.Redis.MinIdleConns,
		})

		// 启动即 PING 一次，尽早暴露连接/认证类异常（如密码错误 ERR invalid password），
		// 而非拖到首次实际操作（resolve / MarkSelfDraining）才报错。仅打印，不改变启用状态。
		pingCtx, pingCancel := context.WithTimeout(context.Background(), xuantanRedisOpTimeout)
		if err := xuantanRDB.Ping(pingCtx).Err(); err != nil {
			log.Warnf("xuantan placement: redis ping failed: %v (redis=%v db=%d)", err, addrs, cfg.Redis.DB)
		} else {
			log.Infof("xuantan placement: redis ping ok (redis=%v db=%d)", addrs, cfg.Redis.DB)
		}
		pingCancel()

		go xuantanCacheGC()

		log.Infof("xuantan placement: enabled from %q, types=%v, redis=%v db=%d ttl=%s bindPrefix=%q idsPrefix=%q",
			path, xuantanTypeKinds, addrs, cfg.Redis.DB, xuantanBindTTL, xuantanKeyPrefix, xuantanIdsPrefix)
	})
}

// xuantanResolveSecret 解析密码类字段：配置为 ${KEY} 占位符时以花括号内的 KEY 作为环境变量名读取其值；
// 配置为空时回退读默认环境变量 defaultEnvKey；否则原样返回。使部署侧只注入 env 即可，无需渲染配置文件。
// 语义与业务侧 core/infra.resolveSecretFromEnv 一致（两侧共读同一份 config 的 dapr.redis 段）。
func xuantanResolveSecret(configVal, defaultEnvKey string) string {
	v := strings.TrimSpace(configVal)
	if v == "" {
		return strings.TrimSpace(os.Getenv(defaultEnvKey))
	}
	if strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}") {
		key := strings.TrimSpace(v[2 : len(v)-1])
		if key == "" {
			return ""
		}
		return strings.TrimSpace(os.Getenv(key))
	}
	return v
}

// xuantanParseDur 解析 Go duration 字符串，空/非法回落 fallback（与业务侧 infra.parseDur 语义一致）。
func xuantanParseDur(s string, fallback time.Duration) time.Duration {
	s = strings.TrimSpace(s)
	if s == "" {
		return fallback
	}
	if d, err := time.ParseDuration(s); err == nil && d > 0 {
		return d
	}
	log.Warnf("xuantan placement: invalid redis duration %q, fallback to %s", s, fallback)
	return fallback
}

// xuantanAppendTypes 把 actorType 列表(配置为空时用 def)按 kind 追加到映射表 dst 并返回。
func xuantanAppendTypes(dst []xuantanTypeKind, configured, def []string, kind int) []xuantanTypeKind {
	list := configured
	if len(list) == 0 {
		list = def
	}
	for _, t := range list {
		if t = strings.TrimSpace(t); t != "" {
			dst = append(dst, xuantanTypeKind{typ: t, kind: kind})
		}
	}
	return dst
}

// xuantanTrimNonEmpty 返回去除首尾空白、剔除空项后的列表副本。
func xuantanTrimNonEmpty(in []string) []string {
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
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
// 仅在策略启用时由 xuantanInit 启动一次；收到 xuantanCacheGCStop（进入 block-shutdown 优雅退出）即返回。
func xuantanCacheGC() {
	t := time.NewTicker(xuantanCacheGCInterval)
	defer t.Stop()
	for {
		select {
		case <-xuantanCacheGCStop:
			return
		case <-t.C:
			now := time.Now()
			xuantanCache.Range(func(k, v any) bool {
				// 仅按时间回收 table 条目；room 无 TTL，规模有界，靠 ring 判活惰性淘汰。
				if e, ok := v.(xuantanCacheEntry); ok && e.kind == xtKindBattle && now.Sub(e.at) > xuantanBindTTL {
					xuantanCache.Delete(k)
				}
				return true
			})
		}
	}
}

// xuantanStopCacheGC 关闭本地缓存 GC goroutine（幂等，可重复/并发调用）。
// 在进入 block-shutdown 优雅退出（MarkSelfDraining）时调用；策略未启用（GC 未启动）时也安全。
func xuantanStopCacheGC() {
	xuantanCacheGCStopOnce.Do(func() { close(xuantanCacheGCStop) })
}

// xuantanSetSelfHost 记录本 daprd 的 ring Host.Name（= hostname:port），供 MarkSelfDraining 使用。
// 由 inflight.New 调用；进程内仅一个 disseminator Inflight，最后一次写入即当前 daprd 的自身地址。
func xuantanSetSelfHost(hostname, port string) {
	xuantanSelfHost = net.JoinHostPort(hostname, port)
}

// MarkSelfDraining 在本 daprd 进入优雅退出（block-shutdown 窗口起点）时调用：把自身 host.Name 写入排空索引
// xt:dapr:draining（ZSET，score=now+ttl），令其它 daprd 的放置策略在「新/重分配」时把本 host 从候选剔除
// ——只拦新分配，既有绑定/粘性不受影响。本 host 随后离开 ring 时该条目靠 score 过期自清（并顺带懒清历史过期成员）。
//
// 放置未启用（无 KEY_XT_PLACEMENT_CONFIG / redis 关闭）或 self host 未知时为 no-op。
// ttl 应覆盖 block-shutdown 窗口（建议 = blockShutdownDuration + 余量）；<=0 回退 defaultXuantanDrainTTL。
func MarkSelfDraining(ctx context.Context, ttl time.Duration) error {
	xuantanInit()
	// 进入 block-shutdown 优雅退出：关停本地缓存 GC goroutine（幂等；策略未启用时亦安全）。
	xuantanStopCacheGC()
	if xuantanRDB == nil || xuantanSelfHost == "" {
		return nil
	}
	if ttl <= 0 {
		ttl = defaultXuantanDrainTTL
	}
	now := time.Now()
	// 懒清 score<=now 的历史过期成员，避免 ZSET 无限增长（含 pod-IP 复用后的陈旧条目）。
	_ = xuantanRDB.ZRemRangeByScore(ctx, xuantanDrainingKey, "-inf", strconv.FormatInt(now.UnixMilli(), 10)).Err()
	expireAt := now.Add(ttl).UnixMilli()
	if err := xuantanRDB.ZAdd(ctx, xuantanDrainingKey, redis.Z{Score: float64(expireAt), Member: xuantanSelfHost}).Err(); err != nil {
		return err
	}
	log.Infof("xuantan placement: self marked draining host=%q ttl=%s", xuantanSelfHost, ttl)
	return nil
}

// xuantanDrainingHosts 读排空索引，返回当前仍在排空（score>now）的 host.Name 集合；空/出错返回 nil。
// 仅在（重）分配路径调用（table 首次 CAS 前 / room 选址前），粘性热路径不调用，故每次读 Redis 可接受。
func xuantanDrainingHosts(ctx context.Context) map[string]struct{} {
	if xuantanRDB == nil {
		return nil
	}
	nowStr := strconv.FormatInt(time.Now().UnixMilli(), 10)
	members, err := xuantanRDB.ZRangeByScore(ctx, xuantanDrainingKey, &redis.ZRangeBy{Min: "(" + nowStr, Max: "+inf"}).Result()
	if err != nil {
		log.Warnf("xuantan placement: read draining set %q failed: %v", xuantanDrainingKey, err)
		return nil
	}
	if len(members) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(members))
	for _, m := range members {
		set[m] = struct{}{}
	}
	return set
}

// xuantanDrainSnap 排空集合的进程内快照（不可变，只读共享）。
type xuantanDrainSnap struct {
	at  time.Time
	set map[string]struct{}
}

// xuantanDrainCache 排空集合的进程内缓存。放置分配是冷路径但在建桌高峰(如 1w/s)会被高频触发，
// 若每次都 ZRANGEBYSCORE 会给单事件循环 goroutine 再加一次 Redis 往返（见性能分析）。
// draining 集合变化极慢（host 进/出排空是分钟级事件），故缓存 xuantanDrainCacheTTL 足矣：
// 代价只是「host 刚进排空的 ~TTL 窗口内可能仍有极少数新分配落上去」——这些实例照样在
// daprd block-shutdown 窗口(分钟级)内收尾，可接受。
var xuantanDrainCache atomic.Pointer[xuantanDrainSnap]

// xuantanDrainingHostsCached 返回排空集合的（近实时）快照：命中缓存零 Redis，过期才刷新一次。
// 返回的 map 只读、绝不原地修改，可安全并发共享。空集合也会被缓存（避免空集时反复打 Redis）。
func xuantanDrainingHostsCached(ctx context.Context) map[string]struct{} {
	if xuantanRDB == nil {
		return nil
	}
	if s := xuantanDrainCache.Load(); s != nil && time.Since(s.at) < xuantanDrainCacheTTL {
		return s.set
	}
	set := xuantanDrainingHosts(ctx)
	xuantanDrainCache.Store(&xuantanDrainSnap{at: time.Now(), set: set})
	return set
}

// xuantanPickAliveExcluding 在 ring 内「存活且不在 exclude 集合」的 host 中，按 actorID 稳定选一个（用于 table
// 首选 host 正在排空时的确定性改选）。稳定选择让并发多 daprd 尽量选同一 host（最终仍由 Redis CAS 收敛）。
// 无可选（全部被排除或环空）时返回 (nil, false)。
func (i *Inflight) xuantanPickAliveExcluding(ring *hashing.Consistent, actorID string, exclude map[string]struct{}) (*hashing.Host, bool) {
	all := i.xuantanRingHosts(ring)
	cands := make([]string, 0, len(all))
	for _, h := range all {
		if _, bad := exclude[h]; !bad {
			cands = append(cands, h)
		}
	}
	if len(cands) == 0 {
		return nil, false
	}
	sort.Strings(cands)
	h := fnv.New32a()
	_, _ = h.Write([]byte(actorID))
	pick := cands[h.Sum32()%uint32(len(cands))]
	if hh, alive := i.xuantanRingHost(ring, pick); alive {
		return hh, true
	}
	return nil, false
}

// resolveXuantan 是自定义放置策略的统一入口/分发器。返回 (resp, handled, err)：
//   - handled=false：本策略不接管该请求（未启用 / actorType 无对应策略），
//     调用方应继续走 Dapr 原生哈希 resolve；
//   - handled=true ：某子策略已给出结果（resp 或 err 之一有效）。
//     注意：Redis 故障也返回 handled=true + ErrActorNoAddress(可重试)，而非降级回原生哈希——
//     降级会破坏粘性(table 迁移)/均衡与容量(room)，故宁可重试也不回退。
//
// 按 actorType 分发到具体策略：table -> resolveXuantanBattle；room -> resolveXuantanMatch。
func (i *Inflight) resolveXuantan(req *api.LookupActorRequest) (*api.LookupActorResponse, bool, error) {
	if xuantanRDB == nil {
		return nil, false, nil // 未启用
	}
	switch xuantanKindOf(req.ActorType) {
	case xtKindBattle:
		return i.resolveXuantanBattle(req)
	case xtKindMatch:
		return i.resolveXuantanMatch(req)
	default:
		return nil, false, nil // 非受管类型
	}
}

// resolveXuantanBattle 牌桌(table)粘性放置策略：初次按哈希分配并写 Redis(TTL)，
// 之后只要绑定 host 仍存活就一直返回它，host 失效才用哈希 + CAS 重分配。详见文件头注释。
func (i *Inflight) resolveXuantanBattle(req *api.LookupActorRequest) (*api.LookupActorResponse, bool, error) {
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

	// ① 本地先算好候选 host（cand）：一致性哈希首选；若首选正在排空则在非排空存活 host 中确定性改选。
	//    排空集合走进程内缓存(xuantanDrainingHostsCached)，建桌高峰下不额外增加 Redis 往返。
	//    只影响「新分配」；已绑定到排空 host 的牌桌会在 ③ 的粘性分支原样返回，不受影响。
	cand, err := ring.GetHost(req.ActorID)
	if err != nil {
		return nil, true, err
	}
	if draining := xuantanDrainingHostsCached(ctx); len(draining) > 0 {
		if _, d := draining[cand.Name]; d {
			alt, ok := i.xuantanPickAliveExcluding(ring, req.ActorID, draining)
			if !ok {
				// 全体存活 host 都在排空 = 整个服务在下线：不分配新桌，报可重试错误，等新实例就绪后重试。
				msg := fmt.Sprintf("xuantan placement: %s all alive hosts draining, refuse new table", req.ActorKey())
				log.Warn(msg)
				return nil, true, messages.ErrActorNoAddress.WithFormat(msg)
			}
			cand = alt
		}
	}

	// ② 单脚本一趟完成「有效性门禁 + 粘性 + CAS 分配」（省去独立 GET，(重)分配从 2 次 RTT 降到 1 次）：
	//    脚本内 GET 现值——不存在→哨兵；== "1"(有效标记)→CAS 覆盖为 cand 并返回 cand；否则原样返回现值。
	ttl := strconv.Itoa(int(xuantanBindTTL.Seconds()))
	res, err := xuantanBindScript.Run(ctx, xuantanRDB, []string{key}, xuantanBattleValidMark, cand.Name, ttl).Result()
	if err != nil {
		// Redis 故障：不降级到原生哈希(会破坏粘性导致迁移)，返回可重试错误等待重试。
		msg := fmt.Sprintf("xuantan placement: %s %q bind failed: %v", req.ActorKey(), key, err)
		log.Warn(msg)
		return nil, true, messages.ErrActorNoAddress.WithFormat(msg)
	}
	finalName, _ := res.(string)

	// ③ 解释脚本返回值（存活判定在 daprd 侧本地做，脚本无需知道 ring）：
	switch {
	case finalName == xuantanNotValid:
		// key 不存在：未被业务预标记为有效(见 MarkBattleValid) => 无效 tableId。
		msg := fmt.Sprintf("xuantan placement: %s table not pre-marked valid (key %q missing)", req.ActorKey(), key)
		log.Warn(msg)
		return nil, true, messages.ErrActorNoAddress.WithFormat(msg)
	case finalName == cand.Name:
		// 我方 CAS 分配成功，或既有绑定恰为本次 cand —— cand 取自存活 ring，必存活，直接采用。
		xuantanCacheStore(cacheKey, finalName, xtKindBattle)
		return i.xuantanResp(cand), true, nil
	default:
		// 返回的是既有绑定的「其它 host」：存活→粘性返回；失效/脏值→无效（牌桌绝不迁移）。
		if h, alive := i.xuantanRingHost(ring, finalName); alive {
			xuantanCacheStore(cacheKey, finalName, xtKindBattle)
			return i.xuantanResp(h), true, nil
		}
		msg := fmt.Sprintf("xuantan placement: %s table invalid bind value %q (host not alive)", req.ActorKey(), finalName)
		log.Warn(msg)
		return nil, true, messages.ErrActorNoAddress.WithFormat(msg)
	}
}

// resolveXuantanMatch 房间(room)放置策略：有效性门禁 + 最少负载 + 粘性不迁移、无 TTL。
//
// 与 table 的差异：
//   - 有效性门禁：仅在(重)分配路径(本地缓存未命中，需访问 Redis 定址)才判定 roomId 是否在
//     有效集合 xt:dapr:ids:<actorType>(业务经 AddIds/SetIds 维护)中——不在集合内即视为无效房间，
//     直接返回异常(不分配 host、不激活)。该判定折入 xuantanMatchBindScript 原子完成，不加额外往返；
//     本地缓存命中(已分配且 host 存活)时零 Redis、直接返回，不再校验。
//   - 选址不用一致性哈希(roomId 是 <1000 的稀疏小集合，哈希会失衡)，改为在存活 host
//     中选"当前承载最少者"；
//   - 绑定无 TTL，常驻；清理靠 host 失效时的重分配，或外部 roomId owner 进程 HDEL；
//   - 扩容(新 host)不自动迁移既有 room，新容量仅承接后续(重)分配，随失效逐步均衡。
//
// 稳态同 table：本地缓存命中 + ring 判活，零 Redis；(重)分配仅在首次/绑定 host 失效时发生。
func (i *Inflight) resolveXuantanMatch(req *api.LookupActorRequest) (*api.LookupActorResponse, bool, error) {
	ring, ok := i.hashTable.Entries[req.ActorType]
	if !ok {
		return nil, true, messages.ErrActorNoAddress.WithFormat(req.ActorKey())
	}

	cacheKey := req.ActorType + ":" + req.ActorID

	// ⓪ 本地缓存命中且 host 仍存活 -> 直接返回(room 不按时间失效，仅 ring 判活)；已分配即已通过门禁。
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

	// 排空过滤：把正在排空的 host 传给脚本，选址时剔除（但仍作为存活参与粘性），只拦新/重分配，不迁移既有 room。
	// 走进程内缓存，避免每次 room (重)分配都读一次 ZSET。
	draining := xuantanDrainingHostsCached(ctx)
	dlist := make([]string, 0, len(draining))
	for h := range draining {
		dlist = append(dlist, h)
	}
	argv := make([]interface{}, 0, len(hosts)+len(dlist)+2)
	argv = append(argv, req.ActorID) // ARGV[1]=field=roomId
	argv = append(argv, len(dlist))  // ARGV[2]=D(排空 host 数)
	for _, h := range dlist {        // ARGV[3..2+D]=排空 host
		argv = append(argv, h)
	}
	for _, h := range hosts { // ARGV[3+D..]=全部存活 host(含排空)
		argv = append(argv, h)
	}
	res, err := xuantanMatchBindScript.Run(ctx, xuantanRDB, []string{hashKey, idsKey}, argv...).Result()
	if err != nil {
		// Redis 故障：不降级到原生哈希(哈希会失衡)，返回可重试错误等待重试。
		msg := fmt.Sprintf("xuantan placement: %s room bind failed: %v", req.ActorKey(), err)
		log.Warn(msg)
		return nil, true, messages.ErrActorNoAddress.WithFormat(msg)
	}
	name, _ := res.(string)
	if name == xuantanNotValid {
		// 有效性门禁：roomId 不在有效集合内 -> 无效房间，直接返回异常(未建绑定、不激活)。
		msg := fmt.Sprintf("xuantan placement: %s room not in valid id set %q", req.ActorKey(), idsKey)
		log.Warn(msg)
		return nil, true, messages.ErrActorNoAddress.WithFormat(msg)
	}
	if name == "" {
		// 无「非排空」存活 host：全在排空(全体服务下线)或 ring 变空等边界 -> 报可重试错误，绝不硬塞到排空 host。
		log.Warnf("xuantan placement: %s no non-draining alive host (alive=%d, draining=%d)", req.ActorKey(), len(hosts), len(dlist))
		return nil, true, messages.ErrActorNoAddress.WithFormat(req.ActorKey())
	}

	if h, alive := i.xuantanRingHost(ring, name); alive {
		xuantanCacheStore(cacheKey, name, xtKindMatch)
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
