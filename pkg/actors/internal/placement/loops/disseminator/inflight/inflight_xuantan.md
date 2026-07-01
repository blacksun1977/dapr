# 玄滩（xuantan）自定义 Actor 放置策略

> 代码：`inflight_xuantan.go`（与 `inflight.go` 同包 `inflight`）
> 接入点：`inflight.go` 的 `New()` 调用一次 `xuantanInit()`；`resolve()` 开头调用 `resolveXuantan()` 钩子。

## 1. 背景与动机

Dapr 原生 actor 放置是**一致性哈希**：集群成员一旦变化（扩容、滚动更新、节点崩溃），就按哈希重排，已激活的 actor 可能被迁移到别的
host。这对两类业务不可接受：

- **牌桌（table）**：一局对局可达数分钟，对局期间**绝不能迁移**。
- **房间（room）**：与 pypy worker 1:1 绑定，每个 host 的 worker 槽位是**硬容量上限**；roomId 是 `<1000` 的稀疏 `uint64`
  小集合，用哈希分配会**严重失衡**，可能撑爆某个 pod。

本策略在 daprd sidecar 内部覆写归属解析（`resolve`）的选择逻辑，对受管 actorType 走自定义放置，其余类型完全旁路、回退 Dapr
原生哈希。

## 2. 总体设计

```
                       LookupActor(req)
                             │
                  inflight.resolve(req)
                             │
                  resolveXuantan(req)      ← 分发器
          ┌──────────────────┼───────────────────┐
   kind=table             kind=room          其他/未启用
          │                  │                    │
 resolveXuantanTable  resolveXuantanRoom   handled=false → 原生哈希
```

- **分发**：`resolveXuantan` 按 `req.ActorType` 用 `xuantanKindOf` 查到策略种类（`xtKindTable=1` / `xtKindRoom=2`
  ），分发到对应子策略；未受管或未启用返回 `handled=false`，由 Dapr 原生 `resolve` 接管。
- **返回约定**：`(resp, handled, err)`
    - `handled=false`：本策略不接管（未启用 / 非受管类型），调用方继续走原生哈希。
    - `handled=true`：本策略给出结果（`resp` 或 `err` 之一有效）。**Redis 故障也走此分支**，返回 `ErrActorNoAddress`（可重试），不降级回原生哈希。
- **启用开关**：设置 `KEY_XT_PLACEMENT_REDIS_ADDR` 即开启；未设置时 `xuantanRDB == nil`，整体旁路，行为与官方一致。

## 3. 环境变量

| 变量                                   | 含义                                        | 默认值              |
|--------------------------------------|-------------------------------------------|------------------|
| `KEY_XT_PLACEMENT_REDIS_ADDR`        | Redis 地址，逗号分隔（多地址即集群/哨兵）。**置空=整个策略关闭**    | 空（关闭）            |
| `KEY_XT_PLACEMENT_REDIS_PASSWORD`    | Redis 密码                                  | 空                |
| `KEY_XT_PLACEMENT_REDIS_DB`          | Redis DB 序号                               | `0`              |
| `KEY_XT_PLACEMENT_KEY_PREFIX`        | 绑定 key 前缀（bind）                           | `xt:dapr:bind:`  |
| `KEY_XT_PLACEMENT_IDS_PREFIX`        | room 有效 id 集合（SET）前缀                      | `xt:dapr:ids:`   |
| `KEY_XT_PLACEMENT_BIND_TTL`          | table 绑定 TTL（Go duration，如 `3h`、`90m`）    | `3h`             |
| `KEY_XT_PLACEMENT_STICKY_TYPE_TABLE` | 走 table 策略的 actorType 集合（逗号分隔）            | `table_py,table` |
| `KEY_XT_PLACEMENT_STICKY_TYPE_ROOM`  | 走 room 策略的 actorType 集合（逗号分隔）             | `room_py,room`   |
| `KEY_XT_PLACEMENT_ROOM_CAP_PER_HOST` | 每 host 的 room 容量上限（= worker 槽数）；`0`/未设=不限 | `0`（不限）          |

> actorType 集合非常小（table ≤3、room ≤4），内部合并成一张带 `kind` 标记的切片 `xuantanTypeKinds`，用**线性扫描**（
`xuantanKindOf`）查找——规模这么小，线性扫描比 map 省去哈希/分配，更快更省内存。

## 4. Redis 数据模型

### 4.1 table（每个 actor 一个 string）

```
key   = <KEY_PREFIX><actorType>:<actorID>     例: xt:dapr:bind:table:123456
value = <hostAddr>
TTL   = KEY_XT_PLACEMENT_BIND_TTL（默认 3h）
```

### 4.2 room（每个 actorType 一张 hash + 一个外部维护的 SET）

```
绑定 hash : <KEY_PREFIX><actorType>    例: xt:dapr:bind:room
            field = <roomId>           value = <hostAddr>     无 TTL
有效集合  : <IDS_PREFIX><actorType>    例: xt:dapr:ids:room   (SET)
            成员 = 全量有效 roomId，由【外部业务进程】增删维护
```

- room 绑定**无 TTL**，常驻。
- 容量统计是 **per-actorType**（每张 hash 独立计数）。

## 5. table 策略（`resolveXuantanTable`）

特点：**初次哈希分配 → 持久化 → 粘性不迁移 → host 失效才重分配**。

流程：

```
⓪ 本地缓存命中且 host 在 ring 内(存活) 且未过期(≤TTL)  → 直接返回（热路径零 Redis）
① GET 绑定
   - 存在且 host 存活 → 回填缓存，返回（粘住，不续期 TTL）
   - 存在但 host 失效 → 记 prev，转 ② 做 CAS 重分配
   - Redis 故障(非 Nil) → 记日志，handled=true + ErrActorNoAddress(可重试，不降级)
   - 无绑定           → prev=""
② ring.GetHost(actorID) 用一致性哈希选新 host
③ Lua CAS 原子写入（xuantanBindScript）：
   - key 不存在        → SET（等价 NX）写 newHost
   - 当前值==prev      → 覆盖为 newHost（失效重分配）
   - 否则(被他人抢写)  → 返回既有权威值
④ 采用权威绑定，回填本地缓存并返回
```

`xuantanBindScript`（CAS 创建/替换）：

```lua
-- KEYS[1]=bindKey  ARGV[1]=expectedOld(新建传"")  ARGV[2]=newHost  ARGV[3]=ttl秒
local cur = redis.call('GET', KEYS[1])
if cur == false then redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3]); return ARGV[2] end
if cur == ARGV[1] then redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3]); return ARGV[2] end
return cur
```

**TTL 策略**：绑定与本地缓存条目均为 `BIND_TTL`，且**不做命中续期**——TTL 仅用于无人值守的自动清理。约束：`actor 存活时长 < TTL`
。牌桌一局 ~5 分钟，远小于 3h，绝不会在对局期间过期。

## 6. room 策略（`resolveXuantanRoom`）

特点：**最少负载 + 容量感知 + 粘性不迁移 + 无 TTL**。

流程：

```
⓪ 本地缓存命中且 host 存活        → 直接返回（room 只按 ring 判活，不按时间过期）
   枚举 ring 内所有存活 host 作为候选集（xuantanRingHosts）
   候选为空 → ErrActorNoAddress（可重试）
   调用 Lua（xuantanRoomBindScript），传入 roomId、cap、存活 host 列表：
     - 现绑定 host 存活            → 直接返回（粘性）
     - 顺带清理：bind 中的 roomId 不在 ids SET → 立即 HDEL（释放容量+自清理）
     - 在“存活且未满 cap”的 host 中选承载最少者(平手按地址名定序) → HSET 写入
     - 全满 → 返回 ''（上层报 ErrActorNoAddress 可重试）
   回填本地缓存并返回
   Redis 故障 → handled=true + ErrActorNoAddress(可重试，不降级)
```

`xuantanRoomBindScript`（最少负载 + 容量 + 粘性 + 清理）：

```lua
-- KEYS[1]=bind hash(xt:dapr:bind:<type>)  KEYS[2]=ids set(xt:dapr:ids:<type>)
-- ARGV[1]=roomId  ARGV[2]=capPerHost(<=0 不限)  ARGV[3..]=存活 host 列表
local field = ARGV[1]
local cap = tonumber(ARGV[2])
local alive = {}
for i = 3, #ARGV do alive[ARGV[i]] = 0 end

local cur = redis.call('HGET', KEYS[1], field)
if cur and alive[cur] ~= nil then return cur end          -- 粘性：存活就不动

local all = redis.call('HGETALL', KEYS[1])
for i = 1, #all, 2 do
    local rid = all[i]
    local h = all[i + 1]
    if redis.call('SISMEMBER', KEYS[2], rid) == 0 then
        redis.call('HDEL', KEYS[1], rid)                  -- 无效 roomId 立即清理
    elseif alive[h] ~= nil then
        alive[h] = alive[h] + 1                           -- 仅统计存活 host 负载
    end
end

local best, bestCount
for i = 3, #ARGV do
    local h = ARGV[i]
    local c = alive[h]
    if cap <= 0 or c < cap then
        if best == nil or c < bestCount or (c == bestCount and h < best) then
            best = h; bestCount = c
        end
    end
end
if best == nil then return '' end                         -- 全满
redis.call('HSET', KEYS[1], field, best)
return best
```

**为什么不用哈希**：roomId 是 `<1000` 的稀疏小集合，样本太小，一致性哈希落点方差大、会失衡；而 room=worker，每 host
有硬容量上限。改为“每次落到最少负载的存活 host”，全量分完后近乎均匀（±1），并严守容量。

**全量 room 已知**：因为 `<1000`，整张 hash 一次 `HGETALL` 在脚本内统计即可，无需独立计数器，也无计数漂移问题。（重）分配只在“首次/绑定
host 失效”时触发，极少发生。

**无效清理**：`xt:dapr:ids:<type>` 是有效 roomId 的唯一真相，由外部业务进程维护。脚本在统计负载时顺带把“已不在 SET 中”的
bind 字段 `HDEL`，既释放容量又自清理。

**扩容不迁移**：新加入的 host 不触发既有 room 迁移（room=pypy worker，迁移很重），只承接后续的新分配/失效重分配，随失效逐步均衡。

## 7. 本地缓存与 GC

- `xuantanCache`（进程级全局 `sync.Map`）：`key = "<actorType>:<actorID>"` → `xuantanCacheEntry{host, kind, at}`。
- **热路径**：命中且 host 仍在 ring（存活）即直接返回，**零 Redis**。host 失效则弃用缓存、回落 Redis。
- **GC 协程**（`xuantanCacheGC`，每分钟）：
    - 仅按 TTL 回收 **table** 条目（`now - at > BIND_TTL`）。
    - **room** 条目不按时间回收（全量 `<1000` 有界），仅靠 ring 判活惰性淘汰。
- 缓存只在“确实从 Redis 拿到/写入权威绑定”后回填（`xuantanCacheStore`）。

## 8. 单激活与一致性保证

- **唯一权威**：Redis 绑定（table 的 string / room 的 hash 字段）是全集群唯一权威。
- **原子收敛**：table 用 “SET NX / 仅当旧值匹配才覆盖” 的 Lua CAS；room 用单条 Lua（HGET 粘性 + HGETALL 统计 +
  HSET）原子完成。并发的多个 daprd 收敛到同一 host。
- **判活复核**：本地缓存每次都用 `xuantanRingHost`（`ring.ReadInternals` 读 `loadMap`）复核存活，host 一旦失效立即弃缓存回
  Redis，不破坏单激活。
- **存活键绝不重分配** → 正常扩容/滚动更新不触发迁移。

## 9. 故障与降级

| 场景                       | 行为                                 |
|--------------------------|------------------------------------|
| 未配置 Redis 地址             | 整体旁路，回退原生哈希                        |
| actorType 非受管            | 旁路，回退原生哈希                          |
| Redis 操作失败（table/room）   | 记日志，`handled=true` + `ErrActorNoAddress`（可重试），**不降级**（降级会破坏 table 粘性、room 均衡/容量） |
| ring 无该 actorType / 候选为空 | `ErrActorNoAddress`（可重试）           |
| room 全满（超 cap）           | `ErrActorNoAddress`（可重试），等扩容/释放    |
| 选中 host 并发下发中失效          | `ErrActorNoAddress`（可重试），下次重选      |

> 单次 Redis 操作超时 `xuantanRedisOpTimeout = 2s`，避免抖动阻塞热路径。

## 10. 外部业务约定（room）

外部进程作为 roomId 的 owner，需把**全量有效 roomId** 维护到 SET：

```
SADD xt:dapr:ids:<actorType> <roomId> ...   # 新增/初始化
SREM xt:dapr:ids:<actorType> <roomId>        # 删除（绑定会在下次重分配脚本里被 HDEL 自清）
```

daprd 侧不负责增删 ids SET，只读取它做有效性校验与自清理。

## 11. 关键参数小结

| 项            | 取值                                        |
|--------------|-------------------------------------------|
| Redis 操作超时   | `2s`                                      |
| 本地缓存 GC 周期   | `1min`                                    |
| table 绑定 TTL | `KEY_XT_PLACEMENT_BIND_TTL`，默认 `3h`，不续期   |
| room 绑定 TTL  | 无（常驻，靠重分配/ids 清理）                         |
| room 容量上限    | `KEY_XT_PLACEMENT_ROOM_CAP_PER_HOST`，默认不限 |
| table 单激活    | Lua CAS（SET NX / 旧值匹配覆盖）                  |
| room 单激活     | 单条 Lua（HGET 粘性 + 最少负载 HSET）               |
