# 玄滩（xuantan）自定义 Actor 放置策略

> 代码：`inflight_xuantan.go`（与 `inflight.go` 同包 `inflight`）
> 接入点：`inflight.go` 的 `New()` 调用一次 `xuantanInit()`；`resolve()` 开头调用 `resolveXuantan()` 钩子。

## 1. 背景与动机

Dapr 原生 actor 放置是**一致性哈希**：集群成员一旦变化（扩容、滚动更新、节点崩溃），就按哈希重排，已激活的 actor 可能被迁移到别的
host。这对两类业务不可接受：

- **牌桌（table）**：一局对局可达数分钟，对局期间**绝不能迁移**。
- **房间（room）**：与 pypy worker 1:1 绑定；roomId 是 `<1000` 的稀疏 `uint64` 小集合，用哈希分配会**严重失衡**，
  可能撑爆某个 pod。改用「最少负载」分配把房间近乎均匀地摊到各存活 host（容量由 provisioning 保证，放置层不设硬上限）。

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
- **启用开关**：设置 `KEY_XT_PLACEMENT_CONFIG` 指向共享配置文件（含顶层 `dapr:` 段）即开启；未设置该路径、
  文件读取/解析失败、或文件里 `dapr.redis.addresses` 为空时 `xuantanRDB == nil`，整体旁路，行为与官方一致。

## 3. 配置（环境变量 + 共享配置文件）

daprd 只认一个环境变量——配置文件路径；所有参数都来自该文件的 `dapr:` 段。**该文件由 daprd 与业务侧
（`core/etc` 的 `DaprConfig`）共读同一份**，保证两侧「同一 Redis、同一 key」。

| 环境变量                   | 含义                                          | 默认值    |
|------------------------|---------------------------------------------|--------|
| `KEY_XT_PLACEMENT_CONFIG` | 共享配置文件路径（YAML，含顶层 `dapr:` 段）。**未设置=整个策略关闭** | 空（关闭） |

### 3.1 配置文件格式（YAML，顶层 `dapr:` 段）

业务侧 actor 进程配置文件与 daprd 复用同一份；`dapr:` 段字段与 `core/etc.DaprConfig` 的 yaml
tag 完全一致，其中 `redis:` 子段格式与业务其余 Redis 配置 `core/infra.RedisConfig` 一致：

```yaml
# 业务侧 actor 进程还会读同文件的 actor: 段；daprd 只读 dapr: 段（call_timeout 为业务出站专用，daprd 忽略）。
dapr:
  redis:                         # Redis 连接（格式同 core/infra.RedisConfig）
    addresses:                   # 地址列表：单条=单节点、多条=Cluster；为空=整个策略关闭
      - "127.0.0.1:6379"
    username: ""                 # Redis 6+ ACL 用户名（可选）
    password: ""                 # Redis 密码
    db: 0                        # 逻辑库编号（仅单节点生效）
    dial_timeout: "2s"           # 拨号/Ping 超时（Go duration），空缺省 2s
    read_timeout: "3s"           # 读超时，空缺省 3s
    write_timeout: "3s"          # 写超时，空缺省 3s
    pool_size: 0                 # 每 endpoint 最大连接数，0=go-redis 估算
    min_idle_conns: 0            # 每 endpoint 最少空闲连接
  key_prefix: "xt:dapr:bind:"    # 绑定 key 前缀（bind）
  ids_prefix: "xt:dapr:ids:"     # room 有效 id 集合（SET）前缀
  bind_ttl: "15m"                # table 绑定 TTL（Go duration，如 15m/1h）；room 无 TTL
  sticky_type_table:             # 走 table 策略的 actorType 列表
    - "table_py"
    - "table"
  sticky_type_room:              # 走 room 策略的 actorType 列表
    - "room_py"
    - "room"
```

| 字段                  | 含义                                        | 默认值                |
|---------------------|-------------------------------------------|--------------------|
| `redis`             | Redis 连接（格式同 `core/infra.RedisConfig`）；`addresses` 为空=整个策略关闭 | —          |
| `key_prefix`        | 绑定 key 前缀（bind）                           | `xt:dapr:bind:`    |
| `ids_prefix`        | room 有效 id 集合（SET）前缀                      | `xt:dapr:ids:`     |
| `bind_ttl`          | table 绑定 TTL（Go duration，如 `15m`、`1h`）    | `15m`              |
| `sticky_type_table` | 走 table 策略的 actorType 列表                  | `[table_py, table]` |
| `sticky_type_room`  | 走 room 策略的 actorType 列表                   | `[room_py, room]`   |

> 业务侧用 `core/infra.NewRedisClient` 据 `redis:` 段建客户端（单/Cluster 自动判定 + 建连 Ping 校验）；
> daprd 侧读同样的 `redis:` 键、按相同语义构造 `redis.UniversalClient`。

> 缺省值在 daprd 侧 `xuantanInit` 与业务侧 `manager` 各自回落，两侧保持一致。业务侧的读写入口见
> `core/actor` 的 `IManager.GetPlacementBinding` / `SetPlacementBinding`（Redis 连接按 `DaprConfig`
> 延迟建立、进程内复用）。
>
> actorType 集合非常小（table ≤3、room ≤4），内部合并成一张带 `kind` 标记的切片 `xuantanTypeKinds`，用**线性扫描**（
`xuantanKindOf`）查找——规模这么小，线性扫描比 map 省去哈希/分配，更快更省内存。

## 4. Redis 数据模型

### 4.1 table（每个 actor 一个 string）

```
key   = <KEY_PREFIX><actorType>:<actorID>     例: xt:dapr:bind:table:123456
value = <hostAddr>
TTL   = dapr.bind_ttl（默认 15m）
```

### 4.2 room（每个 actorType 一张 hash + 一个外部维护的 SET）

```
绑定 hash : <KEY_PREFIX><actorType>    例: xt:dapr:bind:room
            field = <roomId>           value = <hostAddr>     无 TTL
有效集合  : <IDS_PREFIX><actorType>    例: xt:dapr:ids:room   (SET)
            成员 = 全量有效 roomId，由【外部业务进程】增删维护
```

- room 绑定**无 TTL**，常驻。
- 负载统计是 **per-actorType**（每张 hash 独立计数）。

## 5. table 策略（`resolveXuantanTable`）

特点：**有效性门禁 → 初次哈希分配 → 持久化 → 粘性不迁移 → host 失效即拒绝(绝不迁移)**。

**有效性门禁**：牌桌必须先被业务预标记为有效才能分配 host。业务在**预建牌桌实例之前**把绑定 key 置为有效标记
`"1"`（`core.IManager.MarkTableValid`，SET 带 TTL）；daprd 分配时：

- 绑定 key **不存在**（未预标记 / 标记已过期）→ 视为无效 tableId，返回哨兵 → 上层报错，**绝不分配 host**；
- 绑定 key 值为 **`"1"`**（已预标记待分配）→ 门禁通过，进入分配（CAS `"1"` → host）；
- 绑定 key 值为**存活 host** → 已分配，粘性返回；
- 绑定 key 值为**失效 host / 脏值** → 无效（牌桌绝不迁移，host 失效即这局不可恢复）→ 报错。

> 与旧版差异：旧版“key 不存在即 SET NX 建绑定”会给任意 tableId 凭空激活牌桌；现要求业务先 `MarkTableValid`。
> 有效性门禁只在（分配/未命中）路径判定；本地缓存命中(已分配且 host 存活)时零 Redis、直接返回。

流程（**(重)分配只 1 次 Redis 往返**：单脚本一趟完成门禁+粘性+CAS，存活判定在 daprd 侧做，脚本无需知道 ring）：

```
⓪ 本地缓存命中且 host 在 ring 内(存活) 且未过期(≤TTL)  → 直接返回（热路径零 Redis）
① 本地算候选 cand：ring.GetHost(actorID) 一致性哈希首选
   - 若首选 host 正在排空(命中 xt:dapr:draining 进程缓存) → 在非排空存活 host 中按 actorID 确定性改选；
     全在排空(=全体服务下线) → 不分配，handled=true + ErrActorNoAddress(可重试，绝不硬塞排空 host)。
   - 排空集合走进程内缓存(默认 1s)，建桌高峰下不增加 Redis 往返；仅新分配过滤，粘性走脚本内判定不受影响。
② 单脚本一趟（xuantanBindScript，ARGV: "1", cand, ttl）：GET 现值 → 门禁+粘性+CAS 原子完成
③ 据脚本返回值解释（daprd 本地判活）：
   - 返回哨兵           → key 不存在=未预标记 => 无效 tableId，handled=true + ErrActorNoAddress
   - Redis 故障(脚本报错) → 记日志，handled=true + ErrActorNoAddress(可重试，不降级)
   - 返回 == cand        → 我方 CAS 分配成功 / 既有绑定恰为 cand（取自存活 ring，必存活）→ 回填缓存、采用
   - 返回其它 host       → 本地判活：存活=粘性返回(不续期)；失效/脏值=无效(牌桌绝不迁移) + ErrActorNoAddress
```

`xuantanBindScript`（门禁 + 粘性 + 有效标记 CAS，一趟）：

```lua
-- KEYS[1]=bindKey  ARGV[1]=有效标记"1"  ARGV[2]=cand(候选host)  ARGV[3]=ttl秒
local cur = redis.call('GET', KEYS[1])
if cur == false then return '\0xt-not-member' end            -- 未预标记 => 无效(不建绑定)
if cur == ARGV[1] then redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3]); return ARGV[2] end  -- CAS 分配
return cur                                                    -- 已是某 host/脏值 => 原样返回，由 daprd 判活
```

> **1 RTT 设计**：早先「先 GET 判粘性/门禁，再 CAS」是 2 次往返；现改为「本地算好 cand → 只调一次脚本 →
> daprd 对返回值本地判活」。存活判定天然是 daprd 侧知识（ring 成员），放回 Go 侧后脚本连 alive 列表都不用传，
> 且绑定 key 与排空 key 不同 slot 的 Cluster 问题也自然规避（排空过滤在 daprd 侧用缓存做，不进脚本）。
> 叠加排空进程缓存，(重)分配从 3 次 RTT（GET+ZRANGEBYSCORE+CAS）降到 **1 次**。

**谁写有效标记**：游戏业务分配 tid（`Mesh().Sequence().NextId("table")`）后、`create.table.ins` 预建牌桌实例
之前，调 `MarkTableValid(ActorTypeBattlePy, tid)` 写入 `"1"`（见 `game/worker/pypy/create_table.go`）。

**TTL 策略**：绑定与本地缓存条目均为 `BIND_TTL`，且**不做命中续期**——TTL 仅用于无人值守的自动清理。约束：`actor 存活时长 < TTL`
。牌桌一局 ~5 分钟，小于 15m，正常对局不会过期；业务侧对活跃牌桌可续期（`MarkTableValid` 幂等 EXPIRE）。有效标记 `"1"` 也用同一 TTL 写入，覆盖“预标记→首次分配”窗口。

## 6. room 策略（`resolveXuantanRoom`）

特点：**有效性门禁 + 最少负载 + 粘性不迁移 + 无 TTL**。

流程：

```
⓪ 本地缓存命中且 host 存活        → 直接返回（已分配即已通过门禁；零 Redis，不再校验）
   枚举 ring 内所有存活 host 作为候选集（xuantanRingHosts）
   候选为空 → ErrActorNoAddress（可重试）
   读排空集合(xt:dapr:draining，进程缓存) → 正在排空的 host 列表(dlist)
   调用 Lua（xuantanRoomBindScript），传入 roomId、dlist、全部存活 host 列表：
     - 有效性门禁（仅在此分配/未命中路径）：roomId 不在 ids SET → 返回哨兵
       → 上层报 ErrActorNoAddress（无效房间，不建绑定、不激活）
     - 现绑定 host 存活（含正在排空者）→ 直接返回（粘性，排空 host 上的既有 room 不迁移）
     - 顺带清理：bind 中的 roomId 不在 ids SET → 立即 HDEL（自清理）
     - 在【非排空】存活 host 中选承载最少者(平手按地址名定序) → HSET 写入
     - 无「非排空」存活 host（全在排空=全体服务下线）或无存活 host → 返回 ''（上层报 ErrActorNoAddress 可重试，绝不硬塞排空 host）
   回填本地缓存并返回
   Redis 故障 → handled=true + ErrActorNoAddress(可重试，不降级)
```

`xuantanRoomBindScript`（最少负载 + 粘性 + 清理 + 排空过滤）：

```lua
-- KEYS[1]=bind hash(xt:dapr:bind:<type>)  KEYS[2]=ids set(xt:dapr:ids:<type>)
-- ARGV[1]=roomId  ARGV[2]=D(排空 host 数)  ARGV[3..2+D]=排空 host  ARGV[3+D..]=全部存活 host
local field = ARGV[1]
if redis.call('SISMEMBER', KEYS[2], field) == 0 then
    return '\0xt-not-member'                              -- 门禁：无效 roomId，不分配（哨兵）
end

local dcount = tonumber(ARGV[2])
local draining = {}
for i = 3, 2 + dcount do draining[ARGV[i]] = true end     -- 正在排空的 host（选址剔除）

local firstAlive = 3 + dcount
local alive = {}
for i = firstAlive, #ARGV do alive[ARGV[i]] = 0 end       -- 全部存活（含排空，用于粘性判活）

local cur = redis.call('HGET', KEYS[1], field)
if cur and alive[cur] ~= nil then return cur end          -- 粘性：存活就不动（含排空 host）

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

local best, bestCount      -- 仅在「非排空」存活 host 里选承载最少者
for i = firstAlive, #ARGV do
    local h = ARGV[i]
    if not draining[h] then
        local c = alive[h]
        if best == nil or c < bestCount or (c == bestCount and h < best) then best = h; bestCount = c end
    end
end
if best == nil then return '' end                         -- 无「非排空」存活 host（全在排空=全体下线）→ 上层报错
redis.call('HSET', KEYS[1], field, best)
return best
```

**为什么不用哈希**：roomId 是 `<1000` 的稀疏小集合，样本太小，一致性哈希落点方差大、会失衡。改为“每次落到最少负载的存活 host”，全量分完后近乎均匀（±1）。

**全量 room 已知**：因为 `<1000`，整张 hash 一次 `HGETALL` 在脚本内统计即可，无需独立计数器，也无计数漂移问题。（重）分配只在“首次/绑定
host 失效”时触发，极少发生。

**无效清理**：`xt:dapr:ids:<type>` 是有效 roomId 的唯一真相，由外部业务进程维护。脚本在统计负载时顺带把“已不在 SET 中”的
bind 字段 `HDEL`，减轻对应 host 负载并自清理。

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
- **原子收敛**：table 用 “有效标记 `"1"` CAS（仅当值仍为 `"1"` 才覆盖为 host）” 的 Lua；room 用单条 Lua（HGET 粘性 +
  HGETALL 统计 + HSET）原子完成。并发的多个 daprd 收敛到同一 host。
- **有效性门禁**：table 要求业务先 `MarkTableValid` 预标记（key 不存在即无效）；room 要求 roomId 在 ids SET 内。
- **判活复核**：本地缓存每次都用 `xuantanRingHost`（`ring.ReadInternals` 读 `loadMap`）复核存活，host 一旦失效立即弃缓存回
  Redis，不破坏单激活。
- **存活键绝不重分配** → 正常扩容/滚动更新不触发迁移。

## 9. 故障与降级

| 场景                       | 行为                                 |
|--------------------------|------------------------------------|
| 未配置 KEY_XT_PLACEMENT_CONFIG / 文件缺失 / redis.addresses 为空 | 整体旁路，回退原生哈希 |
| actorType 非受管            | 旁路，回退原生哈希                          |
| Redis 操作失败（table/room）   | 记日志，`handled=true` + `ErrActorNoAddress`（可重试），**不降级**（降级会破坏 table 粘性、room 均衡） |
| ring 无该 actorType / 候选为空 | `ErrActorNoAddress`（可重试）           |
| 全部存活 host 都在排空（全体下线）  | 拒绝新分配（table/room），`ErrActorNoAddress`（可重试），**绝不硬塞排空 host** |
| 选中 host 并发下发中失效          | `ErrActorNoAddress`（可重试），下次重选      |

> 单次 Redis 操作超时 `xuantanRedisOpTimeout = 2s`，避免抖动阻塞热路径。

## 10. 外部业务约定

### room（有效 roomId 集合）

外部进程作为 roomId 的 owner，需把**全量有效 roomId** 维护到 SET：

```
SADD xt:dapr:ids:<actorType> <roomId> ...   # 新增/初始化
SREM xt:dapr:ids:<actorType> <roomId>        # 删除（绑定会在下次重分配脚本里被 HDEL 自清）
```

daprd 侧不负责增删 ids SET，只读取它做有效性校验与自清理。

### table（有效牌桌预标记）

业务在**预建牌桌实例之前**把绑定 key 预标记为有效（值 `"1"`）：

```
SET xt:dapr:bind:<actorType>:<tableId> 1 EX <bind_ttl>   # 预标记有效（core.IManager.MarkTableValid）
```

daprd 分配时把 `"1"` CAS 覆盖为实际 host；未预标记（key 不存在）的 tableId 一律拒绝分配。

## 11. 关键参数小结

| 项            | 取值                                        |
|--------------|-------------------------------------------|
| Redis 操作超时   | `2s`                                      |
| 本地缓存 GC 周期   | `1min`                                    |
| table 绑定 TTL | `dapr.bind_ttl`，默认 `15m`，不自动命中续期 |
| room 绑定 TTL  | 无（常驻，靠重分配/ids 清理）                         |
| table 有效性门禁 | 业务 `MarkTableValid` 预标记 `"1"`（未标记即拒绝）      |
| table 单激活    | Lua CAS（仅当值为 `"1"` 才覆盖为 host）              |
| room 单激活     | 单条 Lua（HGET 粘性 + 最少负载 HSET）               |
| 排空索引 key    | `xt:dapr:draining`（ZSET，member=host.Name，score=过期时刻ms） |
| 排空自标记 TTL   | `blockShutdownDuration + 1m`（覆盖 block 窗口）      |
| 排空集合进程缓存   | `1s`（分配路径读缓存，避免高峰下每次分配 ZRANGEBYSCORE）      |
| table (重)分配 RTT | **1 次**（单脚本；排空过滤在 daprd 侧用缓存）       |

## 12. 优雅退出与排空（draining，滚动更新）

滚动更新/缩容时，某 daprd 会收到 SIGTERM 进入优雅退出。若配置了 `dapr.io/block-shutdown-duration`，daprd 在真正
离开 placement ring 之前会**阻塞一段时间**继续服务在途请求（让长生命周期 actor 收尾）。这段窗口内该 host **仍在 ring**，
原生策略仍会把「新 actor」分配上来——但它马上就要退出，属于无效分配。排空机制解决这个问题：

**自标记（daprd 侧）**：daprd 一进入 block-shutdown 窗口，就把**自身 ring 地址** `host.Name`（`hostname:port`）写入排空索引
`xt:dapr:draining`（ZSET，score = now + ttl）。ttl 取 `blockShutdownDuration + 余量`，确保覆盖整个 block 窗口；host 离开
ring 后该条目靠 score 过期自清，读取方也顺带 `ZREMRANGEBYSCORE` 懒清历史成员。

- 接入点：`runtime.go` block-shutdown 分支 → `actors.Interface.MarkSelfDraining(ttl)` → `inflight.MarkSelfDraining`。
- 自身地址来源：`inflight.New(Options{Hostname,Port})` 时经 `xuantanSetSelfHost` 记为进程内单值 `xuantanSelfHost`。
- 未启用玄滩放置（无 `KEY_XT_PLACEMENT_CONFIG` / redis 关闭）时整体 no-op。

**候选过滤（策略侧）**：其它 daprd 在**（重）分配路径**读排空集合，把正在排空的 host 从候选剔除：

- **table**：首选哈希 host 若在排空集合 → 在非排空存活 host 中按 actorID 确定性改选；
- **room**：把排空 host 传入 Lua，选址时跳过；但排空 host **仍算存活**参与粘性判定，故其上的既有 room **不迁移**；
- **全在排空 = 全体服务下线**：table/room 均**拒绝新分配**、返回可重试错误，绝不硬塞到正在排空的 host（等新实例就绪后由上层重试）；
- **只拦新分配**：粘性/既有绑定完全不受影响——排空 host 在离开 ring 前继续承载已激活的 actor。

**与业务侧 drain-aware Close 配合**：业务 Go 进程同样收到 SIGTERM，其 `run.go` 信号处理会等待本机在途 actor 排空
（`manager.LiveCount()` 归零或超时）再 Close。两侧配合：daprd 用 block-shutdown 保住在途调用不被打断 + 自标记停止进新
actor；业务进程等既有 actor 自然收尾。要求 K8s `terminationGracePeriodSeconds` ≥ `block-shutdown-duration` ≥ 业务
drain 超时，留足收尾时间。
