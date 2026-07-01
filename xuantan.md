# 玄滩（xuantan）定制 Dapr —— 打包与部署

本文说明如何**打镜像**、以及如何**最小改动**把定制能力融入原始 Dapr 部署。

策略的设计与实现详见 [`pkg/actors/internal/placement/loops/disseminator/inflight/inflight_xuantan.md`](pkg/actors/internal/placement/loops/disseminator/inflight/inflight_xuantan.md)。

---

## 1. 改了什么（影响面）

定制只发生在 **daprd（sidecar）** 内的 actor 放置解析逻辑：

| 文件 | 改动 |
|---|---|
| `pkg/actors/internal/placement/loops/disseminator/inflight/inflight_xuantan.go` | **新增**：自定义放置策略（table 粘性 / room 最少负载+容量）全部实现 |
| `pkg/actors/internal/placement/loops/disseminator/inflight/inflight.go` | **两行**：`New()` 调一次 `xuantanInit()`；`resolve()` 开头加 `resolveXuantan()` 钩子（`handled=false` 即回退原生哈希） |
| `..._test.go` / `*.md` | 测试与文档 |

影响面结论：

- **只有 daprd 需要重建镜像**；控制面组件（placement / operator / sentry / injector / scheduler）**完全不用动**。
- 未配置 `KEY_XT_PLACEMENT_REDIS_ADDR` 时策略整体旁路，行为与官方 daprd 完全一致——所以**升级镜像本身是零风险的**，真正启用由环境变量控制。

---

## 2. 打镜像

### 2.1 一键脚本（推荐）

[`xuantan-build-daprd.sh`](xuantan-build-daprd.sh) 只编译并打包 daprd 一个组件：

```bash
# 只构建本地镜像
REGISTRY=registry.example.com/dapr ./xuantan-build-daprd.sh

# 构建并推送（tag 默认 xt-<时间戳>，可用 TAG= 覆盖）
REGISTRY=registry.example.com/dapr TAG=xt-1.0.0 PUSH=true ./xuantan-build-daprd.sh

# arm64 集群
REGISTRY=registry.example.com/dapr ARCH=arm64 PUSH=true ./xuantan-build-daprd.sh
```

脚本做三件事：
1. `make build BINARIES=daprd GOOS=linux GOARCH=$ARCH CGO=0` 交叉编译静态 daprd；
2. `docker build --build-arg PKG_FILES=daprd -f docker/Dockerfile <bindir>` 只打 daprd 镜像；
3. 可选 `docker push`。

产物镜像：`${REGISTRY}/daprd:${TAG}`。

### 2.2 等价的手工命令

```bash
ARCH=amd64
make build BINARIES=daprd GOOS=linux GOARCH=$ARCH CGO=0
docker build --platform linux/$ARCH --build-arg PKG_FILES=daprd \
  -f docker/Dockerfile -t registry.example.com/dapr/daprd:xt-1.0.0 \
  dist/linux_${ARCH}/release
docker push registry.example.com/dapr/daprd:xt-1.0.0
```

> 基础镜像是 `gcr.io/distroless/static:nonroot`，二进制默认 `CGO=0` 静态编译，可从 macOS/Linux 任意主机交叉构建。多架构可用 `docker buildx ... --platform linux/amd64,linux/arm64 --push`。

---

## 3. 最小改动融入现有 Dapr 部署

关键点：sidecar 注入器（injector）决定注入哪个 daprd 镜像，其 Helm 值
`dapr_sidecar_injector.image.name` 的解析规则是：

- 值**含 `/`** → 当作**完整镜像引用**（含 registry 与 tag）直接使用；
- 值**不含 `/`** → 拼成 `{{global.registry}}/{{image.name}}:{{global.tag}}`。

所以只要把它设成我们镜像的**完整引用**，就**只替换被注入的 sidecar(daprd)**，控制面镜像保持官方版本不变——这是改动面最小的方式。

### 3.1 Helm 升级（保留原有 values）

```bash
helm upgrade dapr dapr/dapr -n dapr-system --reuse-values \
  --set dapr_sidecar_injector.image.name=registry.example.com/dapr/daprd:xt-1.0.0
```

或在 `values.yaml` 里：

```yaml
dapr_sidecar_injector:
  image:
    name: registry.example.com/dapr/daprd:xt-1.0.0   # 含 "/" => 完整引用，仅改 sidecar
```

> 对比：若改 `global.registry` / `global.tag` 会**同时**换掉所有控制面组件镜像，不是最小改动，不推荐。

### 3.2 启用策略（给 sidecar 注入环境变量）

镜像升级后策略默认旁路；要启用，需给 daprd sidecar 注入 `KEY_XT_PLACEMENT_*` 环境变量。
通过业务 Pod 的注解 `dapr.io/env` 注入（逗号分隔 `KEY=VALUE`）：

```yaml
metadata:
  annotations:
    dapr.io/enabled: "true"
    dapr.io/app-id: "game"
    dapr.io/env: "KEY_XT_PLACEMENT_REDIS_ADDR=redis.prod:6379,KEY_XT_PLACEMENT_STICKY_TYPE_TABLE=table_py\\,table,KEY_XT_PLACEMENT_STICKY_TYPE_ROOM=room_py\\,room,KEY_XT_PLACEMENT_ROOM_CAP_PER_HOST=10"
```

注意：
- `dapr.io/env` 内多个变量用 `,` 分隔，故 actorType 列表里的 `,` 需转义为 `\,`（YAML 中再加一层转义即 `\\,`）。
- **凡是承载 table/room 这些 actorType 的应用，都要带相同的环境变量并使用定制 sidecar 镜像**，否则集群内行为不一致。
- 全部环境变量含义见 [inflight_xuantan.md 第 3 节](pkg/actors/internal/placement/loops/disseminator/inflight/inflight_xuantan.md#3-环境变量)。

### 3.3 生效与验证

```bash
# 注入器会对新建/重启的 Pod 注入新 sidecar；滚动重启相关应用即可
kubectl rollout restart deploy/<your-app> -n <ns>

# 确认 sidecar 用的是定制镜像
kubectl get pod <pod> -n <ns> -o jsonpath='{.spec.containers[?(@.name=="daprd")].image}'

# 确认策略已启用（应看到 "xuantan placement: enabled, types=..."）
kubectl logs <pod> -n <ns> -c daprd | grep -i "xuantan placement"
```

### 3.4 回滚

```bash
# 改回官方 sidecar 镜像并重启应用即可；或仅移除 KEY_XT_PLACEMENT_REDIS_ADDR 让策略旁路
helm upgrade dapr dapr/dapr -n dapr-system --reuse-values \
  --set dapr_sidecar_injector.image.name=daprd
kubectl rollout restart deploy/<your-app> -n <ns>
```

---

## 4. 依赖与前置

- 一个可达的 **Redis**（绑定表 / room 有效 id 集合的存储），地址即 `KEY_XT_PLACEMENT_REDIS_ADDR`。
- room 策略要求外部业务进程维护有效 roomId 集合 `SET xt:dapr:ids:<actorType>`（详见 inflight_xuantan.md 第 10 节）。
- 集群节点架构与 `ARCH` 一致。

---

## 5. 测试

```bash
# 单体测试（无需 Redis）
go test ./pkg/actors/internal/placement/loops/disseminator/inflight/ -run TestXuantan -v

# 集成测试（需 Redis；用 docker 起一个临时实例）
docker run -d --rm --name xt-redis-test -p 6399:6379 redis:7-alpine
XT_TEST_REDIS_ADDR=127.0.0.1:6399 \
  go test ./pkg/actors/internal/placement/loops/disseminator/inflight/ -run TestXuantan -v -count=1
docker rm -f xt-redis-test
```
