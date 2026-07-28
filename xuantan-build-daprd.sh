#!/usr/bin/env bash
#
# 玄滩(xuantan)dapr —— 统一构建 sidecar(daprd) 与 放置控制面(placement) 到同一镜像。
#
# 【为什么合并】本 fork 基于官方 ~v1.18(pkg/placement 与全部 proto 与 v1.18.1 逐字节一致)。
# 此前只重建 daprd、placement 却复用官方旧镜像(1.15.4),导致 daprd(1.18 代码，成员按活 stream
# 派生)连着 1.15.4 placement(旧 raft-FSM + 一次性 faultyHostDetect)——版本错配，出现死宿主孤儿
# 残留 / 脑裂。故此处【从同一份源码同时编译 daprd + placement】，打进同一镜像、同一 tag，
# 保证控制面与 sidecar 版本严格一致。
#
# 镜像内含两个二进制(均置于 /)：
#   /daprd      —— 注入到业务 Pod 的 sidecar(消费方 command 指定 /daprd)
#   /placement  —— actor 放置环控制面(placement chart 的 command 指定 /placement)
# 不设 ENTRYPOINT：daprd 注入器与 placement StatefulSet 各自显式指定 command，互不影响。
#
# 用 docker buildx 一次构建 amd64 + arm64 多架构镜像，同一 tag 即多架构 manifest。
#
# 用法:
#   ./xuantan-build-daprd.sh                 # 构建并推送(默认 PUSH=true)
#   PUSH=false ./xuantan-build-daprd.sh      # 仅验证构建，不推送
#   TAG=v1.1 ./xuantan-build-daprd.sh        # 覆盖 tag
#
# 可配置环境变量:
#   REGISTRY    镜像仓库前缀(默认 harbor.ops.tuyoops.com/xuantan)
#   IMAGE_NAME  镜像名(默认 xtdapr)
#   TAG         镜像 tag(默认 v1.0)
#   ARCHS       目标架构列表(默认 "amd64 arm64")
#   BUILDER     buildx builder 名称(默认 poker)
#   PUSH        是否推送(默认 true)
#
set -euo pipefail
cd "$(dirname "$0")"

REGISTRY="${REGISTRY:-harbor.ops.tuyoops.com/xuantan}"
IMAGE_NAME="${IMAGE_NAME:-xtdapr}"
TAG="${TAG:-v1.1}"
ARCHS="${ARCHS:-amd64 arm64}"
BUILDER="${BUILDER:-poker}"
PUSH="${PUSH:-true}"

# 一并构建的二进制：sidecar + 放置控制面(同源同版本)。
BINARIES="daprd placement"

if [[ -z "${REGISTRY}" ]]; then
  echo "ERROR: 请设置 REGISTRY, 例如 REGISTRY=registry.example.com/dapr" >&2
  exit 1
fi

IMAGE="${REGISTRY}/${IMAGE_NAME}:${TAG}"

# 组装 buildx 的 --platform 参数, 如 "linux/amd64,linux/arm64"。
PLATFORMS=""
for ARCH in ${ARCHS}; do
  PLATFORMS+="linux/${ARCH},"
done
PLATFORMS="${PLATFORMS%,}"

echo ">> [1/3] 交叉编译各架构二进制 (${BINARIES}, 静态 CGO=0)"
for ARCH in ${ARCHS}; do
  BIN_DIR="dist/linux_${ARCH}/release"
  echo "   - 编译 linux/${ARCH}: ${BINARIES}"
  make build BINARIES="${BINARIES}" GOOS=linux GOARCH="${ARCH}" CGO=0
  for BIN in ${BINARIES}; do
    if [[ ! -x "${BIN_DIR}/${BIN}" ]]; then
      echo "ERROR: 未找到 ${BIN_DIR}/${BIN}" >&2
      exit 1
    fi
  done
done

echo ">> [2/3] 准备 buildx builder: ${BUILDER}"
if ! docker buildx inspect "${BUILDER}" >/dev/null 2>&1; then
  docker buildx create --name "${BUILDER}" --driver docker-container --bootstrap
fi

echo ">> [3/3] 构建多架构镜像 (${PLATFORMS}): ${IMAGE}"
# 构建上下文取 dist/, 由 Dockerfile 按 TARGETARCH 选择对应架构的二进制。
# 多架构镜像无法 --load 到本地 docker, 因此仅在 PUSH=true 时导出(--push)。
if [[ "${PUSH}" == "true" ]]; then
  OUTPUT_FLAG="--push"
else
  OUTPUT_FLAG=""
  echo "   - 未设置 PUSH=true, 仅验证构建(不导出镜像)"
fi

docker buildx build \
  --builder "${BUILDER}" \
  --platform "${PLATFORMS}" \
  ${OUTPUT_FLAG} \
  -t "${IMAGE}" \
  -f - \
  dist <<'DOCKERFILE'
FROM gcr.io/distroless/static:nonroot
ARG TARGETARCH
WORKDIR /
# 同镜像内放两个二进制：daprd(sidecar) 与 placement(控制面)。
# 不设 ENTRYPOINT：由各消费方(daprd 注入器 / placement StatefulSet) 显式指定 command。
COPY /linux_${TARGETARCH}/release/daprd /
COPY /linux_${TARGETARCH}/release/placement /
USER 65532:65532
DOCKERFILE

echo
echo "完成。"
if [[ "${PUSH}" == "true" ]]; then
  docker pull "${IMAGE}" || true
  echo "多架构统一镜像 (${PLATFORMS}) = ${IMAGE}"
  echo "  内含: /daprd (sidecar) + /placement (控制面)，同源同版本。"
else
  echo "已验证多架构构建 (${PLATFORMS}); 设置 PUSH=true 才会推送镜像 ${IMAGE}"
fi
