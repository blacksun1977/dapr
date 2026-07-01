#!/usr/bin/env bash
#
# 玄滩(xuantan)自定义放置策略 —— 仅构建 daprd(sidecar) 镜像。
#
# 我们的改动只在 daprd sidecar 里(见 pkg/.../inflight/inflight_xuantan.go)，
# 控制面组件(placement/operator/sentry/injector/scheduler) 无需重建。
# 本脚本因此只编译 daprd 一个二进制并打成镜像，避免构建其余 5 个组件。
#
# 用法:
#   REGISTRY=registry.example.com/dapr ./xuantan-build-daprd.sh           # 只构建
#   REGISTRY=registry.example.com/dapr PUSH=true ./xuantan-build-daprd.sh # 构建并推送
#
# 可配置环境变量:
#   REGISTRY    镜像仓库前缀(必填), 例: registry.example.com/dapr
#   IMAGE_NAME  镜像名(默认 daprd)
#   TAG         镜像 tag(默认 xt-<时间戳>)
#   ARCH        目标架构 amd64|arm64(默认 amd64, 需与集群节点一致)
#   PUSH        是否推送(默认 false)
#
set -euo pipefail
cd "$(dirname "$0")"

REGISTRY="${REGISTRY:-}"
IMAGE_NAME="${IMAGE_NAME:-daprd}"
TAG="${TAG:-xt-$(date +%Y%m%d-%H%M%S)}"
ARCH="${ARCH:-amd64}"
PUSH="${PUSH:-false}"

if [[ -z "${REGISTRY}" ]]; then
  echo "ERROR: 请设置 REGISTRY, 例如 REGISTRY=registry.example.com/dapr" >&2
  exit 1
fi

IMAGE="${REGISTRY}/${IMAGE_NAME}:${TAG}"
BIN_DIR="dist/linux_${ARCH}/release"

echo ">> [1/3] 编译 daprd 二进制 (linux/${ARCH}, 静态 CGO=0)"
make build BINARIES=daprd GOOS=linux GOARCH="${ARCH}" CGO=0

if [[ ! -x "${BIN_DIR}/daprd" ]]; then
  echo "ERROR: 未找到 ${BIN_DIR}/daprd" >&2
  exit 1
fi

echo ">> [2/3] 构建 daprd 镜像: ${IMAGE}"
# Dockerfile 的构建上下文必须是包含 daprd 二进制的目录; PKG_FILES=daprd 仅拷贝 daprd。
docker build --platform "linux/${ARCH}" \
  --build-arg PKG_FILES=daprd \
  -f docker/Dockerfile \
  -t "${IMAGE}" \
  "${BIN_DIR}"

if [[ "${PUSH}" == "true" ]]; then
  echo ">> [3/3] 推送镜像: ${IMAGE}"
  docker push "${IMAGE}"
else
  echo ">> [3/3] 跳过推送 (设置 PUSH=true 可推送)"
fi

echo
echo "完成。自定义 sidecar 镜像 = ${IMAGE}"
echo "最小接入(只换 sidecar 镜像, 控制面不动):"
echo "  helm upgrade dapr dapr/dapr -n dapr-system --reuse-values \\"
echo "    --set dapr_sidecar_injector.image.name=${IMAGE}"
