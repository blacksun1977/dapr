#!/usr/bin/env bash
#
# 玄滩(xuantan)自定义放置策略 —— 仅构建 daprd(sidecar) 镜像。
#
# 我们的改动只在 daprd sidecar 里(见 pkg/.../inflight/inflight_xuantan.go)，
# 控制面组件(placement/operator/sentry/injector/scheduler) 无需重建。
# 本脚本因此只编译 daprd 一个二进制并打成镜像，避免构建其余 5 个组件。
#
# 本脚本用 docker buildx builder 一次性构建 amd64 + arm64 的多架构镜像,
# 使用同一个 tag(不加架构后缀),推送后即为一个多架构 manifest,
# amd64/arm64 节点均可拉取同一个 tag。
#
# 用法:
#   REGISTRY=registry.example.com/dapr ./xuantan-build-daprd.sh           # 只构建(不导出)
#   REGISTRY=registry.example.com/dapr PUSH=true ./xuantan-build-daprd.sh # 构建并推送多架构镜像
#
# 可配置环境变量:
#   REGISTRY    镜像仓库前缀(必填), 例: registry.example.com/dapr
#   IMAGE_NAME  镜像名(默认 daprd)
#   TAG         镜像 tag(默认 xt-<时间戳>)
#   ARCHS       目标架构列表(默认 "amd64 arm64")
#   BUILDER     buildx builder 名称(默认 xuantan-builder)
#   PUSH        是否推送(默认 false)
#
set -euo pipefail
cd "$(dirname "$0")"

REGISTRY="harbor.ops.tuyoops.com/xuantan"
IMAGE_NAME="${IMAGE_NAME:-daprd}"
TAG="${TAG:-xt.1.15.4}"
ARCHS="${ARCHS:-amd64 arm64}"
BUILDER="${BUILDER:-xuantan-builder}"
PUSH="${PUSH:-false}"

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

echo ">> [1/3] 交叉编译各架构 daprd 二进制 (静态 CGO=0)"
for ARCH in ${ARCHS}; do
  BIN_DIR="dist/linux_${ARCH}/release"
  echo "   - 编译 linux/${ARCH}"
  make build BINARIES=daprd GOOS=linux GOARCH="${ARCH}" CGO=0
  if [[ ! -x "${BIN_DIR}/daprd" ]]; then
    echo "ERROR: 未找到 ${BIN_DIR}/daprd" >&2
    exit 1
  fi
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
  --push \
  -t "${IMAGE}" \
  -f - \
  dist <<'DOCKERFILE'
FROM gcr.io/distroless/static:nonroot
ARG TARGETARCH
WORKDIR /
COPY /linux_${TARGETARCH}/release/daprd /
USER 65532:65532
DOCKERFILE

echo
echo "完成。"
if [[ "${PUSH}" == "true" ]]; then
  echo "多架构 sidecar 镜像 (${PLATFORMS}) = ${IMAGE}"
else
  echo "已验证多架构构建 (${PLATFORMS}); 设置 PUSH=true 才会推送镜像 ${IMAGE}"
fi
echo "最小接入(只换 sidecar 镜像, 控制面不动):"
echo "  helm upgrade dapr dapr/dapr -n dapr-system --reuse-values \\"
echo "    --set dapr_sidecar_injector.image.name=${IMAGE}"
