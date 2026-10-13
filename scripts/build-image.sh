#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Build the skew-enhanced datagen container image.
#
#   scripts/build-image.sh [tag]     # default tag: ldbc/datagen-enhance:latest
#
# Base image strategy:
#   1. if `ubuntu:20.04` is present locally (or Docker Hub is reachable) use it;
#   2. otherwise import a minimal rootfs from cdimage.ubuntu.com as
#      ldbc/ubuntu-base:20.04 and build against it.
# The whole build downloads from github.com, repo1.maven.org and
# archive.apache.org only.
# ---------------------------------------------------------------------------
set -euo pipefail

TAG=${1:-ldbc/datagen-enhance:latest}
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
UBUNTU_BASE=ldbc/ubuntu-base:20.04
BUILD_ARGS=()

have_base() { docker image inspect "$1" >/dev/null 2>&1; }

if have_base ubuntu:20.04 || docker pull -q ubuntu:20.04 >/dev/null 2>&1; then
  echo "== base image: ubuntu:20.04"
elif have_base "$UBUNTU_BASE"; then
  echo "== base image: $UBUNTU_BASE (cached)"
  BUILD_ARGS+=(--build-arg "BASE=$UBUNTU_BASE")
else
  echo "== docker.io unreachable; importing ubuntu-base rootfs from cdimage.ubuntu.com"
  curl -fSsL "https://cdimage.ubuntu.com/ubuntu-base/releases/20.04.1/release/ubuntu-base-20.04.1-base-amd64.tar.gz" \
    | docker import - "$UBUNTU_BASE"
  BUILD_ARGS+=(--build-arg "BASE=$UBUNTU_BASE")
fi

docker build "${BUILD_ARGS[@]}" -t "$TAG" "$REPO"
echo "== built $TAG"
