#!/bin/bash

info() {
  echo "[INFO] $*" >/dev/stderr
}

error() {
  echo "[ERROR] $*" >/dev/stderr
}

cbin="docker"
build_image_name="seastar-foo/builder:fedora39"

usage() {
  cat <<EOF
usage: $0 [options]

options:
  --with-podman   Use podman instead of docker
  --help          This message
EOF
}

while [[ $# -gt 0 ]]; do
  case $1 in
  --with-podman)
    cbin="podman"
    export CBIN="podman"
    ;;
  --help)
    usage
    exit 0
    ;;
  esac
  shift 1
done

{ [[ -d "seastar" ]] && [[ -d ".git" ]]; } ||
  { error "must be run at the root of the repository!" && exit 1; }

info "updating seastar submodule"
git submodule update --init --recursive || exit 1

info "creating build docker image at ${build_image_name}"
${cbin} build -t "${build_image_name}" . || exit 1

info "building seastar"

pushd seastar || exit 1

info "building seastar debug build"
../c-run.sh ./configure.py --mode=debug --c++-standard=20 || exit 1
../c-run.sh ninja -C build/debug || exit 1

info "building seastar release build"
../c-run.sh ./configure.py --mode=release --c++-standard=20 || exit 1
../c-run.sh ninja -C build/release || exit 1

popd || exit 1

info "building seastar-foo"

pushd src || exit 1

info "building release binary"
../c-run.sh ./run.sh --build --build-only || exit 1
info "building debug binary"
../c-run.sh ./run.sh --build --build-only --debug-target || exit 1

popd || exit 1

echo "[SUCCESS] ready to play!"
