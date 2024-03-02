#!/bin/bash

store_path="store.foo"

usage() {
  cat <<EOF
usage: $0 [options]

options:
  --debug-target      Use a debug build
  --build-only        Only build the binary, don't run.
  --build | -b        Builds the binary as well.
  --store-path PATH   Where to keep the data store (default: ${store_path})
  --with-debug        Run with debug flags
  -- [ARGS]           Pass additional arguments to binary
  --help              This message
EOF
}

do_run=1
build=0
tgt=foo
seastar_path=
debug_args=
args=()

while [[ $# -gt 0 ]]; do
  case $1 in
  --debug-target)
    tgt="foo-debug"
    seastar_path="$(realpath ../seastar/build/debug)"
    ;;
  --build-only)
    do_run=0
    ;;
  --build | -b)
    build=1
    ;;
  --store-path)
    store_path=$2
    shift 1
    ;;
  --with-debug)
    debug_args=(
      "--abort-on-seastar-bad-alloc"
      "--dump-memory-diagnostics-on-alloc-failure-kind=all"
    )
    ;;
  --help)
    usage
    exit 0
    ;;
  --)
    shift 1
    args=("$@")
    break
    ;;
  *)
    echo "unknown option '$1'" >/dev/stderr
    usage
    exit 1
    ;;
  esac
  shift 1
done

if [[ $build -eq 1 ]]; then
  make ${tgt} || exit 1
fi

if [[ $do_run -eq 1 ]]; then

  LD_LIBRARY_PATH="${seastar_path}" ./${tgt} \
    --default-log-level=debug \
    "--store-path=${store_path}" \
    "${debug_args[*]}" \
    "${args[*]}" || exit 1

fi
