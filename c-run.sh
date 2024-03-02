#!/bin/bash
#
# Based on scylladb/scylladb's dbuild tool
#
# Helps running stuff within a container context, so we don't have to deal
# with annoying dependencies when building against seastar.
#

cbin=${CBIN:-docker}

build_image_name="seastar-foo/builder:fedora39"
mydir=$(dirname "$(realpath "$0")")
groups=()

for gid in $(id -G); do
  groups+=(--group-add "$gid")
done

tmpdir=$(mktemp -d)

kill_it() {
  exit_code=$?
  rm -fr "${tmpdir}"
  exit $exit_code
}

trap kill_it SIGTERM SIGINT SIGHUP EXIT

${cbin} run -it \
  -u "$(id -u):$(id -g)" \
  "${groups[@]}" \
  -v /etc/passwd:/etc/passwd:ro \
  -v /etc/group:/etc/group:ro \
  --pids-limit -1 \
  --security-opt seccomp=unconfined \
  --security-opt label=disable \
  --network host \
  --cap-add SYS_PTRACE \
  --ulimit nofile="$(ulimit -Sn):$(ulimit -Hn)" \
  -v "${mydir}:${mydir}" \
  -v "${PWD}:${PWD}" \
  -v "${tmpdir}:/tmp" \
  -v "/etc/localtime:/etc/localtime:ro" \
  -w "${PWD}" \
  -e HOME="${HOME}" \
  "${build_image_name}" \
  "$@"
