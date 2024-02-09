#!/bin/bash
#
# Based on scylladb/scylladb's dbuild tool
#
# Helps running stuff within a container context, so we don't have to deal
# with annoying dependencies when building against seastar.
#
mydir=$(dirname "$(realpath "$0")")
groups=()

for gid in $(id -G); do
  groups+=(--group-add "$gid")
done

tmpdir=$(mktemp -d)

kill_it() {
  rm -fr "${tmpdir}"
}

trap kill_it SIGTERM SIGINT SIGHUP EXIT

docker run -it \
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
  seastar-build:fedora39 \
  "$@"
