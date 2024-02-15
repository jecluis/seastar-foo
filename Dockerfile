# Builds a container image able to build seastar, build binaries linked against
# it, and run commands within said container's context.
#
# Mostly to maintain a stable environment where it's feasible to build seastar,
# and not having to fight dependencies on our host OS (*wink*wink*tumbleweed).
#
FROM docker.io/fedora:39

ADD ./seastar/install-dependencies.sh ./
RUN ./install-dependencies.sh

RUN dnf install -y clang
RUN dnf install -y systemd-devel
RUN dnf install -y compiler-rt

CMD /bin/bash
