# foo

## Building

To build the code in this repository, we'll need to ensure we have a `seastar`
library to build against. The `Makefile` assumes `seastar` has been built under
the submodule located in `seastar`, at the root of the repository.

The `Makefile` defines two targets: `foo`, which relies on a seastar release
build, and `foo-debug`, relying on a `seastar` debug build. Building either
requires having previously built `seastar`.

To make the process of building and running the binaries, we have created a
consistent build toolchain, relying on a container image to build the various
artifacts. We provide helper scripts to set up the environment, and to build and
run binaries.

### Setting up

Running the `build.sh` script at the root of the repository, a container image
will be built, that can then be used to build both `seastar` and the `foo`
binaries. This script assumes `docker` to be available, but allows for `podman`
to be used instead by providing the `--use-podman` option.

Following container image creation, `seastar` and the `foo` binaries will be
built. Relying on the `c-run.sh` script, which runs commands within a container
image's instance, we are able to build the binaries using the toolchain
available in the image previously built, while still using our filesystem. We
recommend looking into the script to understand how it works.

To setup the environment, simply run

    ./build.sh

at the root of the repository.

### Building and Running

Within the `src/` directory there's a `run.sh` script. This can be used to both
build and run the `foo` binaries. We recommend reading it to better understand
how it works. `./run.sh --help` will show its usage.

We rely on the `c-run.sh` script at the root of the repository to execute the
`run.sh` script within the context of the toolchain image.

To simply build the `foo` release binary, we can thus execute

    ../c-run.sh ./run.sh --build --build-only

For a debug binary, we should add `--debug-target` to the options list.

If we want to just run the binary, without building it, we can run instead

    ../c-run.sh ./run.sh

While building first and then running the binary will require the `--build`
option to be provided.

Please check `./run.sh --help` for other options that may be of interest,
especially the `--store-path` option to specify where the data store should be kept.

## LICENSE

This work is licensed under the Apache License, version 2.0, unless otherwise
specified, either by individual files or submodules (see `seastar/LICENSE` for
further information about seastar's licensing).

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
