#!/usr/bin/env python3

# Copyright 2024 Joao Eduardo Luis <joao@1e3ms.io>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
import random
import signal
import string
from typing import Any

import aiohttp
import click

do_stop = False

map_lock = asyncio.Lock()
key_values: dict[str, str] = {}
keys: list[str] = []


def _stop() -> None:
    global do_stop
    print("stopping...")
    do_stop = True


def sig_handler(signum: int, frame: Any) -> None:
    _stop()


signal.signal(signal.SIGINT, sig_handler)
signal.signal(signal.SIGTERM, sig_handler)


def _gen_random_str(len: int) -> str:
    return "".join(random.choices(string.ascii_letters + string.digits, k=len))


async def _put(addr: str) -> None:
    key = _gen_random_str(random.randrange(10, 256))
    data = _gen_random_str(random.randrange(10, 1024 * 1024))  # 10 bytes to 1MB

    async with aiohttp.ClientSession() as session:
        try:
            async with session.put(f"{addr}/put/{key}", data=data) as res:
                print(f"PUT: res={res.status}")
                if res.ok:
                    async with map_lock:
                        key_values[key] = data
                        keys.append(key)
        except:
            print(f"PUT: error on key {key}")
            _stop()


async def _get(addr: str) -> None:
    async with map_lock:
        if len(keys) == 0:
            print("GET: no keys yet")
            return
        key = random.choice(keys)
        data = key_values[key]

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{addr}/get/{key}") as res:
                print(f"GET: res={res.status}")
                if res.ok:
                    res_data = await res.text()
                    if res_data != data:
                        print(f"GET: failed data sanity on key '{key}'")
                else:
                    print(f"GET: failed obtaining key '{key}': res={res.status}")
        except:
            print(f"GET: error on key {key}")
            _stop()


async def _remove_key(addr: str, key: str) -> None:
    async with aiohttp.ClientSession() as session:
        try:
            async with session.delete(f"{addr}/remove/{key}") as res:
                print(f"DELETE: res={res.status}")
                if not res.ok:
                    print(f"DELETE: failed removing key '{key}'")
        except:
            print(f"DELETE: error on key {key}")
            _stop()


async def _remove(addr: str) -> None:
    async with map_lock:
        if len(keys) == 0:
            print("DELETE: no keys yet")
            return
        key = random.choice(keys)
        keys.remove(key)
        del key_values[key]

    await _remove_key(addr, key)


async def _list(addr: str) -> None:
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(f"{addr}/list") as res:
                print(f"LIST: res={res.status}")
                if res.ok:
                    keys = [x for x in (await res.text()).split("\n") if len(x) > 0]
                    print(f"LIST: got {len(keys)}")
        except:
            print("LIST: error")
            _stop()


async def _cleanup(addr: str) -> None:
    while len(keys) > 0:
        async with map_lock:
            k = keys.pop()
            del key_values[k]
        await _remove_key(addr, k)


ops = [_put, _get, _remove, _list]


async def run_one(addr: str) -> None:

    while not do_stop:
        op = random.choice(ops)
        await op(addr)
        await asyncio.sleep(1)
        pass

    print("stopping task...")


async def badger(addr: str, cores: int) -> None:

    procs = [run_one(addr) for _ in range(0, cores)]
    print(f"running {cores} tasks")
    await asyncio.gather(*procs)

    cleanup = [_cleanup(addr) for _ in range(0, cores)]
    await asyncio.gather(*cleanup)


@click.command()
@click.argument("address", type=str, required=True)
@click.option("--num-cores", type=int, default=1, help="number of available cores")
def cli(address: str, num_cores: int):
    print(f"badger {address}, cores: {num_cores}")
    asyncio.run(badger(address, num_cores))


if __name__ == "__main__":
    cli()
