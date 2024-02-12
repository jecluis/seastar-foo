COMPILER = /usr/bin/clang++
CFLAGS = -g
MODE = release

foo: foo.cc cache.cc sharded_cache.cc httpd.cc store.cc
	$(COMPILER) foo.cc cache.cc sharded_cache.cc httpd.cc store.cc \
		$(shell pkg-config --libs --cflags --static ../seastar/build/$(MODE)/seastar.pc) $(CFLAGS) -o foo

app: /opt/seastar/build/$(MODE)/libseastar.a app.cc
	$(COMPILER) app.cc $(shell pkg-config --libs --cflags --static /opt/seastar/build/$(MODE)/seastar.pc) $(CFLAGS) -o app

/opt/seastar/build/$(MODE)/libseastar.a:
	cd /opt/seastar && ./configure.py --mode="$(MODE)" --disable-dpdk --disable-hwloc --cflags="$(CFLAGS)" --compiler="$(COMPILER)"
	ninja -C /opt/seastar/build/$(MODE) libseastar.a
