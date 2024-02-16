COMPILER = /usr/bin/clang++
CFLAGS = -g
MODE = release

foo: foo.cc cache.cc httpd.cc \
	 store.cc store/manifest.cc store/bucket.cc store/lst.cc store/shard.cc
	$(COMPILER) \
		foo.cc cache.cc httpd.cc \
		store.cc store/manifest.cc store/bucket.cc store/lst.cc \
		store/shard.cc \
		-I. \
		$(shell pkg-config --libs --cflags --static ../seastar/build/release/seastar.pc) $(CFLAGS) -o foo

foo-debug: foo.cc cache.cc httpd.cc \
	store.cc store/manifest.cc store/bucket.cc store/lst.cc store/shard.cc
	/usr/bin/g++ \
		foo.cc cache.cc httpd.cc \
		store.cc store/manifest.cc store/bucket.cc store/lst.cc \
		store/shard.cc \
		-I. \
		$(shell pkg-config --libs --cflags ../seastar/build/debug/seastar.pc) $(CFLAGS) -o foo-debug

app: /opt/seastar/build/$(MODE)/libseastar.a app.cc
	$(COMPILER) app.cc $(shell pkg-config --libs --cflags --static /opt/seastar/build/$(MODE)/seastar.pc) $(CFLAGS) -o app

/opt/seastar/build/$(MODE)/libseastar.a:
	cd /opt/seastar && ./configure.py --mode="$(MODE)" --disable-dpdk --disable-hwloc --cflags="$(CFLAGS)" --compiler="$(COMPILER)"
	ninja -C /opt/seastar/build/$(MODE) libseastar.a
