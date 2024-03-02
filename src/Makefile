COMPILER = /usr/bin/clang++
CFLAGS = -g

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
