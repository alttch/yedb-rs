VERSION=0.5.2

all: test

test:
	cargo build
	./target/debug/yedb-server /tmp/yedb-test-db1 --pid-file /tmp/yedb-server-test.pid -v &
	sleep 0.1
	#cargo test -- --test-threads=1 --nocapture
	cargo test -- --nocapture
	kill `cat /tmp/yedb-server-test.pid`
	rm -rf /tmp/yedb-test-db1

clean:
	find . -type d -name target -exec rm -rf {} \; || exit 0
	find . -type f -name Cargo.lock -exec rm -f {} \; || exit 0

tag:
	git tag -a v${VERSION} -m v${VERSION}
	git push origin --tags

ver:
	sed -i 's/^version = ".*/version = "${VERSION}"/g' Cargo.toml

doc:
	grep -v "^//!" src/lib.rs > src/lib.rs.tmp
	sed 's|^|//! |g' README.md > src/lib.rs
	cat src/lib.rs.tmp >> src/lib.rs
	rm -f src/lib.rs.tmp
	cargo doc

pub: doc test publish-cargo-crate

publish-cargo-crate:
	cargo publish
