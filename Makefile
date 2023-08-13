export PATH:=$(PATH):$(HOME)/.cargo/bin

.phony: build

build: format
	cargo build 

format:
	cargo fmt

clean:
	cargo clean

update:
	cargo update
