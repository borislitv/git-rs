.PHONY: check core-source-check vendor-check cache-build cache-check integration failures chart kubernetes istio image bench

HELM ?= helm
K3S_CONTAINER ?= git-rs-k3s-test
VENDOR_TARGET_DIR ?= $(abspath $(if $(CARGO_TARGET_DIR),$(CARGO_TARGET_DIR),target)/vendor)
GITOXIDE_DIR ?= .local/gitoxide
VENDOR_PACKAGES = -p gix-pack -p gix-zlib -p gix-blame -p gix-diff -p gix-imara-diff

core-source-check:
	python3 tools/check-core-source.py
	python3 -B tests/core_source_test.py

check: core-source-check
	python3 -B tests/integration_process_test.py
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --locked --workspace --all-targets

# Test the exact external checkout, never a copied application source subtree.
vendor-check: core-source-check
	python3 tools/check-core-source.py --dependency-root "$(GITOXIDE_DIR)"
	cargo test --locked --manifest-path "$(GITOXIDE_DIR)/Cargo.toml" --target-dir "$(VENDOR_TARGET_DIR)" $(VENDOR_PACKAGES)
	cargo clippy --locked --manifest-path "$(GITOXIDE_DIR)/Cargo.toml" --target-dir "$(VENDOR_TARGET_DIR)" $(VENDOR_PACKAGES) --lib --tests -- -D warnings
	cargo test --locked --manifest-path "$(GITOXIDE_DIR)/Cargo.toml" --target-dir "$(VENDOR_TARGET_DIR)" -p gix-blame -p gix-diff -p gix-imara-diff --doc

cache-build:
	cargo build --locked --bin git-rs --example cache-process
	cargo test --locked --test cache_contract --test cache_coordinator --test cache_budget --no-run

# Prebuild outside the bounded measurement unit with make cache-build.
cache-check:
	python3 -B tests/cache_gate.py run --target-dir "$${CARGO_TARGET_DIR:-target}"

integration:
	cargo build --locked
	python3 tests/integration.py --provision --large-mib 64

failures:
	cargo build --locked
	python3 tests/failures.py --provision

chart:
	python3 tests/chart.py --helm $(HELM)

kubernetes: image
	python3 tests/kubernetes.py --cluster-container $(K3S_CONTAINER) --helm $(HELM)

# Uses an existing production image; owns a separate disposable cluster.
istio:
	python3 tests/istio.py --helm $(HELM)

image:
	docker build --target release -t git-rs:dev .

bench:
	cargo bench --bench cache
