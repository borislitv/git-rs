.PHONY: check cache-build cache-check integration failures chart kubernetes istio image bench

HELM ?= helm
K3S_CONTAINER ?= git-rs-k3s-test

check:
	cargo fmt -- --check
	cargo clippy --all-targets -- -D warnings
	cargo test --locked --all-targets

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
