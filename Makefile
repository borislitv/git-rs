.PHONY: check integration failures chart kubernetes image bench

HELM ?= helm
K3S_CONTAINER ?= git-rs-k3s-test

check:
	cargo fmt -- --check
	cargo clippy --all-targets -- -D warnings
	cargo test --locked --all-targets

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

image:
	docker build --target release -t git-rs:dev .

bench:
	cargo bench --bench cache
