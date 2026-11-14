.PHONY: check core-source-check vendor-check cache-build cache-check blame-core-build blame-core-check blame-core-cleanup metadata integration receive upload history failures maintenance cache-matrix chart kubernetes istio image bench
.PHONY: blame-gate-prepare blame-gate-check blame-gate-cleanup
.PHONY: leader-dependency-build leader-dependency-check runtime-contracts-build vm-package
.PHONY: istio-prepare

HELM ?= helm
VENDOR_TARGET_DIR ?= $(abspath $(if $(CARGO_TARGET_DIR),$(CARGO_TARGET_DIR),target)/vendor)
GITOXIDE_DIR ?= .local/gitoxide
CODEC_DIR ?= .local/iconv-native
KUBERT_DIR ?= .local/kubert
BLAME_GATE_BUNDLE ?= $(abspath .local/blame-prebuilt)
BLAME_GATE_BUILDER_IMAGE ?= git-rs:blame-build
BLAME_GATE_IMAGE ?= git-rs:blame-test
RUNTIME_IMAGE ?= git-rs:dev
RUNTIME_FIXTURE_IMAGE ?= git-rs:test-server
ORACLE_IMAGE ?= git-rs:test
ISTIO_ARTIFACTS ?= $(abspath .local/istio-artifacts)
SOURCE_REVISION ?= $(shell git rev-parse HEAD)
VENDOR_PACKAGES = -p gix-pack -p gix-zlib -p gix-blame -p gix-diff -p gix-imara-diff -p gix-traverse -p gix-revwalk

core-source-check:
	python3 tools/check-core-source.py
	python3 -B tests/core_source_test.py

check: core-source-check
	python3 -B tests/integration_process_test.py
	python3 -B tests/failure_gateways_test.py
	python3 -B tests/blame_gate_test.py
	python3 -B tests/blame_images_test.py
	python3 -B tests/runtime_boundaries_test.py
	python3 -B tests/runtime_process_test.py
	python3 -B tests/maintenance_fixture_test.py
	python3 -B tests/runtime_gate_test.py
	python3 -B tests/runtime_exec_test.py
	python3 -B tests/kubernetes_processes_test.py
	python3 -B tests/istio_artifacts_test.py
	python3 -B tests/playground_test.py
	python3 -B tests/cache_matrix_test.py
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings
	cargo test --locked --workspace --all-targets

# Test the exact external checkout, never a copied application source subtree.
vendor-check: core-source-check
	python3 tools/check-core-source.py --dependency-root "$(GITOXIDE_DIR)" --codec-root "$(CODEC_DIR)" --kubert-root "$(KUBERT_DIR)"
	cargo test --locked --manifest-path "$(GITOXIDE_DIR)/Cargo.toml" --target-dir "$(VENDOR_TARGET_DIR)" $(VENDOR_PACKAGES)
	cargo clippy --locked --manifest-path "$(GITOXIDE_DIR)/Cargo.toml" --target-dir "$(VENDOR_TARGET_DIR)" $(VENDOR_PACKAGES) --lib --tests -- -D warnings
	cargo test --locked --manifest-path "$(GITOXIDE_DIR)/Cargo.toml" --target-dir "$(VENDOR_TARGET_DIR)" $(VENDOR_PACKAGES) --doc
	cargo test --locked --manifest-path "$(CODEC_DIR)/Cargo.toml" --target-dir "$(VENDOR_TARGET_DIR)/iconv-native" --lib --tests
	cargo clippy --locked --manifest-path "$(CODEC_DIR)/Cargo.toml" --target-dir "$(VENDOR_TARGET_DIR)/iconv-native" --lib --tests -- -D warnings
	# The upstream library workspace has no committed lock; the application's graph remains locked above.
	cargo check --manifest-path "$(KUBERT_DIR)/Cargo.toml" --target-dir "$(VENDOR_TARGET_DIR)/kubert" -p kubert --no-default-features --features lease,openssl-tls,k8s-openapi/v1_33 --lib

cache-build:
	cargo build --locked --bin git-rs --example cache-process
	cargo test --locked --test cache_contract --test cache_coordinator --test cache_budget --no-run

# Prebuild outside the bounded measurement unit with make cache-build.
cache-check:
	python3 -B tests/cache_gate.py run --target-dir "$${CARGO_TARGET_DIR:-target}"

# Preparation is outside measurement and requires the exact .local/cache-v1 checkout.
blame-gate-prepare:
	python3 -B tests/blame_gate.py prepare --bundle "$(BLAME_GATE_BUNDLE)" --builder-image "$(BLAME_GATE_BUILDER_IMAGE)" --image "$(BLAME_GATE_IMAGE)"

# Split CI supplies verified images, bounded units/slices and a sealed manifest hash.
blame-gate-check:
	python3 -B tests/blame_gate.py run --bundle "$(BLAME_GATE_BUNDLE)" --unit "$$BLAME_GATE_UNIT" --slice "$$BLAME_GATE_SLICE" --manifest-sha256 "$$BLAME_GATE_MANIFEST_SHA256"

blame-gate-cleanup:
	python3 -B tests/blame_gate.py cleanup

blame-core-build:
	cargo build --locked --bin git-rs --example blame-http-fixture

# Use prebuilt binaries inside systemd with RuntimeMaxSec=900, bounded resources,
# KillMode=control-group and ExecStopPost=/usr/bin/make -C <checkout> blame-core-cleanup.
# The fresh /tmp fixture keeps Unix socket paths short and retains synthetic logs.
blame-core-check:
	python3 -B -c 'import os,re; assert re.fullmatch(r"[0-9a-f]{32}", os.environ.get("INVOCATION_ID", "")), "run inside bounded systemd with ExecStopPost=make blame-core-cleanup"'
	LANG=C LC_ALL=C GIT_RS_TEST_OWNER_LABEL="git-rs.blame-core-owner=$$INVOCATION_ID" python3 -B tests/integration.py --provision --blame-core-only --binary "$${CARGO_TARGET_DIR:-target}/debug/git-rs" --blame-fixture-binary "$${CARGO_TARGET_DIR:-target}/debug/examples/blame-http-fixture" --fixture-dir "$${BLAME_FIXTURE_DIR:-/tmp/git-rs-blame-$$INVOCATION_ID}"

blame-core-cleanup:
	python3 -B -c 'import os,re,subprocess; invocation=os.environ.get("INVOCATION_ID", ""); assert re.fullmatch(r"[0-9a-f]{32}", invocation), "cleanup requires systemd INVOCATION_ID"; query=["docker","ps","-aq","--filter","label=git-rs.blame-core-owner="+invocation]; ids=subprocess.run(query,check=True,capture_output=True,text=True,timeout=5).stdout.split(); subprocess.run(["docker","rm","-f",*ids],check=True,timeout=20) if ids else None; assert not subprocess.run(query,check=True,capture_output=True,timeout=5).stdout.strip(), "owned blame fixtures remain"'

# All contracts consume prebuilt inputs in independently bounded systemd units.
runtime-contracts-build:
	cargo test --locked --test metadata --test maintenance --test maintenance_leader --test leader_dependency --no-run
	python3 -B tests/runtime_gate.py export-contracts --target-dir "$${CARGO_TARGET_DIR:-target}" --destination "$${RUNTIME_CONTRACT_DIR:-.local/runtime-contracts}"

# Run as root: the observer retains BPF/cgroup privileges and drops work to 10001.
metadata integration receive upload history failures maintenance cache-matrix leader-dependency-check kubernetes istio:
	python3 -B tests/runtime_exec.py run --phase "$(if $(filter leader-dependency-check,$@),leader,$@)" --unit "$$RUNTIME_GATE_UNIT" --slice "$$RUNTIME_GATE_SLICE" --revision "$(SOURCE_REVISION)" --runtime-image "$(RUNTIME_IMAGE)" --fixture-image "$(RUNTIME_FIXTURE_IMAGE)" --oracle-image "$(ORACLE_IMAGE)" --target-dir "$${RUNTIME_CONTRACT_DIR:-.local/runtime-contracts}" --helm "$(HELM)" --istio-artifacts "$(ISTIO_ARTIFACTS)"

chart:
	python3 tests/chart.py --helm $(HELM)

# Public chart/CRD downloads are preparation, outside the measured Istio unit.
istio-prepare:
	python3 -B tests/istio.py --prepare --artifacts "$(ISTIO_ARTIFACTS)"

image:
	docker build --build-arg "VCS_REF=$(SOURCE_REVISION)" --target release -t git-rs:dev .

leader-dependency-build: runtime-contracts-build

vm-package:
	python3 -B tools/vm-release.py --image "$(RUNTIME_IMAGE)" --directory "$(VM_PACKAGE_DIR)"

bench:
	cargo bench --bench cache
