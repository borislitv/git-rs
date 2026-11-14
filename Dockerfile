FROM ubuntu:26.04@sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c AS codec-base
# One OS package transaction supplies the ABI used by compilation and serving.
# Runtime pruning below does not upgrade or copy libc/gconv files piecemeal.
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       build-essential ca-certificates curl xz-utils libssl-dev pkg-config \
       libcurl4-openssl-dev libexpat1-dev zlib1g-dev gettext \
       coreutils gnu-coreutils openssh-server tini libcurl4t64 libexpat1 libssl3t64 zlib1g perl \
       python3 python3-venv postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
     && rm -f /etc/ssh/ssh_host_* \
     && mkdir -p /usr/share/git-rs \
     && dpkg-query -W -f='${Package}\t${Version}\n' libc6 libc-bin libssl3t64 \
        > /usr/share/git-rs/abi-packages.tsv

FROM codec-base AS runtime-base
RUN apt-mark showmanual > /tmp/manual-packages \
    && xargs -r apt-mark auto < /tmp/manual-packages \
    && apt-mark manual ca-certificates coreutils gnu-coreutils libcurl4t64 libexpat1 \
        libssl3t64 zlib1g openssh-server tini libgcc-s1 passwd \
    && DEBIAN_FRONTEND=noninteractive apt-get --no-download autoremove --purge -y \
    && rm /tmp/manual-packages \
    && dpkg-query -W -f='${Package}\t${Version}\n' libc6 libc-bin libssl3t64 > /tmp/abi-packages.tsv \
    && cmp /usr/share/git-rs/abi-packages.tsv /tmp/abi-packages.tsv \
    && rm /tmp/abi-packages.tsv \
    && mkdir -p /run/sshd /var/empty/git-rs \
    && groupadd -g 10001 git-rs \
    && useradd -u 10001 -g 10001 -M -d /var/empty/git-rs -s /bin/sh -p '*' git-rs \
    && dpkg-query -W -f='${Package}\t${Version}\n' > /usr/share/git-rs/packages.tsv

FROM codec-base AS toolchain
WORKDIR /tmp/rust
# Official component archives avoid installing rustup or unused LLVM tools.
RUN set -eu; \
    for component in \
      rustc:2ff05ff9c08a90861e7f6e4065b40a8b87463b4576186fd613871c3e37f2091f \
      cargo:53f3521949404d595efb06a55c12c5658d4ed1c75566f33d3d514eaebe70bd8a \
      rust-std:109b074225bf5b9ea3820890876bfdf4ed161adbfaf94b2b4b5984c85ee5d958 \
      rustfmt:e3b36c4c7ec3486c059d8067ff48787676612007123aad7a4e8ec12c48ee9ba2 \
      clippy:458339712a058e52cc4522ff9ef8b59cd1ea1340312216cd94194c464a3f6dc5; do \
      name=${component%%:*}-1.99.1-x86_64-unknown-linux-gnu; \
      curl --fail --show-error --location --max-time 180 --retry 3 \
        "https://static.rust-lang.org/dist/$name.tar.xz" -o component.tar.xz; \
      printf '%s  component.tar.xz\n' "${component#*:}" | sha256sum -c -; \
      tar -xJf component.tar.xz; \
      "./$name/install.sh" --prefix=/usr/local --disable-ldconfig; \
      rm -rf "$name" component.tar.xz; \
    done; rustc --version; cargo --version

FROM toolchain AS rust-toolchain

FROM rust-toolchain AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock Dockerfile ./
COPY src ./src
COPY migrations ./migrations
COPY benches ./benches
COPY tests/support ./tests/support
COPY tests/receive_fixture.rs ./tests/receive_fixture.rs
COPY crates/git-core ./crates/git-core
COPY docs/*-source.json ./docs/
COPY tools/check-core-source.py tools/dependency-notices.py ./tools/
RUN python3 tools/check-core-source.py \
    && cargo fetch --locked \
    && cargo build --release --offline --locked \
    && cargo metadata --format-version 1 --offline --locked > /tmp/cargo-metadata.json \
    && python3 tools/dependency-notices.py --metadata /tmp/cargo-metadata.json --output /build/release-notices \
    && rm /tmp/cargo-metadata.json

FROM build AS test-fixture-build
RUN cargo build --release --offline --locked --example blame-http-fixture \
    && cargo test --release --offline --locked --test receive_fixture --no-run --message-format=json > /tmp/receive-artifacts.json \
    && python3 -c 'import json,pathlib,shutil; rows=[json.loads(line) for line in pathlib.Path("/tmp/receive-artifacts.json").read_text().splitlines()]; found=[row["executable"] for row in rows if row.get("reason")=="compiler-artifact" and row.get("target",{}).get("name")=="receive_fixture" and row.get("executable")]; assert len(found)==1; shutil.copyfile(found[0],"/build/target/release/receive-fixture")' \
    && chmod 0555 /build/target/release/receive-fixture \
    && rm /tmp/receive-artifacts.json

FROM toolchain AS git-build
WORKDIR /tmp/git
RUN curl --fail --show-error --location --max-time 180 --retry 3 \
      https://www.kernel.org/pub/software/scm/git/git-2.56.0.tar.xz -o git.tar.xz \
    && printf '%s  git.tar.xz\n' 26c56c296b38c0695b26fa95f475f1d01704d2d38e73465ca30b0b2f5dc789d3 | sha256sum -c - \
    && tar -xJf git.tar.xz --strip-components=1 \
    && make -j2 prefix=/usr gitexecdir=/usr/lib/git-core NO_TCLTK=YesPlease all \
    && make prefix=/usr gitexecdir=/usr/lib/git-core NO_TCLTK=YesPlease DESTDIR=/opt/git install \
    && /opt/git/usr/bin/git --version

FROM runtime-base AS runtime
ARG VCS_REF
COPY --from=build /build/target/release/git-rs /usr/local/bin/git-rs
COPY --from=build /build/release-notices/ /usr/share/git-rs/dependencies/
COPY docs/*-source.json /usr/share/git-rs/sources/
LABEL org.opencontainers.image.source="https://github.com/borislitv/git-rs" \
      org.opencontainers.image.revision="${VCS_REF}" \
      io.git-rs.role="runtime"
USER 10001:10001
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/git-rs"]

# External native clients/oracles have their own build branch and filesystem.
# No Git install, helper, package marker or source layer reaches `runtime`.
FROM codec-base AS client-oracle
ARG VCS_REF
COPY --from=git-build /opt/git/usr/ /usr/
RUN test "$(/usr/bin/git --version)" = "git version 2.56.0" \
    && test "$(/usr/bin/git --exec-path)" = /usr/lib/git-core \
    && test -x /usr/lib/git-core/git-http-backend \
    && mkdir -p /usr/share/git-rs \
    && dpkg-query -W -f='${Package}\t${Version}\n' > /usr/share/git-rs/packages.tsv \
    && printf '%s\n' 'git 2.56.0' \
       'source https://www.kernel.org/pub/software/scm/git/git-2.56.0.tar.xz' \
       'sha256 26c56c296b38c0695b26fa95f475f1d01704d2d38e73465ca30b0b2f5dc789d3' \
       > /usr/share/git-rs/git-source.txt \
    && groupadd -g 10001 git-rs \
    && useradd -u 10001 -g 10001 -M -d /var/empty/git-rs -s /bin/sh -p '*' git-rs
LABEL org.opencontainers.image.source="https://github.com/borislitv/git-rs" \
      org.opencontainers.image.revision="${VCS_REF}" \
      io.git-rs.role="external-client-oracle"

FROM runtime AS test-server
COPY --from=test-fixture-build /build/target/release/examples/blame-http-fixture /usr/local/bin/examples/blame-http-fixture
COPY --from=test-fixture-build /build/target/release/receive-fixture /usr/local/bin/tests/receive-fixture
LABEL io.git-rs.role="runtime-fixture"

FROM client-oracle AS test
COPY --from=build /build/target/release/git-rs /usr/local/bin/git-rs
COPY --from=test-fixture-build /build/target/release/examples/blame-http-fixture /usr/local/bin/examples/blame-http-fixture
RUN python3 -m venv /opt/test-python \
    && /opt/test-python/bin/pip install --no-cache-dir PyYAML==6.0.3 jsonschema==4.26.0 \
    && mkdir -p /opt/git-rs/.local && chown 10001:10001 /opt/git-rs/.local && chmod 0700 /opt/git-rs/.local
COPY tests /opt/git-rs/tests
COPY charts/git-rs /opt/git-rs/charts/git-rs
COPY tools/check-runtime-boundaries.py /opt/git-rs/tools/
ENV PATH="/opt/test-python/bin:${PATH}" LANG=C LC_ALL=C
WORKDIR /opt/git-rs
USER 10001:10001
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/git-rs"]

FROM runtime AS release
