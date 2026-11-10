FROM ubuntu:26.04@sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c AS toolchain
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       build-essential ca-certificates curl xz-utils \
    && rm -rf /var/lib/apt/lists/*
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
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*

FROM rust-toolchain AS build
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
COPY benches ./benches
COPY tests/support/cache_process.rs ./tests/support/cache_process.rs
COPY crates/git-core ./crates/git-core
# Cargo fetch resolves the owned Gitoxide source at the immutable lockfile pin.
RUN cargo fetch --locked && cargo build --release --offline --locked

FROM toolchain AS git-build
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    libssl-dev libcurl4-openssl-dev libexpat1-dev zlib1g-dev gettext \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /tmp/git
RUN curl --fail --show-error --location --max-time 180 --retry 3 \
      https://www.kernel.org/pub/software/scm/git/git-2.56.0.tar.xz -o git.tar.xz \
    && printf '%s  git.tar.xz\n' 26c56c296b38c0695b26fa95f475f1d01704d2d38e73465ca30b0b2f5dc789d3 | sha256sum -c - \
    && tar -xJf git.tar.xz --strip-components=1 \
    && make -j2 prefix=/usr gitexecdir=/usr/lib/git-core NO_TCLTK=YesPlease all \
    && make prefix=/usr gitexecdir=/usr/lib/git-core NO_TCLTK=YesPlease DESTDIR=/opt/git install \
    && /opt/git/usr/bin/git --version

FROM ubuntu:26.04@sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c AS runtime
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get upgrade -y \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    curl ca-certificates coreutils gnu-coreutils openssh-server tini libcurl4t64 libexpat1 libssl3t64 zlib1g perl \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /etc/ssh/ssh_host_* \
    && mkdir -p /run/sshd /var/empty/git-rs \
    && groupadd -g 10001 git-rs && useradd -u 10001 -g 10001 -M -d /var/empty/git-rs -s /bin/sh -p '*' git-rs
COPY --from=git-build /opt/git/usr/ /usr/
RUN test "$(/usr/bin/git --version)" = "git version 2.56.0" \
    && test "$(/usr/bin/git --exec-path)" = /usr/lib/git-core \
    && test -x /usr/lib/git-core/git-http-backend \
    && mkdir -p /usr/share/git-rs \
    && dpkg-query -W -f='${Package}\t${Version}\n' > /usr/share/git-rs/packages.tsv \
    && printf '%s\n' 'git 2.56.0' \
       'source https://www.kernel.org/pub/software/scm/git/git-2.56.0.tar.xz' \
       'sha256 26c56c296b38c0695b26fa95f475f1d01704d2d38e73465ca30b0b2f5dc789d3' \
       > /usr/share/git-rs/git-source.txt
COPY --from=build /build/target/release/git-rs /usr/local/bin/git-rs
USER 10001:10001
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/git-rs"]

FROM runtime AS test
USER 0:0
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python3 python3-venv postgresql-client \
    && rm -rf /var/lib/apt/lists/* \
    && python3 -m venv /opt/test-python \
    && /opt/test-python/bin/pip install --no-cache-dir PyYAML==6.0.3 jsonschema==4.26.0 \
    && mkdir -p /opt/git-rs/.local && chown 10001:10001 /opt/git-rs/.local && chmod 0700 /opt/git-rs/.local
COPY tests /opt/git-rs/tests
ENV PATH="/opt/test-python/bin:${PATH}"
WORKDIR /opt/git-rs
USER 10001:10001

FROM runtime AS release
