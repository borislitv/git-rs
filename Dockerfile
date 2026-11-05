FROM ubuntu:26.04@sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c AS build
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends cargo rustc \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY migrations ./migrations
COPY benches ./benches
RUN cargo build --release --offline --locked

FROM ubuntu:26.04@sha256:6a06111c3b0a8600feadb270aec771dc239bf407b25625f93f44ab00e11f4d3c AS runtime
ARG GIT_DEB_VERSION=1:2.53.0-1ubuntu1.1
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    git=${GIT_DEB_VERSION} git-man=${GIT_DEB_VERSION} curl postgresql-client ca-certificates coreutils openssh-server tini \
    && git --version && curl --version && psql --version \
    && test "$(git --version)" = "git version 2.53.0" \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /etc/ssh/ssh_host_* \
    && mkdir -p /run/sshd /var/empty/git-rs \
    && groupadd -g 10001 git-rs && useradd -u 10001 -g 10001 -M -d /var/empty/git-rs -s /bin/sh -p '*' git-rs
COPY --from=build /build/target/release/git-rs /usr/local/bin/git-rs
USER 10001:10001
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/git-rs"]

FROM runtime AS test
USER 0:0
RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    python3 python3-yaml python3-jsonschema \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/git-rs/.local && chown 10001:10001 /opt/git-rs/.local && chmod 0700 /opt/git-rs/.local
COPY tests /opt/git-rs/tests
WORKDIR /opt/git-rs
USER 10001:10001

FROM runtime AS release
