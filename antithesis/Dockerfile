# syntax=docker/dockerfile:1
# TODO-RAINCLAUDE: images for running the simulated control plane under Antithesis; build via antithesis/config/docker-compose.yaml (context is the repo root).

# TODO-RAINCLAUDE: the toolchain here must match rust-toolchain.toml (rustup would otherwise download a second one), and the Debian release must match the runtime images below so the binaries link against the same libc and xmlsec generation they run on. Debian releases are pinned rather than `stable` because package names change across releases (trixie renamed libxmlsec1 to libxmlsec1t64).
FROM rust:1.97.1-trixie AS builder-base

# TODO-RAINCLAUDE: package list mirrors the Linux branch of tools/install_builder_prerequisites.sh.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libclang-dev \
        libpq-dev \
        libxmlsec1-dev \
        libxmlsec1-openssl \
        pkg-config \
        xmlsec1 \
    && rm -rf /var/lib/apt/lists/*

# TODO-RAINCLAUDE: build scripts materialize `.gitstub` files through git-stub-vcs, which needs real history at the repo root and rejects shallow clones. The build context carries no VCS (see .dockerignore), so the builder gets a treeless partial clone: commits only, with trees and blobs fetched on demand, which git-stub-vcs does not classify as shallow. It must not be bare because cargo walks the package's git repo to fingerprint build scripts and rejects bare repos; --no-checkout leaves the index empty so cargo sees every source file as untracked and lists it. Cloned before COPY so the layer survives source edits; the fetch after COPY picks up commits newer than the cached clone.
ARG OMICRON_GIT_URL=https://github.com/oxidecomputer/omicron
RUN git clone --no-checkout --filter=tree:0 "$OMICRON_GIT_URL" /omicron-history

WORKDIR /omicron
COPY . .
RUN git -C /omicron-history fetch --filter=tree:0 origin \
    && printf 'gitdir: /omicron-history/.git\n' > /omicron/.git


# TODO-RAINCLAUDE: no cargo here on purpose; compose builds images in parallel and two cargo processes racing on the shared registry cache mount corrupt it. The pin and checksum are the same ones cargo xtask download uses (tools/cockroachdb_checksums), and the URL mirrors dev-tools/downloader/src/lib.rs.
FROM debian:trixie-slim AS cockroach-download

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY tools/cockroachdb_checksums /cockroachdb_checksums

RUN set -eu \
    && . /cockroachdb_checksums \
    && curl --fail --silent --show-error --location \
        --output /cockroach.tgz \
        "https://buildomat.eng.oxide.computer/public/file/oxidecomputer/cockroach/linux-amd64/${COCKROACH_COMMIT}/cockroach.tgz" \
    && echo "${CIDL_SHA256_LINUX}  /cockroach.tgz" | sha256sum --check --strict \
    && mkdir -p /out /unpack \
    && tar -xzf /cockroach.tgz -C /unpack \
    && cp /unpack/cockroach/cockroach /out/cockroach \
    && /out/cockroach version


FROM builder-base AS builder

# TODO-RAINCLAUDE: the sancov rustflags in antithesis/cargo-config.toml plus the `antithesis` features are what make these binaries Antithesis-instrumented; sharing=locked keeps any future concurrent cargo stage from racing on these caches.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/omicron/target,sharing=locked \
    mkdir -p /out \
    && cargo build \
        --profile antithesis \
        --config antithesis/cargo-config.toml \
        --features omicron-nexus/antithesis,omicron-antithesis-workload/antithesis,dns-server/antithesis,omicron-sled-agent/antithesis,omicron-omdb/antithesis \
        --bin nexus \
        --bin sled-agent-sim \
        --bin dns-server \
        --bin schema-updater \
        --bin omdb \
        --bin omicron-antithesis-workload \
    && for bin in nexus sled-agent-sim dns-server schema-updater omdb omicron-antithesis-workload; do \
        cp "target/x86_64-unknown-linux-gnu/antithesis/$bin" "/out/$bin"; \
    done


FROM debian:trixie-slim AS omicron-antithesis

# TODO-RAINCLAUDE: libpq5 is the runtime half of the pq-sys dependency and libxmlsec1t64-openssl (with the libxmlsec1t64 and libxml2 it pulls in) is the runtime half of samael's SAML support; curl is for compose healthchecks. Verify with `ldd /opt/oxide/bin/* | grep "not found"` after changing the builder image.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libpq5 \
        libxmlsec1t64-openssl \
    && rm -rf /var/lib/apt/lists/*

ENV NO_COLOR=1
ENV PATH=/opt/oxide/bin:$PATH

COPY --from=builder /out/nexus /out/sled-agent-sim /out/dns-server /out/schema-updater /out/omdb /out/omicron-antithesis-workload /opt/oxide/bin/
COPY schema/crdb /opt/oxide/sc  hema/crdb
# TODO-RAINCLAUDE: service configs are baked in rather than bind-mounted from antithesis/config so the image runs identically under Antithesis and under rootless podman on SELinux hosts, where host file mounts are unreadable without relabeling.
COPY antithesis/config/nexus.toml antithesis/config/dns-server.toml /opt/oxide/config/

# TODO-RAINCLAUDE: Antithesis reads DWARF from /symbols; the binaries are unstripped so symlinks are enough.
RUN mkdir -p /symbols /var/oxide/dns-storage /var/tmp/omicron_tmp \
    && for bin in nexus sled-agent-sim dns-server schema-updater omdb omicron-antithesis-workload; do \
        ln -s "/opt/oxide/bin/$bin" "/symbols/$bin"; \
    done


# TODO-RAINCLAUDE: the test commands live only in the workload image; Antithesis discovers /opt/antithesis/test/v1 in every container and would otherwise run each command once per service built from the shared image.
FROM omicron-antithesis AS omicron-antithesis-workload

COPY antithesis/test/v1 /opt/antithesis/test/v1


FROM debian:trixie-slim AS omicron-antithesis-cockroach

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

ENV NO_COLOR=1
ENV PATH=/opt/oxide/bin:$PATH

COPY --from=cockroach-download /out/cockroach /opt/oxide/bin/cockroach
COPY schema/crdb/dbinit.sql /opt/oxide/schema/crdb/dbinit.sql
COPY antithesis/cockroach-seed-antithesis.sql /opt/oxide/schema/cockroach-seed-antithesis.sql
COPY antithesis/cockroach-healthcheck.sh antithesis/cockroach-seed.sh /opt/oxide/bin/

# TODO-RAINCLAUDE: seeding at build time keeps the runtime hermetic and fast; the store already holds the omicron schema at the version Nexus expects.
RUN /opt/oxide/bin/cockroach-seed.sh /data

VOLUME /data
