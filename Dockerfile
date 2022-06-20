# 
# Dockerfile: build a Docker image for Omicron.  This is used by the console for
# prototyping and development.  This will not be used for deployment to a real 
# rack.
# 
# ------------------------------------------------------------------------------
# Cargo Build Stage
# ------------------------------------------------------------------------------

FROM rust:latest as cargo-build

ENV DEBIAN_FRONTEND=noninteractive

WORKDIR /usr/src/omicron

COPY . .

WORKDIR /usr/src/omicron

# sudo and path thing are only needed to get prereqs script to run
ENV PATH=/usr/src/omicron/out/cockroachdb/bin:/usr/src/omicron/out/clickhouse:${PATH} 
RUN apt-get update && apt-get install -y sudo --no-install-recommends && rm -rf /var/lib/apt/lists/*
RUN tools/install_prerequisites.sh -y

RUN cargo build --release

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------

FROM debian:sid-slim

RUN apt-get update && apt-get install -y \
	ca-certificates \
	libpq5 \
	libssl1.1 \
	libsqlite3-0 \
	--no-install-recommends \
	&& rm -rf /var/lib/apt/lists/*


COPY --from=cargo-build /usr/src/omicron/target/release/nexus /usr/bin/nexus
COPY --from=cargo-build /usr/src/omicron/target/release/omicron-dev /usr/bin/omicron-dev
COPY --from=cargo-build /usr/src/omicron/target/release/omicron-package /usr/bin/omicron-package
COPY --from=cargo-build /usr/src/omicron/target/release/sled-agent-sim /usr/bin/sled-agent-sim

CMD ["sled-agent-sim"]
