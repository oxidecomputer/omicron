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
RUN cargo build --release

# ------------------------------------------------------------------------------
# Final Stage
# ------------------------------------------------------------------------------

FROM debian:sid-slim

RUN apt-get update && apt-get install -y \
	ca-certificates \
	libpq5 \
	libssl1.1 \
	--no-install-recommends \
	&& rm -rf /var/lib/apt/lists/*


COPY --from=cargo-build /usr/src/omicron/target/release/bootstrap_agent /usr/bin/bootstrap_agent
COPY --from=cargo-build /usr/src/omicron/target/release/nexus /usr/bin/nexus
COPY --from=cargo-build /usr/src/omicron/target/release/omicron_dev /usr/bin/omicron_dev
COPY --from=cargo-build /usr/src/omicron/target/release/omicron_package /usr/bin/omicron_package
COPY --from=cargo-build /usr/src/omicron/target/release/sled_agent /usr/bin/sled_agent

CMD ["sled_agent"]
