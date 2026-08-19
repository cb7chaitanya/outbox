FROM rust:1.94-bookworm AS builder
WORKDIR /src
COPY . .
ARG PACKAGE
RUN cargo build --release -p "$PACKAGE" && cp "target/release/$PACKAGE" /tmp/service

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /tmp/service /usr/local/bin/service
ENTRYPOINT ["/usr/local/bin/service"]
