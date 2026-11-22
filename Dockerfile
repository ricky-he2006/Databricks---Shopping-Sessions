# Shopping Sessions: one Rust binary with no crate dependencies.
# The scorers build this natively on linux/amd64; on an Apple-silicon Mac the
# builder stage cross-compiles, so `docker buildx build --platform linux/amd64`
# stays fast there too.
FROM --platform=$BUILDPLATFORM rust:1.90-slim-bookworm AS builder
ARG BUILDARCH
ARG TARGETARCH
WORKDIR /src
RUN case "$TARGETARCH" in \
      amd64) target=x86_64-unknown-linux-gnu; linker=x86_64-linux-gnu-gcc; cross=gcc-x86-64-linux-gnu ;; \
      arm64) target=aarch64-unknown-linux-gnu; linker=aarch64-linux-gnu-gcc; cross=gcc-aarch64-linux-gnu ;; \
      *) exit 1 ;; \
    esac && \
    if [ "$BUILDARCH" != "$TARGETARCH" ]; then \
        apt-get update && apt-get install -y --no-install-recommends "$cross" "libc6-dev-$TARGETARCH-cross"; \
    fi && \
    rustup target add "$target" && printf '%s\n%s\n' "$target" "$linker" > /target-info
COPY native/ ./
# Build the ranked v3/v4 sources first. Only the final wide build applies the
# changes measured on byte-compression-capable AMD hosts.
RUN target="$(head -n 1 /target-info)" && linker="$(tail -n 1 /target-info)" && \
    if [ "$BUILDARCH" = "$TARGETARCH" ]; then linker=cc; fi && \
    mkdir -p /app && \
    if [ "$TARGETARCH" = amd64 ]; then \
        apt-get update && apt-get install -y --no-install-recommends patch "$cross" "libc6-dev-$TARGETARCH-cross" && \
        for level in base v3 v4 wide; do \
            cpu=x86-64-$level; profile=v5; features=; \
            case "$level" in base) cpu=x86-64 ;; v4) profile=v4 ;; wide) patch -p2 < platform/wide.patch || exit 1; cpu=x86-64-v4; features="-C target-feature=+avx512vbmi2"; profile=wide ;; esac; \
            RUSTFLAGS="-C linker=$linker -C target-cpu=$cpu $features -Cprofile-use=/src/profile/$profile.profdata" \
              cargo build --locked --release --target "$target" || exit 1; \
            cp "target/$target/release/shopping-sessions" "/app/shopping-sessions.$level" || exit 1; \
        done && "$linker" -O2 platform/select-engine.c -o /app/shopping-sessions; \
    else \
        RUSTFLAGS="-C linker=$linker -C target-cpu=generic" cargo build --locked --release --target "$target" && \
        cp "target/$target/release/shopping-sessions" /app/shopping-sessions; \
    fi

FROM debian:bookworm-slim
COPY --from=builder /app /app
ENTRYPOINT ["/app/shopping-sessions"]
