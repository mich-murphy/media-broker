FROM rust:1.98-slim-trixie@sha256:f47a8de237dcbb0b0ce1099901e60a89728e3d51f24e664b40e947171538ade7 AS build

WORKDIR /app
# Compile the locked dependencies against placeholder targets so this layer is
# reused until Cargo.toml or Cargo.lock changes.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
    && touch src/lib.rs \
    && echo 'fn main() {}' > src/main.rs \
    && cargo build --release --locked
# COPY keeps the context's mtimes; touch makes cargo rebuild the real crate.
COPY src ./src
RUN touch src/lib.rs src/main.rs \
    && cargo build --release --locked

# The runtime image carries only the binary: no toolchain, cargo, or source tree.
FROM gcr.io/distroless/cc-debian13:nonroot@sha256:54df941ed0d06a1bd95ef5e0ce391fd8d9f94b64782dc9a60062727849ee3f97

COPY --from=build /app/target/release/media-broker /usr/local/bin/media-broker
USER 65532:65532
EXPOSE 8000
ENV MEDIA_BROKER_BIND_HOST=127.0.0.1
CMD ["/usr/local/bin/media-broker"]
