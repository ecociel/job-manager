ARG PACKAGE=job

FROM rust:1.82.0-slim-bookworm AS chef

RUN apt-get update && apt-get install -y \
    libssl-dev \
    pkg-config \
    g++ \
    libuv1-dev \
    git \
    build-essential \
    cmake \
    curl \
    wget \
    protobuf-compiler \
    zlib1g-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

ARG CHEF_TAG=0.1.63
RUN cargo install cargo-chef --locked --version $CHEF_TAG
WORKDIR /build

RUN git clone https://github.com/datastax/cpp-driver.git \
    && cd cpp-driver \
    && mkdir build && cd build \
    && cmake .. \
    && make \
    && make install \
    && ldconfig

FROM chef AS planner
ARG PACKAGE
COPY . .
RUN cargo chef prepare --recipe-path recipe.json --bin $PACKAGE

FROM chef AS builder
ARG PACKAGE
COPY --from=planner /build/recipe.json recipe.json
RUN cargo chef cook --recipe-path recipe.json --bin $PACKAGE
COPY . .
RUN RUSTFLAGS="-L /usr/local/lib" cargo build --bin $PACKAGE


FROM ubuntu:22.04 AS runtime
ARG PACKAGE

RUN apt-get update && apt-get install -y \
    libssl3 \
    libuv1 \
    libzstd1 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=builder /build/target/debug/$PACKAGE /app

COPY --from=chef /usr/local/lib /usr/local/lib/

RUN ldconfig

CMD ["/app"]