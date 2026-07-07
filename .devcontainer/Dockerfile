FROM docker.io/ubuntu:24.04

ARG USERNAME=ubuntu
ARG RUST_VERSION=stable
ARG SQLX_CLI_VERSION=0.8.3

ENV DEBIAN_FRONTEND=noninteractive
ENV RUSTUP_HOME=/usr/local/rustup
ENV CARGO_HOME=/usr/local/cargo
ENV PATH=/usr/local/cargo/bin:${PATH}

RUN apt-get update \
	&& apt-get install -y --no-install-recommends \
		ca-certificates \
		curl \
		gnupg \
		lsb-release \
		sudo \
		tzdata \
	&& install -d /usr/share/postgresql-common/pgdg \
	&& curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
		| gpg --dearmor -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg \
	&& echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.gpg] https://apt.postgresql.org/pub/repos/apt noble-pgdg main" \
		> /etc/apt/sources.list.d/pgdg.list \
	&& apt-get update \
	&& apt-get install -y --no-install-recommends \
		build-essential \
		clang \
		cmake \
		git \
		lld \
		lldb \
		llvm \
		libclang-dev \
		libpq-dev \
		libssl-dev \
		pkg-config \
		postgresql-client-17 \
		protobuf-compiler \
		zlib1g-dev \
	&& rm -rf /var/lib/apt/lists/*

RUN echo "${USERNAME} ALL=(root) NOPASSWD:ALL" > "/etc/sudoers.d/${USERNAME}" \
	&& chmod 0440 "/etc/sudoers.d/${USERNAME}"

RUN curl --proto '=https' --tlsv1.2 -fsSL https://sh.rustup.rs \
	| sh -s -- -y --no-modify-path --profile minimal --default-toolchain "${RUST_VERSION}" \
	&& rustup component add rustfmt clippy rust-src \
	&& cargo install sqlx-cli --version "${SQLX_CLI_VERSION}" --no-default-features --features rustls,postgres \
	&& chmod -R a+w "${RUSTUP_HOME}" "${CARGO_HOME}"

USER ${USERNAME}
WORKDIR /workspace
