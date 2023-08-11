FROM ubuntu

# apt
RUN apt update -y; \
    apt install -y curl build-essential pkg-config libssl-dev

# rust
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs -o /tmp/rustup.sh
RUN bash /tmp/rustup.sh -y
RUN bash -c 'PATH="$HOME/.cargo/bin:$PATH" rustup toolchain install stable'
RUN bash -c 'PATH="$HOME/.cargo/bin:$PATH" rustup default stable'

# analyzer
ARG ANALYZER_BUILD_PATH=/prism/
ADD . $ANALYZER_BUILD_PATH
WORKDIR $ANALYZER_BUILD_PATH
RUN PATH=$PATH:${CARGO_HOME}/bin make build
