# Stage 1: Build the application
FROM rust:latest AS builder
WORKDIR /usr/src/mdb

# Copy the Cargo manifest and lock file
COPY Cargo.toml Cargo.lock ./

# Create a dummy src/main.rs to cache dependencies
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release --quiet

# Copy the actual source code
COPY src ./src

# Build the application
RUN cargo build --release --quiet

# Stage 2: Create the final, smaller image
FROM debian:bookworm-slim
WORKDIR /usr/local/bin

# Copy the binary from the builder stage
COPY --from=builder /usr/src/mdb/target/release/mdb .

# Set the command to run the application
CMD ["./mdb"]
