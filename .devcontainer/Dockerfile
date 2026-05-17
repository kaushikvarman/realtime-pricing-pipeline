# Base: Microsoft devcontainer image with JDK 17 + Maven + Gradle preinstalled.
# We layer Python 3.11 on top (Debian Bookworm ships 3.11 in apt).
FROM mcr.microsoft.com/devcontainers/java:1-17-bookworm

# Python toolchain — kept minimal; pip will pull the rest from requirements-dev.txt
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        python3-venv \
        python-is-python3 \
 && rm -rf /var/lib/apt/lists/*

# Sanity check during build — fails the image if a tool is missing
RUN java -version && mvn -version && python3 --version
