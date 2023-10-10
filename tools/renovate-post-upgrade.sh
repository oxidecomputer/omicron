#!/bin/bash

# This script is run after Renovate upgrades dependencies or lock files.

set -euo pipefail

# Download and install cargo-hakari if it is not already installed.
if ! command -v cargo-hakari &> /dev/null; then
    # Need cargo-binstall to install cargo-hakari.
    if ! command -v cargo-binstall &> /dev/null; then
        # Fetch cargo binstall.
        echo "Installing cargo-binstall..."
        curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash
    fi

    # Install cargo-hakari.
    echo "Installing cargo-hakari..."
    cargo binstall cargo-hakari --no-confirm
fi

# Run cargo hakari to regenerate the workspace-hack file.
echo "Running cargo-hakari..."
cargo hakari generate
