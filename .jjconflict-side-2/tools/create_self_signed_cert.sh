#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.


# Creates a self-signed certificate.
#
# For those with access, certificates are available in:
#
# https://github.com/oxidecomputer/configs/tree/master/nginx/ssl/wildcard.oxide-preview.com

set -eu

# Set the CWD to Omicron's source.
SOURCE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "${SOURCE_DIR}/.."

OUTPUT_DIR="out/certs"
CERT_PATH="$OUTPUT_DIR/cert.pem"
KEY_PATH="$OUTPUT_DIR/key.pem"

mkdir -p "$OUTPUT_DIR"

openssl req -newkey rsa:4096 \
            -x509 \
            -sha256 \
            -days 3650 \
            -nodes \
            -out "$CERT_PATH" \
            -keyout "$KEY_PATH" \
            -subj '/CN=localhost'
