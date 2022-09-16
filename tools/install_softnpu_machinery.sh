#!/bin/bash
#
# This script fetches the following from CI
#
#   - the softnpu ASIC simulator (softnpu)
#   - a softnpu admin program (softnpuadm)
#   - the sidecar-lite precompiled P4 program
#

# This is the softnpu ASIC emulator
if [[ ! -f out/softnpu/softnpu ]]; then
    echo "fetching softnpu"
    curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GGBA9MYC3AYJZV01BC9XV23E/kYeYK1DsRJRMoDMUsxzXrN0RFLp2VNnOZjQLKoH9HG0oaXjd/01GGBA9YPJQF7RVKEA74XFB3SC/01GGBAMYWT29BHHJST3SWTV9P5/softnpu
    chmod +x softnpu
    mkdir -p out/softnpu
    mv softnpu out/softnpu/
fi

# This is an ASIC administration program.
if [[ ! -f out/softnpu/softnpuadm ]]; then
    echo "fetching softnpuadm"
    curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GGBA9MYC3AYJZV01BC9XV23E/kYeYK1DsRJRMoDMUsxzXrN0RFLp2VNnOZjQLKoH9HG0oaXjd/01GGBA9YPJQF7RVKEA74XFB3SC/01GGBAMZJRR1WW61W102GAGA8F/softnpuadm
    chmod +x softnpuadm
    mkdir -p out/softnpu
    mv softnpuadm out/softnpu/
fi

# Fetch the pre-compiled sidecar_lite p4 program
if [[ ! -f out/softnpu/libsidecar_lite.so ]]; then
    echo "fetching libsidecar_lite.so"
curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GFXXNWVDWX6RPR675D42BJGX/vPcfIUTP0JVAC7WXHML8tR2OO5RVsExuarh5pMjTRsKL2Ky9/01GFXXPBKRSTNKCXRVJ0XR00VB/01GFXY2N4PY7NW90PW9JYTHWYF/libsidecar_lite.so
    mv libsidecar_lite.so out/softnpu/
fi
