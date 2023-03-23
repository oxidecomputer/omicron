#!/bin/bash
#
# This script fetches the following from CI
#
#   - the softnpu ASIC simulator (softnpu)
#   - a softnpu admin program (scadm)
#   - the sidecar-lite precompiled P4 program
#

# This is the softnpu ASIC emulator
if [[ ! -f out/softnpu/softnpu ]]; then
    echo "fetching softnpu"
    curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GTD3CPEENJZ9K1VA0J3GYD5D/WZ2Rw4MGOeSr06SEfmyuMfsp7i5rgwzvENWnAUjShI8FGryp/01GTD3D91TCD903Z0BAYSA31JR/01GTD3T8VFSZTE59Y0SVAR4CWC/softnpu
    chmod +x softnpu
    mkdir -p out/softnpu
    mv softnpu out/softnpu/
fi

# This is an ASIC administration program.
if [[ ! -f out/softnpu/scadm ]]; then
    echo "fetching scadm"
    curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GTD3Y38K0Q14F2989R20QCBA/Ny1N0clhEwz2nfbsdTY8ta7pd9IYy1ofxG6ViCDN6Uy4xW3F/01GTD3YZ0TC9SCB4TFNYWP0DEM/01GTD4CCW2F8Q0NQGD7WEY91D0/scadm
    chmod +x scadm
    mv scadm out/softnpu/
fi

# Fetch the pre-compiled sidecar_lite p4 program
if [[ ! -f out/softnpu/libsidecar_lite.so ]]; then
    echo "fetching libsidecar_lite.so"
    curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GTD3Y38K0Q14F2989R20QCBA/Ny1N0clhEwz2nfbsdTY8ta7pd9IYy1ofxG6ViCDN6Uy4xW3F/01GTD3YZ0TC9SCB4TFNYWP0DEM/01GTD4CA8KRGR8HK4S6B0DEEJC/libsidecar_lite.so
    mv libsidecar_lite.so out/softnpu/
fi

# This is the CLI client for dendrite
if [[ ! -f out/softnpu/swadm ]]; then
    echo "fetching swadm"
    curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GVRJ8RKWX9R26DX39KMGYZ3Z/IXahhCNnTV5VY8QPRWC9acX9ZNaDFLnY7TyTOZ0ch3rnHFqs/01GVRJ9QZ278DAD31FVSD5NH2A/01GVRKVC0F833P1XR0W9W8X6N7/swadm
    chmod +x swadm
    mv swadm out/softnpu/
fi
