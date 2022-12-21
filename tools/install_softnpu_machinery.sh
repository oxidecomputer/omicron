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
    curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GTCVF6XEWX9QPCAS1NJDG66A/umvT8rwyIctVrqZjqLuq80vMsPVXwuRTgvEYE4Qlri32WpDC/01GTCVG86PRSHCAKJM07RS8D8K/01GTCVX29XP4THAK13DE9VABMP/softnpu
    chmod +x softnpu
    mkdir -p out/softnpu
    mv softnpu out/softnpu/
fi

# This is an ASIC administration program.
if [[ ! -f out/softnpu/scadm ]]; then
    echo "fetching scadm"
    curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GTCVK75XHVVFEEA095F74VZQ/BUIVBgmdTOsvGd8Qpewr6pkjFq88v8Xzxt09kabWOJMI6Hlm/01GTCVKZHBEQZ5JKA70KFFJSS5/01GTCW13WX8M3BM07PYHYSZ2K5/scadm
    chmod +x scadm
    mv scadm out/softnpu/
fi

# Fetch the pre-compiled sidecar_lite p4 program
if [[ ! -f out/softnpu/libsidecar_lite.so ]]; then
    echo "fetching libsidecar_lite.so"
    curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GTCVK75XHVVFEEA095F74VZQ/BUIVBgmdTOsvGd8Qpewr6pkjFq88v8Xzxt09kabWOJMI6Hlm/01GTCVKZHBEQZ5JKA70KFFJSS5/01GTCW11MBZEJDVDXC4RDRPSSM/libsidecar_lite.so
    mv libsidecar_lite.so out/softnpu/
fi

# This is the CLI client for dendrite
if [[ ! -f out/softnpu/swadm ]]; then
    echo "fetching swadm"
    curl -OL https://buildomat.eng.oxide.computer/wg/0/artefact/01GT8QDBN11SWM0CPBWV4J100D/fK83u57UKT3UqadYXs7DyAjoKIDF5OdmACmkswwfCW6hLolf/01GT8QDN7MC6KC93J974W7QFK3/01GT8RVFCKGMA9N32XVP5ZXKG2/swadm
    chmod +x swadm
    mv swadm out/softnpu/
fi
