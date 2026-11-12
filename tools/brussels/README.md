# Hubris artifacts downloaded with Brussels

This directory contains the Sigstore attestations of the Hubris releases
produced by [Brussels], our (private) Hubris release tooling. The `releng` tool
will use the attestations to download the releases from GitHub.

## Extracting the version number from an attestation

Unfortunately we don't control the format of the attestations, and there is no
cleartext place inside of a file that specifies the version number of the
release the file points to. You can use this command to extract it:

```bash
jq -r '.dsseEnvelope.payload | @base64d | fromjson | .predicate.tag' NAME.sigstore.json
```

[Brussels]: https://github.com/oxidecomputer/brussels
