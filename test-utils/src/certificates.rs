// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Utilities for tests that need certificates.

use rustls_pki_types::CertificateDer;

// Utility structure for making a test certificate
pub struct CertificateChain {
    root_cert: CertificateDer<'static>,
    intermediate_cert: CertificateDer<'static>,
    end_cert: CertificateDer<'static>,
    end_keypair: rcgen::Certificate,
}

impl CertificateChain {
    pub fn new<S: Into<String>>(subject_alt_name: S) -> Self {
        let params =
            rcgen::CertificateParams::new(vec![subject_alt_name.into()]);
        Self::with_params(params)
    }

    pub fn with_params(params: rcgen::CertificateParams) -> Self {
        let mut root_params = rcgen::CertificateParams::new(vec![]);
        root_params.is_ca =
            rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let root_keypair = rcgen::Certificate::from_params(root_params)
            .expect("failed to generate root keys");

        let mut intermediate_params = rcgen::CertificateParams::new(vec![]);
        intermediate_params.is_ca =
            rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let intermediate_keypair =
            rcgen::Certificate::from_params(intermediate_params)
                .expect("failed to generate intermediate keys");

        let end_keypair = rcgen::Certificate::from_params(params)
            .expect("failed to generate end-entity keys");

        let root_cert = CertificateDer::from(
            root_keypair
                .serialize_der()
                .expect("failed to serialize root cert"),
        );
        let intermediate_cert = CertificateDer::from(
            intermediate_keypair
                .serialize_der_with_signer(&root_keypair)
                .expect("failed to serialize intermediate cert"),
        );
        let end_cert = CertificateDer::from(
            end_keypair
                .serialize_der_with_signer(&intermediate_keypair)
                .expect("failed to serialize end-entity cert"),
        );

        Self { root_cert, intermediate_cert, end_cert, end_keypair }
    }

    pub fn end_cert_private_key_as_der(&self) -> Vec<u8> {
        self.end_keypair.serialize_private_key_der()
    }

    pub fn end_cert_private_key_as_pem(&self) -> String {
        self.end_keypair.serialize_private_key_pem()
    }

    fn cert_chain(&self) -> Vec<CertificateDer> {
        vec![
            self.end_cert.clone(),
            self.intermediate_cert.clone(),
            self.root_cert.clone(),
        ]
    }

    pub fn cert_chain_as_pem(&self) -> String {
        tls_cert_to_pem(&self.cert_chain())
    }
}

fn tls_cert_to_pem(certs: &Vec<CertificateDer>) -> String {
    let mut serialized_certs = String::new();
    for cert in certs {
        let encoded_cert = pem::encode(&pem::Pem {
            tag: "CERTIFICATE".to_string(),
            contents: cert.as_ref().to_owned(),
        });

        serialized_certs.push_str(&encoded_cert);
    }
    serialized_certs
}
