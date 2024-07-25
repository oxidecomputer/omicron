use attest_data::Nonce;
use der::{Encode, Reader};
use dice_verifier::PkiPathSignatureVerifier;
use hubpack::SerializedSize;
use pem_rfc7468::{LineEnding, PemLabel};
use x509_cert::{der::DecodePem, Certificate, PkiPath};

fn main() {
    let ipcc = ipcc::Ipcc::new().unwrap();

    let cert_chain_bytes = ipcc.rot_get_cert_chain().unwrap();
    let mut pem_bytes = vec![];

    std::fs::write("chain.pem", &cert_chain_bytes).unwrap();
    let mut idx = 0;
    while idx < cert_chain_bytes.len() {
        let reader = der::SliceReader::new(&cert_chain_bytes[idx..]).unwrap();
        let header = reader.peek_header().unwrap();
        let seq_len: usize = header.length.try_into().unwrap();
        let tag_len: usize = header.encoded_len().unwrap().try_into().unwrap();
        let end = idx + seq_len + tag_len;

        let pem = pem_rfc7468::encode_string(
            Certificate::PEM_LABEL,
            LineEnding::default(),
            &cert_chain_bytes[idx..end],
        )
        .unwrap();

        pem_bytes.extend_from_slice(&pem.as_bytes());
        idx += seq_len + tag_len;
    }

    let cert_chain: PkiPath = Certificate::load_pem_chain(&pem_bytes).unwrap();
    let root = std::fs::read("root.pem").unwrap();

    println!("Cert chain of len {}", cert_chain.len());
    for c in &cert_chain {
        println!("{}", c.tbs_certificate.subject);
    }
    let verifier = PkiPathSignatureVerifier::new(Some(
        Certificate::from_pem(root).unwrap(),
    ))
    .unwrap();
    println!("verify chain");
    verifier.verify(&cert_chain).unwrap();

    println!("getting measurement log");
    let log = ipcc.rot_get_measurement_log().unwrap();

    println!("usincg Nonce from platform RNG");
    let nonce = Nonce::from_platform_rng().unwrap();

    println!(
        "Using nonce {:x?} for attestation {}",
        nonce,
        nonce.as_ref().len()
    );

    let (attest, _) =
        hubpack::deserialize(&ipcc.rot_attest(&nonce.as_ref()).unwrap())
            .unwrap();

    println!("verify attesattion");

    dice_verifier::verify_attestation(&cert_chain[0], &attest, &log, &nonce)
        .unwrap();

    //std::fs::write("certs.pem", &cert);
    println!("all done");
}
