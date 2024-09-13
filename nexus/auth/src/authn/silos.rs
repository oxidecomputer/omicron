// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Silo related authentication types and functions

use anyhow::{anyhow, Result};
use base64::Engine;
use dropshot::HttpError;
use samael::metadata::ContactPerson;
use samael::metadata::ContactType;
use samael::metadata::EntityDescriptor;
use samael::metadata::NameIdFormat;
use samael::metadata::HTTP_REDIRECT_BINDING;
use samael::schema::Response as SAMLResponse;
use samael::service_provider::ServiceProvider;
use samael::service_provider::ServiceProviderBuilder;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
pub struct SamlIdentityProvider {
    pub idp_metadata_document_string: String,
    pub idp_entity_id: String,
    pub sp_client_id: String,
    pub acs_url: String,
    pub slo_url: String,
    pub technical_contact_email: String,
    pub public_cert: Option<String>,
    pub private_key: Option<String>,
    pub group_attribute_name: Option<String>,
}

impl TryFrom<nexus_db_model::SamlIdentityProvider> for SamlIdentityProvider {
    type Error = anyhow::Error;
    fn try_from(
        model: nexus_db_model::SamlIdentityProvider,
    ) -> Result<Self, Self::Error> {
        let provider = SamlIdentityProvider {
            idp_metadata_document_string: model.idp_metadata_document_string,
            idp_entity_id: model.idp_entity_id,
            sp_client_id: model.sp_client_id,
            acs_url: model.acs_url,
            slo_url: model.slo_url,
            technical_contact_email: model.technical_contact_email,
            public_cert: model.public_cert,
            private_key: model.private_key,
            group_attribute_name: model.group_attribute_name,
        };

        // check that the idp metadata document string parses into an EntityDescriptor
        let _idp_metadata: EntityDescriptor =
            provider.idp_metadata_document_string.parse()?;

        // check that there is a valid sign in url
        let _sign_in_url = provider.sign_in_url(None)?;

        Ok(provider)
    }
}

pub enum IdentityProviderType {
    Saml(SamlIdentityProvider),
}

impl SamlIdentityProvider {
    pub fn sign_in_url(&self, relay_state: Option<String>) -> Result<String> {
        let idp_metadata: EntityDescriptor =
            self.idp_metadata_document_string.parse()?;

        // return the *first* SSO HTTP-Redirect binding URL in the IDP metadata:
        //
        //   <md:SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://..."/>
        let sso_descriptors = idp_metadata
            .idp_sso_descriptors
            .as_ref()
            .ok_or_else(|| anyhow!("no IDPSSODescriptor"))?;

        if sso_descriptors.is_empty() {
            return Err(anyhow!("zero SSO descriptors"));
        }

        // Currently, we only support redirect binding
        let redirect_binding_locations = sso_descriptors[0]
            .single_sign_on_services
            .iter()
            .filter(|x| x.binding == HTTP_REDIRECT_BINDING)
            .map(|x| x.location.clone())
            .collect::<Vec<String>>();

        if redirect_binding_locations.is_empty() {
            return Err(anyhow!("zero redirect binding locations"));
        }

        let redirect_url = redirect_binding_locations[0].clone();

        // Create the authn request
        let provider = self.make_service_provider(idp_metadata)?;
        let authn_request = provider
            .make_authentication_request(&redirect_url)
            .map_err(|e| anyhow!(e.to_string()))?;

        let encoded_relay_state = if let Some(relay_state) = relay_state {
            relay_state
        } else {
            "".to_string()
        };

        let authn_request_url = if let Some(key) = self.private_key_bytes()? {
            // sign authn request if keys were supplied
            let pkey = openssl::pkey::PKey::private_key_from_der(&key)
                .map_err(|e| anyhow!(e.to_string()))?;
            authn_request.signed_redirect(&encoded_relay_state, pkey)
        } else {
            authn_request.redirect(&encoded_relay_state)
        }
        .map_err(|e| anyhow!(e.to_string()))?
        .ok_or_else(|| anyhow!("request url was none!".to_string()))?;

        Ok(authn_request_url.to_string())
    }

    fn make_service_provider(
        &self,
        idp_metadata: EntityDescriptor,
    ) -> Result<ServiceProvider> {
        let mut sp_builder = ServiceProviderBuilder::default();
        sp_builder.entity_id(self.sp_client_id.clone());
        sp_builder.allow_idp_initiated(true);
        sp_builder.contact_person(ContactPerson {
            email_addresses: Some(vec![self.technical_contact_email.clone()]),
            contact_type: Some(ContactType::Technical.value().to_string()),
            ..ContactPerson::default()
        });
        sp_builder.idp_metadata(idp_metadata);

        // 3.4.1.1 Element <NameIDPolicy>: If the Format value is omitted or set
        // to urn:oasis:names:tc:SAML:2.0:nameid-format:unspecified, then the
        // identity provider is free to return any kind of identifier
        sp_builder.authn_name_id_format(Some(
            NameIdFormat::UnspecifiedNameIDFormat.value().into(),
        ));

        sp_builder.acs_url(self.acs_url.clone());
        sp_builder.slo_url(self.slo_url.clone());

        if let Some(cert) = &self.public_cert_bytes()? {
            if let Ok(parsed) = openssl::x509::X509::from_der(&cert) {
                sp_builder.certificate(Some(parsed));
            }
        }

        Ok(sp_builder.build()?)
    }

    fn public_cert_bytes(&self) -> Result<Option<Vec<u8>>> {
        if let Some(cert) = &self.public_cert {
            Ok(Some(
                base64::engine::general_purpose::STANDARD
                    .decode(cert.as_bytes())?,
            ))
        } else {
            Ok(None)
        }
    }

    fn private_key_bytes(&self) -> Result<Option<Vec<u8>>> {
        if let Some(key) = &self.private_key {
            Ok(Some(
                base64::engine::general_purpose::STANDARD
                    .decode(key.as_bytes())?,
            ))
        } else {
            Ok(None)
        }
    }

    pub fn authenticated_subject(
        &self,
        body_bytes: &str,
        max_issue_delay: Option<chrono::Duration>,
    ) -> Result<(AuthenticatedSubject, Option<String>), HttpError> {
        // Given a post body, decode that as a SAMLResponse. With POST binding,
        // the SAMLResponse will be POSTed to Nexus as a base64 encoded XML
        // file, with optional relay state being whatever Nexus sent as part of
        // the SAMLRequest.
        let saml_post: SamlLoginPost = serde_urlencoded::from_str(body_bytes)
            .map_err(|e| {
            HttpError::for_bad_request(
                None,
                format!("error reading url encoded POST body! {}", e),
            )
        })?;

        let raw_response_bytes = base64::engine::general_purpose::STANDARD
            .decode(saml_post.saml_response.as_bytes())
            .map_err(|e| {
                HttpError::for_bad_request(
                    None,
                    format!("error base64 decoding SAMLResponse! {}", e),
                )
            })?;

        // This base64 decoded string is the SAMLResponse XML. Be aware that
        // parsing unauthenticated arbitrary XML is garbage and a source of
        // bugs. Samael uses serde to deserialize XML, so that at least is a
        // little more safe than other languages.
        let raw_response_str = std::str::from_utf8(&raw_response_bytes)
            .map_err(|e| {
                HttpError::for_bad_request(
                    None,
                    format!("decoded SAMLResponse not utf8 string! {}", e),
                )
            })?;

        let saml_response: SAMLResponse =
            raw_response_str.parse().map_err(|e| {
                HttpError::for_bad_request(
                    None,
                    format!("could not parse SAMLResponse string! {}", e),
                )
            })?;

        // Match issuer of SAMLResponse to this object's IDP entity id.
        let issuer = saml_response
            .issuer
            .as_ref()
            .ok_or_else(|| {
                HttpError::for_bad_request(
                    None,
                    "SAMLResponse has no issuer!".into(),
                )
            })?
            .value
            .as_ref()
            .ok_or_else(|| {
                HttpError::for_bad_request(
                    None,
                    "SAMLResponse has a blank issuer!".into(),
                )
            })?;

        if issuer != &self.idp_entity_id {
            return Err(HttpError::for_bad_request(
                None,
                format!(
                    "SAMLResponse issuer {} does not match configured idp entity id {}",
                    issuer, self.idp_entity_id,
                ),
            ));
        }

        // Drop the parsed SAMLResponse, create a samael ServiceProvider object,
        // and use it to parse the same SAMLResponse string into a
        // samael::Assertion but now in the context of the service provider.
        //
        // Notably based on the way samael is written this will validate any
        // signatures, and elide parts of the XML that are not signed.
        drop(saml_response);

        let idp_metadata: EntityDescriptor =
            self.idp_metadata_document_string.parse().map_err(|e| {
                HttpError::for_internal_error(format!(
                    "idp_metadata_document_string bad! {}",
                    e
                ))
            })?;

        let mut service_provider =
            self.make_service_provider(idp_metadata).map_err(|e| {
                HttpError::for_internal_error(format!(
                    "make_service_provider failed! {}",
                    e
                ))
            })?;

        if let Some(max_issue_delay) = max_issue_delay {
            service_provider.max_issue_delay = max_issue_delay;
        }

        let assertion = service_provider
            .parse_base64_response(&saml_post.saml_response, None)
            .map_err(|e| {
                HttpError::for_bad_request(
                    None,
                    format!("could not extract SAMLResponse assertion! {}", e),
                )
            })?;

        // If the response isn't signed, then parse_response above will fail.
        // Every assertion should also be signed. Check the signature and digest
        // schemes against an explicit allow list.
        let assertion_signature = assertion.signature.ok_or_else(|| {
            HttpError::for_bad_request(
                None,
                "assertion is missing signature!".to_string(),
            )
        })?;

        match assertion_signature.signed_info.signature_method.algorithm.value()  {
            // List taken from Signature section of
            // https://www.w3.org/TR/xmldsig-core1/#sec-AlgID, removing
            // discouraged items.

            // Required
            "http://www.w3.org/2001/04/xmldsig-more#rsa-sha256" |
            "http://www.w3.org/2001/04/xmldsig-more#ecdsa-sha256" |
            // Optional
            "http://www.w3.org/2001/04/xmldsig-more#rsa-sha224" |
            "http://www.w3.org/2001/04/xmldsig-more#rsa-sha384" |
            "http://www.w3.org/2001/04/xmldsig-more#rsa-sha512" |
            "http://www.w3.org/2001/04/xmldsig-more#ecdsa-sha224" |
            "http://www.w3.org/2001/04/xmldsig-more#ecdsa-sha384" |
            "http://www.w3.org/2001/04/xmldsig-more#ecdsa-sha512" |
            "http://www.w3.org/2009/xmldsig11#dsa-sha256" => {}

            signature_algorithm => {
                return Err(
                    HttpError::for_bad_request(
                        None,
                        format!("signature algorithm {} is not allowed", signature_algorithm),
                    )
                );
            }
        }

        // What is being asserted? Extract subject
        let subject = assertion.subject.ok_or_else(|| {
            HttpError::for_bad_request(None, "no subject in assertion".into())
        })?;

        let subject_name_id = &subject.name_id.ok_or_else(|| {
            HttpError::for_bad_request(
                None,
                "no subject name id in assertion".into(),
            )
        })?;

        // Extract group membership attributes
        let mut groups = vec![];

        if let Some(group_attribute_name) = &self.group_attribute_name {
            if let Some(attribute_statements) = &assertion.attribute_statements
            {
                for attribute_statement in attribute_statements {
                    for attribute in &attribute_statement.attributes {
                        if let Some(name) = &attribute.name {
                            if name == group_attribute_name {
                                for attribute_value in &attribute.values {
                                    if let Some(value) = &attribute_value.value
                                    {
                                        // Read comma separated group names
                                        for group in value.split(',') {
                                            // Trim whitespace
                                            let group =
                                                group.trim().to_string();

                                            // Skip empty groups
                                            if group.is_empty() {
                                                continue;
                                            }

                                            groups.push(group);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let authenticated_subject = AuthenticatedSubject {
            external_id: subject_name_id.value.clone(),
            groups,
        };

        Ok((authenticated_subject, saml_post.relay_state))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SamlLoginPost {
    #[serde(rename = "SAMLResponse")]
    pub saml_response: String,
    #[serde(rename = "RelayState")]
    pub relay_state: Option<String>,
}

pub struct AuthenticatedSubject {
    pub external_id: String,
    pub groups: Vec<String>,
}
