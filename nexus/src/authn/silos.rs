// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Silo related authentication types and functions

use crate::db::model::SiloSamlIdentityProvider;

use anyhow::{anyhow, bail, Result};
use samael::metadata::ContactPerson;
use samael::metadata::ContactType;
use samael::metadata::EntityDescriptor;
use samael::metadata::NameIdFormat;
use samael::metadata::HTTP_REDIRECT_BINDING;
use samael::service_provider::ServiceProvider;
use samael::service_provider::ServiceProviderBuilder;

pub enum SiloIdentityProviderType {
    Local,
    Ldap,
    Saml(Box<SiloSamlIdentityProvider>),
}

impl SiloSamlIdentityProvider {
    /// return an error if this SiloSamlIdentityProvider is invalid
    pub fn validate(&self) -> Result<()> {
        // check that the idp metadata document string parses into an EntityDescriptor
        let _idp_metadata: EntityDescriptor =
            self.idp_metadata_document_string.parse()?;

        // check that there is a valid sign in url
        let _sign_in_url = self.sign_in_url(None)?;

        // if keys were supplied, check that both public and private are here
        if self.get_public_cert_bytes()?.is_some()
            && self.get_private_key_bytes()?.is_none()
        {
            bail!("public and private key must be supplied together");
        }
        if self.get_public_cert_bytes()?.is_none()
            && self.get_private_key_bytes()?.is_some()
        {
            bail!("public and private key must be supplied together");
        }

        // TODO validate DER keys

        Ok(())
    }

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

        let authn_request_url =
            if let Some(key) = self.get_private_key_bytes()? {
                // sign authn request if keys were supplied
                authn_request.signed_redirect(&encoded_relay_state, &key)
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

        if let Some(cert) = &self.public_cert {
            if let Ok(decoded) = base64::decode(cert.as_bytes()) {
                if let Ok(parsed) = openssl::x509::X509::from_der(&decoded) {
                    sp_builder.certificate(Some(parsed));
                }
            }
        }

        Ok(sp_builder.build()?)
    }

    fn get_public_cert_bytes(&self) -> Result<Option<Vec<u8>>> {
        if let Some(cert) = &self.public_cert {
            Ok(Some(base64::decode(cert.as_bytes())?))
        } else {
            Ok(None)
        }
    }

    fn get_private_key_bytes(&self) -> Result<Option<Vec<u8>>> {
        if let Some(key) = &self.private_key {
            Ok(Some(base64::decode(key.as_bytes())?))
        } else {
            Ok(None)
        }
    }
}
