// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

//! Silo related authentication types and functions

use crate::context::OpContext;
use crate::db::lookup::LookupPath;
use crate::db::{model, DataStore};
use omicron_common::api::external::LookupResult;

use anyhow::{anyhow, Result};
use samael::metadata::ContactPerson;
use samael::metadata::ContactType;
use samael::metadata::EntityDescriptor;
use samael::metadata::NameIdFormat;
use samael::metadata::HTTP_REDIRECT_BINDING;
use samael::service_provider::ServiceProvider;
use samael::service_provider::ServiceProviderBuilder;

pub struct SamlIdentityProvider {
    pub idp_metadata_document_string: String,
    pub sp_client_id: String,
    pub acs_url: String,
    pub slo_url: String,
    pub technical_contact_email: String,
    pub public_cert: Option<String>,
    pub private_key: Option<String>,
}

impl TryFrom<model::SamlIdentityProvider> for SamlIdentityProvider {
    type Error = anyhow::Error;
    fn try_from(
        model: model::SamlIdentityProvider,
    ) -> Result<Self, Self::Error> {
        let provider = SamlIdentityProvider {
            idp_metadata_document_string: model.idp_metadata_document_string,
            sp_client_id: model.sp_client_id,
            acs_url: model.acs_url,
            slo_url: model.slo_url,
            technical_contact_email: model.technical_contact_email,
            public_cert: model.public_cert,
            private_key: model.private_key,
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

impl IdentityProviderType {
    /// First, look up the provider type, then look in for the specific
    /// provider details.
    pub async fn lookup(
        datastore: &DataStore,
        opctx: &OpContext,
        silo_name: &model::Name,
        provider_name: &model::Name,
    ) -> LookupResult<Self> {
        let (.., identity_provider) = LookupPath::new(opctx, datastore)
            .silo_name(silo_name)
            .identity_provider_name(provider_name)
            .fetch()
            .await?;

        match identity_provider.provider_type {
            model::IdentityProviderType::Saml => {
                let (.., saml_identity_provider) =
                    LookupPath::new(opctx, datastore)
                        .silo_name(silo_name)
                        .saml_identity_provider_name(provider_name)
                        .fetch()
                        .await?;

                Ok(IdentityProviderType::Saml(
                    saml_identity_provider.try_into()
                        .map_err(|e: anyhow::Error|
                            // If an error is encountered converting from the
                            // model to the authn type here, this is a server
                            // error: it was validated before it went into the
                            // DB.
                            omicron_common::api::external::Error::internal_error(
                                &format!(
                                    "saml_identity_provider.try_into() failed! {}",
                                    &e.to_string()
                                )
                            )
                        )?
                ))
            }
        }
    }
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

        if let Some(cert) = &self.public_cert_bytes()? {
            if let Ok(parsed) = openssl::x509::X509::from_der(&cert) {
                sp_builder.certificate(Some(parsed));
            }
        }

        Ok(sp_builder.build()?)
    }

    fn public_cert_bytes(&self) -> Result<Option<Vec<u8>>> {
        if let Some(cert) = &self.public_cert {
            Ok(Some(base64::decode(cert.as_bytes())?))
        } else {
            Ok(None)
        }
    }

    fn private_key_bytes(&self) -> Result<Option<Vec<u8>>> {
        if let Some(key) = &self.private_key {
            Ok(Some(base64::decode(key.as_bytes())?))
        } else {
            Ok(None)
        }
    }
}
