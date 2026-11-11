This directory currently only contains data files related to SAML integration
tests.

saml_idp_descriptor.xml comes from https://en.wikipedia.org/wiki/SAML_metadata#Identity_provider_metadata.

The samling (https://fujifish.github.io/samling/samling.html) tool was used to
generate saml_response.xml. I edited the NotOnOrAfter values to make the
SAMLResponses expire in the distant future, then turned that into a signing
template and manually signed with xmlsec1. Different scenarios required editing
the SAML response and resigning (for example, adding comments or group
attributes).

To create a signing template, find (or add) the appropriate ds:Signature
block(s) and blank out the DigestValue, SignatureValue, and X509Certificate:

    <ds:Signature xmlns:ds="http://www.w3.org/2000/09/xmldsig#">
      <ds:SignedInfo>
        <ds:CanonicalizationMethod Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
        <ds:SignatureMethod Algorithm="http://www.w3.org/2001/04/xmldsig-more#rsa-sha256"/>
        <ds:Reference URI="#_f2f02ca12d7292f8a4e03b7b99d71a45f8896c2677">
          <ds:Transforms>
            <ds:Transform Algorithm="http://www.w3.org/2000/09/xmldsig#enveloped-signature"/>
            <ds:Transform Algorithm="http://www.w3.org/2001/10/xml-exc-c14n#"/>
          </ds:Transforms>
          <ds:DigestMethod Algorithm="http://www.w3.org/2001/04/xmlenc#sha256"/>
          <ds:DigestValue />     <----- HERE
        </ds:Reference>
      </ds:SignedInfo>
      <ds:SignatureValue></ds:SignatureValue>     <----- HERE
      <ds:KeyInfo>
        <ds:X509Data>
          <ds:X509Certificate></ds:X509Certificate>     <----- HERE
        </ds:X509Data>
      </ds:KeyInfo>
    </ds:Signature>

Edit the SignatureMethod or DigestMethod as required.

Download samling.key and samling.crt from the samling tool's site, then sign
with xmlsec1, first signing the assertion (where node id is for the
saml:Assertion node), then the whole response:

    KEY="samling.key"
    CRT="samling.crt"

    xmlsec1 --sign --print-crypto-error-msgs --trusted-pem ${CRT} --privkey-pem $KEY,$CRT \
        --output tmp.xml --id-attr:ID Assertion --node-id _a8d644a6c83fc56337b0330d8b0ff684978a5fe56c response_template_to_sign.xml

    xmlsec1 --sign --trusted-pem ${CRT} --privkey-pem $KEY,$CRT \
        --output response_signed.xml --id-attr:ID urn:oasis:names:tc:SAML:2.0:protocol:Response tmp.xml

Verify with:

    xmlsec1 --verify --insecure --trusted-pem ${CRT} --id-attr:ID Response response_signed.xml
    xmlsec1 --verify --insecure --trusted-pem ${CRT} --id-attr:ID Assertion --node-id _a8d644a6c83fc56337b0330d8b0ff684978a5fe56c response_signed.xml

