#!/usr/bin/env bash

set -euo pipefail
umask 077

: "${GCP_SUBJECT:?Set GCP_SUBJECT to the numeric unique ID of the GCP service account}"

export OXIDE_HOST=${NEXUS_URL:-http://127.0.0.1:12220}
unset OXIDE_TOKEN OXIDE_PROFILE

login() {
    local silo=$1 username=$2 profile=$3 cookie client_id device token user_id
    cookie=$(OXIDE_TOKEN='' oxide api --method POST --include \
        --header 'Authorization:' --raw-field "username=$username" \
        --raw-field password=oxide "/v1/login/$silo/local" |
        awk 'tolower($1) == "set-cookie:" {
            value = $2; sub(/^"/, "", value); sub(/;.*/, "", value); print value
        }')
    [[ $cookie == session=* ]]
    client_id=$(uuidgen)
    device=$(printf 'client_id=%s&ttl_seconds=86400' "$client_id" |
        OXIDE_TOKEN='' oxide api --method POST --input - \
        --header 'Authorization:' --header "Cookie:$cookie" \
        --header 'Content-Type:application/x-www-form-urlencoded' /device/auth)
    jq '{user_code}' <<< "$device" |
        OXIDE_TOKEN='' oxide api --method POST --input - \
        --header 'Authorization:' --header "Cookie:$cookie" /device/confirm >/dev/null
    token=$(jq -r --arg client_id "$client_id" '{
        grant_type: "urn:ietf:params:oauth:grant-type:device_code",
        client_id: $client_id, device_code: .device_code
    } | to_entries | map((.key | @uri) + "=" + (.value | @uri)) | join("&")' \
        <<< "$device" | OXIDE_TOKEN='' oxide api --method POST --input - \
        --header 'Authorization:' \
        --header 'Content-Type:application/x-www-form-urlencoded' /device/token)
    user_id=$(OXIDE_TOKEN=$(jq -er .access_token <<< "$token") \
        oxide current-user view | jq -er .id)
    OXIDE_TOKEN='' oxide api --method POST --header 'Authorization:' \
        --header "Cookie:$cookie" /v1/logout >/dev/null
    oxide --profile "$profile" auth logout --force >/dev/null
    mkdir -p "$HOME/.config/oxide"
    jq -r --arg profile "$profile" --arg host "$OXIDE_HOST" --arg user "$user_id" '
        "\n[profile.\($profile | tojson)]",
        "host = \($host | tojson)",
        "token = \(.access_token | tojson)",
        "token_id = \(.token_id | tojson)",
        "user = \($user | tojson)",
        "time_expires = \(.time_expires | tojson)"
    ' <<< "$token" >> "$HOME/.config/oxide/credentials.toml"
    chmod 600 "$HOME/.config/oxide/credentials.toml"
    printf 'Saved profile %s\n' "$profile"
}

login test-suite-silo test-privileged inbound-bootstrap

# Set up test resources. We'll test silo- and project-scoped OIDC grants, so we set up multiple
# silos and projects, each with a simple test resource to act on:
# * silo a
#   * project 1
#     * anti-affinity group fixture
#   * project 2
#     * anti-affinity group fixture
#   * project 3
#     * anti-affinity group fixture
# * silo b
#   * project 4
#     * anti-affinity group fixture
# * silo c
#   * project 5
#     * anti-affinity group fixture

oxide --profile inbound-bootstrap silo create --json-body /dev/stdin <<'JSON'
{"name":"a","description":"Inbound OIDC PoC","identity_mode":"local_only",
 "discoverable":true,"admin_group_name":null,"tls_certificates":[],
 "mapped_fleet_roles":{},"quotas":{"cpus":0,"memory":0,"storage":0}}
JSON
oxide --profile inbound-bootstrap silo create --json-body /dev/stdin <<'JSON'
{"name":"b","description":"Inbound OIDC PoC","identity_mode":"local_only",
 "discoverable":true,"admin_group_name":null,"tls_certificates":[],
 "mapped_fleet_roles":{},"quotas":{"cpus":0,"memory":0,"storage":0}}
JSON
oxide --profile inbound-bootstrap silo create --json-body /dev/stdin <<'JSON'
{"name":"c","description":"Inbound OIDC PoC","identity_mode":"local_only",
 "discoverable":true,"admin_group_name":null,"tls_certificates":[],
 "mapped_fleet_roles":{},"quotas":{"cpus":0,"memory":0,"storage":0}}
JSON

admin_a=$(oxide --profile inbound-bootstrap silo idp local user create \
    --silo a --json-body /dev/stdin <<'JSON' | jq -er .id
{"external_id":"admin","password":{"mode":"password","value":"oxide"}}
JSON
)
admin_b=$(oxide --profile inbound-bootstrap silo idp local user create \
    --silo b --json-body /dev/stdin <<'JSON' | jq -er .id
{"external_id":"admin","password":{"mode":"password","value":"oxide"}}
JSON
)
admin_c=$(oxide --profile inbound-bootstrap silo idp local user create \
    --silo c --json-body /dev/stdin <<'JSON' | jq -er .id
{"external_id":"admin","password":{"mode":"password","value":"oxide"}}
JSON
)

oxide --profile inbound-bootstrap silo policy update --silo a --json-body /dev/stdin <<JSON
{"role_assignments":[{"identity_type":"silo_user","identity_id":"$admin_a","role_name":"admin"}]}
JSON
oxide --profile inbound-bootstrap silo policy update --silo b --json-body /dev/stdin <<JSON
{"role_assignments":[{"identity_type":"silo_user","identity_id":"$admin_b","role_name":"admin"}]}
JSON
oxide --profile inbound-bootstrap silo policy update --silo c --json-body /dev/stdin <<JSON
{"role_assignments":[{"identity_type":"silo_user","identity_id":"$admin_c","role_name":"admin"}]}
JSON

login a admin inbound-a
login b admin inbound-b
login c admin inbound-c

project_1=$(oxide --profile inbound-a project create --json-body /dev/stdin <<'JSON' | jq -er .id
{"name":"project-1","description":"Inbound OIDC PoC","defaults":{}}
JSON
)
project_2=$(oxide --profile inbound-a project create --json-body /dev/stdin <<'JSON' | jq -er .id
{"name":"project-2","description":"Inbound OIDC PoC","defaults":{}}
JSON
)
oxide --profile inbound-a project create --json-body /dev/stdin <<'JSON'
{"name":"project-3","description":"Inbound OIDC PoC","defaults":{}}
JSON
oxide --profile inbound-b project create --json-body /dev/stdin <<'JSON'
{"name":"project-4","description":"Inbound OIDC PoC","defaults":{}}
JSON
oxide --profile inbound-c project create --json-body /dev/stdin <<'JSON'
{"name":"project-5","description":"Inbound OIDC PoC","defaults":{}}
JSON

oxide --profile inbound-a instance anti-affinity create --project project-1 \
    --name fixture --description 'Inbound OIDC PoC' --policy allow --failure-domain sled
oxide --profile inbound-a instance anti-affinity create --project project-2 \
    --name fixture --description 'Inbound OIDC PoC' --policy allow --failure-domain sled
oxide --profile inbound-a instance anti-affinity create --project project-3 \
    --name fixture --description 'Inbound OIDC PoC' --policy allow --failure-domain sled
oxide --profile inbound-b instance anti-affinity create --project project-4 \
    --name fixture --description 'Inbound OIDC PoC' --policy allow --failure-domain sled
oxide --profile inbound-c instance anti-affinity create --project project-5 \
    --name fixture --description 'Inbound OIDC PoC' --policy allow --failure-domain sled

# Configure OIDC resources, using `oxide api` because we haven't built `oxide` with the new OpenAPI
# spec. Create identity providers in silos a and b, using GCP for convenience, and a trust policy in
# each as well:
# * silo a / trust policy silo-a-project-access
#   * project 1 : viewer
#   * project 2 : collaborator
# * silo b / trust policy silo-b-silo-access
#   * silo b : viewer

oxide --profile inbound-a api --method POST --input - \
    /v1/federation/inbound/identity-providers <<'JSON'
{"name":"gcp-silo-a","description":"Inbound OIDC PoC",
 "issuer":"https://accounts.google.com","audience":"oxide-inbound-poc",
 "verification_type":"oidc_discovery"}
JSON
oxide --profile inbound-b api --method POST --input - \
    /v1/federation/inbound/identity-providers <<'JSON'
{"name":"gcp-silo-2","description":"Inbound OIDC PoC",
 "issuer":"https://accounts.google.com","audience":"oxide-inbound-poc",
 "verification_type":"oidc_discovery"}
JSON

policy_json=$(jq -n --arg subject "$GCP_SUBJECT" \
    '"assume(claims) if claims.sub = \($subject | tojson);"')

oxide --profile inbound-a api --method POST --input - \
    /v1/federation/inbound/trust-policies <<JSON
{"name":"silo-a-project-access","description":"Inbound OIDC PoC",
 "identity_provider":"gcp-silo-a",
 "policy":$policy_json,
 "grants":[
   {"resource_kind":"project","resource_id":"$project_1","role_name":"viewer"},
   {"resource_kind":"project","resource_id":"$project_2","role_name":"collaborator"}
 ]}
JSON
silo_b=$(oxide --profile inbound-bootstrap silo view --silo b | jq -er .id)
oxide --profile inbound-b api --method POST --input - \
    /v1/federation/inbound/trust-policies <<JSON
{"name":"silo-b-silo-access","description":"Inbound OIDC PoC",
 "identity_provider":"gcp-silo-2",
 "policy":$policy_json,
 "grants":[{"resource_kind":"silo","resource_id":"$silo_b","role_name":"viewer"}]}
JSON
