#!/usr/bin/env bash

set -euo pipefail
umask 077

: "${GCP_SUBJECT:?Set GCP_SUBJECT to the numeric unique ID of the GCP service account}"
: "${GCP_OIDC_TOKEN:?Set GCP_OIDC_TOKEN to a fresh GCP ID token for audience oxide-inbound-poc}"

printf 'Setting up service account E2E fixtures on a fresh run-all instance.\n'
audit_start=$(date -u +%Y-%m-%dT%H:%M:%SZ)

export OXIDE_HOST=${NEXUS_URL:-http://127.0.0.1:12220}
unset OXIDE_TOKEN OXIDE_PROFILE

expect_error() {
    local status=$1 output
    shift
    if output=$("$@" 2>&1); then
        printf 'FAIL: expected HTTP %s, but the request succeeded\n' "$status" >&2
        return 1
    fi
    if [[ $output != *"status code: $status "* && $output != *"status: $status "* ]]; then
        printf 'FAIL: expected HTTP %s, got:\n%s\n' "$status" "$output" >&2
        return 1
    fi
    printf 'PASS: expected HTTP %s\n' "$status"
}

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
    token=$(jq -j --arg client_id "$client_id" '{
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

silo_a=$(oxide --profile inbound-bootstrap silo view --silo a | jq -er .id)
silo_b=$(oxide --profile inbound-bootstrap silo view --silo b | jq -er .id)
project_4=$(oxide --profile inbound-b project view --project project-4 | jq -er .id)
project_5=$(oxide --profile inbound-c project view --project project-5 | jq -er .id)

idp_a=$(oxide --profile inbound-a api --method POST --input - \
    /v1/federation/inbound/identity-providers <<'JSON' | jq -er .id
{"name":"gcp-silo-a","description":"Service account E2E",
 "issuer":"https://accounts.google.com","audience":"oxide-inbound-poc",
 "verification_type":"oidc_discovery"}
JSON
)
oxide --profile inbound-b api --method POST --input - \
    /v1/federation/inbound/identity-providers <<'JSON'
{"name":"gcp-silo-b","description":"Service account E2E",
 "issuer":"https://accounts.google.com","audience":"oxide-inbound-poc",
 "verification_type":"oidc_discovery"}
JSON

policy_json=$(jq -n --arg subject "$GCP_SUBJECT" \
    '"assume(claims) if claims.sub = \($subject | tojson);"')

sa_a=$(oxide --profile inbound-a api --method POST --input - \
    '/v1/service-accounts/silo?silo=a' <<JSON | jq -er .id
{"name":"silo-a-project-access","description":"Service account E2E",
 "grants":[
   {"resource_kind":"project","resource_id":"$project_1","role_name":"viewer"},
   {"resource_kind":"project","resource_id":"$project_2","role_name":"collaborator"}
 ],
 "federation":{"identity_provider":"gcp-silo-a","policy":$policy_json}}
JSON
)
sa_b=$(oxide --profile inbound-b api --method POST --input - \
    '/v1/service-accounts/silo?silo=b' <<JSON | jq -er .id
{"name":"silo-b-silo-access","description":"Service account E2E",
 "grants":[{"resource_kind":"silo","resource_id":"$silo_b","role_name":"viewer"}],
 "federation":{"identity_provider":"gcp-silo-b","policy":$policy_json}}
JSON
)
sa_project=$(oxide --profile inbound-a api --method POST --input - \
    '/v1/service-accounts/project?project=project-1' <<JSON | jq -er .id
{"name":"project-reader","description":"Service account E2E",
 "grants":[{"resource_kind":"project","resource_id":"$project_1","role_name":"viewer"}]}
JSON
)

printf 'Checking authenticated assumption and project scope.\n'
direct=$(oxide --profile inbound-a api --method POST --input - \
    "/v1/service-accounts/silo/$sa_a/token" <<'JSON'
{"ttl_seconds":3600}
JSON
)
direct_token=$(jq -er .token <<< "$direct")
direct_id=$(jq -er .id <<< "$direct")
project_token=$(oxide --profile inbound-a api --method POST --input - \
    '/v1/service-accounts/project/project-reader/token?project=project-1' <<'JSON' | jq -er .token
{"ttl_seconds":3600}
JSON
)
OXIDE_TOKEN=$project_token oxide instance anti-affinity view \
    --project project-1 --anti-affinity-group fixture | jq -e '.name == "fixture"' >/dev/null
expect_error 404 env OXIDE_TOKEN="$project_token" oxide instance anti-affinity list \
    --project project-2
expect_error 403 env OXIDE_TOKEN="$project_token" oxide api --method POST --input - \
    "/v1/service-accounts/project/$sa_project/token" <<'JSON'
{}
JSON
OXIDE_TOKEN=$direct_token oxide instance anti-affinity view \
    --project project-1 --anti-affinity-group fixture | jq -e '.name == "fixture"' >/dev/null
expect_error 403 env OXIDE_TOKEN="$direct_token" oxide instance anti-affinity create \
    --project project-1 --name denied --description E2E --policy allow --failure-domain sled
OXIDE_TOKEN=$direct_token oxide instance anti-affinity create --project project-2 \
    --name direct-write --description E2E --policy allow --failure-domain sled >/dev/null
expect_error 404 env OXIDE_TOKEN="$direct_token" oxide instance anti-affinity list \
    --project project-3
expect_error 404 env OXIDE_TOKEN="$direct_token" oxide instance anti-affinity list \
    --project "$project_5"

printf 'Exchanging GCP JWTs using OIDC discovery.\n'
federated_a=$(jq -n --arg jwt "$GCP_OIDC_TOKEN" '{jwt: $jwt}' |
    oxide --profile inbound-a api --method POST --input - \
        --header 'Host:a.sys.oxide-dev.test' --header 'Authorization:' \
        "/v1/service-accounts/silo/$sa_a/federation-token")
federated_a_token=$(jq -er .token <<< "$federated_a")
federated_a_id=$(jq -er .id <<< "$federated_a")
federated_b=$(jq -n --arg jwt "$GCP_OIDC_TOKEN" '{jwt: $jwt}' |
    oxide --profile inbound-b api --method POST --input - \
        --header 'Host:b.sys.oxide-dev.test' --header 'Authorization:' \
        "/v1/service-accounts/silo/$sa_b/federation-token")
federated_b_token=$(jq -er .token <<< "$federated_b")

printf 'Checking federated project grants and silo isolation.\n'
OXIDE_TOKEN=$federated_a_token oxide instance anti-affinity view \
    --project project-1 --anti-affinity-group fixture | jq -e '.name == "fixture"' >/dev/null
expect_error 403 env OXIDE_TOKEN="$federated_a_token" oxide instance anti-affinity create \
    --project project-1 --name denied --description E2E --policy allow --failure-domain sled
OXIDE_TOKEN=$federated_a_token oxide instance anti-affinity view \
    --project project-2 --anti-affinity-group fixture | jq -e '.name == "fixture"' >/dev/null
OXIDE_TOKEN=$federated_a_token oxide instance anti-affinity create --project project-2 \
    --name federated-write --description E2E --policy allow --failure-domain sled >/dev/null
expect_error 404 env OXIDE_TOKEN="$federated_a_token" oxide instance anti-affinity list \
    --project project-3
expect_error 404 env OXIDE_TOKEN="$federated_a_token" oxide instance anti-affinity create \
    --project project-3 --name denied --description E2E --policy allow --failure-domain sled
expect_error 404 env OXIDE_TOKEN="$federated_a_token" oxide instance anti-affinity list \
    --project "$project_4"
expect_error 404 env OXIDE_TOKEN="$federated_a_token" oxide instance anti-affinity list \
    --project "$project_5"
expect_error 404 env OXIDE_TOKEN="$federated_a_token" oxide instance anti-affinity create \
    --project "$project_5" --name denied --description E2E --policy allow --failure-domain sled

printf 'Checking federated silo viewer grants.\n'
OXIDE_TOKEN=$federated_b_token oxide instance anti-affinity view \
    --project project-4 --anti-affinity-group fixture | jq -e '.name == "fixture"' >/dev/null
expect_error 403 env OXIDE_TOKEN="$federated_b_token" oxide instance anti-affinity create \
    --project project-4 --name denied --description E2E --policy allow --failure-domain sled
expect_error 404 env OXIDE_TOKEN="$federated_b_token" oxide instance anti-affinity list \
    --project "$project_1"
expect_error 404 env OXIDE_TOKEN="$federated_b_token" oxide instance anti-affinity list \
    --project "$project_5"
expect_error 404 env OXIDE_TOKEN="$federated_b_token" oxide instance anti-affinity create \
    --project "$project_5" --name denied --description E2E --policy allow --failure-domain sled

printf 'Checking audit provenance for direct and federated writes.\n'
audit=$(oxide --profile inbound-bootstrap api --method GET \
    --header 'api-version:2026100903.0.0' --raw-field "start_time=$audit_start" \
    --field limit=1000 /v1/system/audit-log)
jq -e --arg sa "$sa_a" --arg silo "$silo_a" --arg token "$direct_id" '
    any(.items[]; .actor.kind == "service_account" and
        .actor.service_account_id == $sa and .actor.silo_id == $silo and
        .credential_id == $token and .auth_method == "service_account_token" and
        .operation_id == "anti_affinity_group_create" and .result.kind == "success" and
        .actor.federation == null)
' <<< "$audit" >/dev/null
jq -e --arg sa "$sa_a" --arg token "$federated_a_id" --arg idp "$idp_a" \
    --arg sub "$GCP_SUBJECT" '
    any(.items[]; .actor.kind == "service_account" and
        .actor.service_account_id == $sa and .credential_id == $token and
        .auth_method == "service_account_token" and
        .operation_id == "anti_affinity_group_create" and .result.kind == "success" and
        .actor.federation == {idp_id: $idp, iss: "https://accounts.google.com", sub: $sub})
' <<< "$audit" >/dev/null

printf 'Checking revocation and federation policy invalidation.\n'
oxide --profile inbound-a api --method DELETE \
    "/v1/service-accounts/silo/$sa_a/tokens/$direct_id" >/dev/null
expect_error 401 env OXIDE_TOKEN="$direct_token" oxide instance anti-affinity list \
    --project project-1
oxide --profile inbound-a api --method PATCH --input - \
    "/v1/service-accounts/silo/$sa_a" <<'JSON' >/dev/null
{"federation":{"identity_provider":"gcp-silo-a","policy":"assume(_claims) if false;"}}
JSON
expect_error 401 env OXIDE_TOKEN="$federated_a_token" oxide instance anti-affinity list \
    --project project-1
jq -n --arg jwt "$GCP_OIDC_TOKEN" '{jwt: $jwt}' |
    expect_error 403 oxide --profile inbound-a api --method POST --input - \
        --header 'Host:a.sys.oxide-dev.test' --header 'Authorization:' \
        "/v1/service-accounts/silo/$sa_a/federation-token"
oxide --profile inbound-a api --method PATCH --input - \
    "/v1/service-accounts/silo/$sa_a" <<JSON >/dev/null
{"federation":{"identity_provider":"gcp-silo-a","policy":$policy_json}}
JSON

printf 'PASS: service account E2E checks completed.\n'
printf 'Profiles inbound-a, inbound-b, and inbound-c remain available for exploration.\n'
