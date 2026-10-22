#!/usr/bin/env bash
# Run on Linux administration host/Bastion, not inside the collector runtime.
set -euo pipefail
set +x
umask 077
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
usage() {
  cat <<'USAGE'
Usage: bash deploy.sh --inventory FILE --data-dir /var/lib/mongodb --state-dir /secure/state --prepare
Options: --version 9.0.4 --replica-set rs-products
Local preparation ONLY. No SSH, sudo, installation or initialization.
Copy node<ID>/ to its VM and run install.sh there. Preserve shared CA/key.
USAGE
}
validate_inventory() {
  local inventory=$1 id alias dns ip extra octet row label
  ids=(); aliases=(); names=(); ips=()
  while IFS= read -r row || [[ -n $row ]]; do
    row=${row%$'\r'}
    read -r id alias dns ip extra <<< "$row" || true
    [[ -z ${id:-} || $id == \#* ]] && continue
    [[ -z ${extra:-} && $id =~ ^(0|[1-9][0-9]{0,2})$ && $alias =~ ^[a-zA-Z][a-zA-Z0-9_-]*$ &&
       $dns =~ ^[a-z][a-z0-9-]*(\.[a-z0-9-]+)+$ && $ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail 'invalid inventory row'
    (( ${#dns} <= 253 )) || fail 'DNS name too long'
    IFS=. read -r -a labels <<< "$dns"
    for label in "${labels[@]}"; do
      [[ $label =~ ^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$ ]] || fail 'invalid DNS label'
    done
    IFS=. read -r -a octets <<< "$ip"
    for octet in "${octets[@]}"; do
      [[ $octet == 0 || $octet =~ ^[1-9][0-9]{0,2}$ ]] && (( 10#$octet <= 255 )) || fail 'invalid IPv4 octet'
    done
    [[ $ip == 10.* || $ip == 192.168.* || ( ${octets[0]} == 172 && ${octets[1]} -ge 16 && ${octets[1]} -le 31 ) ]] || fail 'private IPv4 required'
    ids+=("$id"); aliases+=("$alias"); names+=("$dns"); ips+=("$ip")
  done < "$inventory"
  (( ${#ids[@]} >= 1 && ${#ids[@]} <= 7 )) || fail 'inventory requires 1..7 voting data members'
  for id in ids aliases names ips; do
    local -n values=$id
    [[ $(printf '%s\n' "${values[@]}" | sort -u | wc -l) == ${#ids[@]} ]] || fail 'inventory identifiers/hosts/IPs must be unique'
  done
}
require_data_path() {
  [[ $1 =~ ^/(srv|var/lib)/[A-Za-z0-9_/-]+$ && $1 != */../* && $1 != */.. && $1 != */./* && $1 != */. && $1 != */ && $1 != *//* ]] || fail 'unsafe data path'
}
make_assets() {
  local i
  mkdir -m 0700 "$state"
  printf '%s\n' "$deployment_spec" > "$state/deployment.spec"
  printf '%s\n' "$version" > "$state/version.txt"
  openssl req -x509 -newkey rsa:3072 -sha256 -nodes -days 365 -keyout "$state/ca.key" -out "$state/ca.crt" \
    -subj '/O=Team3/CN=Team3 MongoDB Bootstrap CA' \
    -addext 'basicConstraints=critical,CA:TRUE' -addext 'keyUsage=critical,keyCertSign,cRLSign' 2>/dev/null
  openssl rand -base64 756 > "$state/cluster.key"
  MONGO_STATE=$state MONGO_VERSION=$version python3 - <<'PY'
import json,os,secrets
from pathlib import Path
p=Path(os.environ['MONGO_STATE'])/'admin.json'
p.write_text(json.dumps(dict(username='pj3-mongo-admin',password=secrets.token_urlsafe(40),server_version=os.environ['MONGO_VERSION'])))
PY
  for i in "${!ids[@]}"; do
    mkdir -m 0700 "$state/node${ids[i]}"
    openssl req -newkey rsa:3072 -nodes -keyout "$state/node${ids[i]}/server.key" -out "$state/node${ids[i]}/server.csr" \
      -subj "/O=Team3/CN=${names[i]}" 2>/dev/null
    cat > "$state/node${ids[i]}/extensions.cnf" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:${names[i]},DNS:localhost,IP:${ips[i]},IP:127.0.0.1
EOF
    openssl x509 -req -in "$state/node${ids[i]}/server.csr" -CA "$state/ca.crt" -CAkey "$state/ca.key" \
      -CAcreateserial -out "$state/node${ids[i]}/server.crt" -days 180 -sha256 -extfile "$state/node${ids[i]}/extensions.cnf" 2>/dev/null
    cat "$state/node${ids[i]}/server.key" "$state/node${ids[i]}/server.crt" > "$state/node${ids[i]}/node.pem"
    cp "$state/ca.crt" "$state/cluster.key" "$state/node${ids[i]}/"
    cp "$SCRIPT_DIR/node.sh" "$SCRIPT_DIR/bootstrap.js" "$SCRIPT_DIR/install.sh" "$SCRIPT_DIR/cluster.sh" "$state/node${ids[i]}/"
    printf '%s %s %s %s\n' "$version" "$data" "${ips[i]}" "${names[i]}" > "$state/node${ids[i]}/settings.txt"
    local j
    : > "$state/node${ids[i]}/hosts.txt"
    for j in "${!ids[@]}"; do printf '%s %s\n' "${ips[j]}" "${names[j]}" >> "$state/node${ids[i]}/hosts.txt"; done
    cat > "$state/node${ids[i]}/mongod.conf" <<EOF
storage:
  dbPath: $data
  wiredTiger:
    engineConfig:
      cacheSizeGB: 0.5
systemLog:
  destination: file
  logAppend: true
  path: /var/log/mongodb/mongod.log
net:
  port: 27017
  bindIp: 127.0.0.1,${ips[i]}
  tls:
    mode: requireTLS
    certificateKeyFile: /etc/mongodb/pki/node.pem
    CAFile: /etc/mongodb/pki/ca.crt
    allowConnectionsWithoutCertificates: true
security:
  authorization: enabled
  keyFile: /etc/mongodb/cluster.key
  clusterAuthMode: keyFile
replication:
  replSetName: ${replica_set:-rs-products}
  oplogSizeMB: 1024
processManagement:
  timeZoneInfo: /usr/share/zoneinfo
EOF
  done
  MONGO_STATE=$state MONGO_RS=${replica_set:-rs-products} MONGO_MEMBERS=$(for i in "${!ids[@]}"; do printf '%s %s\n' "${ids[i]}" "${names[i]}"; done) python3 - <<'INNER'
import json,os
from pathlib import Path
members=[dict(_id=int(r.split()[0]),host=r.split()[1]+':27017',priority=1,votes=1) for r in os.environ['MONGO_MEMBERS'].splitlines()]
(Path(os.environ['MONGO_STATE'])/'cluster.json').write_text(json.dumps(dict(_id=os.environ['MONGO_RS'],members=members)))
INNER
  for i in "${!ids[@]}"; do
    cp "$state/cluster.json" "$state/node${ids[i]}/"
    (cd "$state/node${ids[i]}"; sha256sum settings.txt hosts.txt mongod.conf ca.crt node.pem cluster.key cluster.json node.sh bootstrap.js install.sh cluster.sh > bundle.sha256)
  done
}
main() {
  local inventory= data= state= version=9.0.4 replica_set=rs-products mode= arg parent ancestor
  while (( $# )); do
    arg=$1; shift
    case $arg in
      --inventory|--data-dir|--state-dir|--version|--replica-set)
        (( $# )) || fail "missing value for $arg"
        case $arg in --inventory) inventory=$1;; --data-dir) data=$1;; --state-dir) state=$1;; --version) version=$1;; --replica-set) replica_set=$1;; esac; shift ;;
      --prepare) [[ -z $mode ]] || fail 'choose one action'; mode=$arg ;;
      --help|-h) usage; return ;;
      *) fail "unknown argument: $arg (bulk remote installation removed)" ;;
    esac
  done
  [[ -f $inventory && -n $data && -n $state && $mode == --prepare ]] || { usage; fail 'inventory, data-dir, state-dir and --prepare required'; }
  [[ $version =~ ^9\.0\.[0-9]+$ ]] && (( 10#${version##*.} >= 4 )) || fail 'pin a 9.0.x version >= 9.0.4'
  [[ $replica_set =~ ^[A-Za-z][A-Za-z0-9_-]{0,62}$ ]] || fail 'invalid replica set name'
  require_data_path "$data"; validate_inventory "$inventory"
  for arg in openssl python3 sha256sum realpath; do command -v "$arg" >/dev/null || fail "missing command: $arg"; done
  [[ $state =~ ^/[A-Za-z0-9_./-]+$ && $state != */../* && $state != */.. && $state != / && $state != */ ]] || fail 'unsafe state-dir'
  parent=$(dirname "$state"); [[ -d $parent ]] || fail 'state-dir parent must exist'
  [[ $(realpath -m "$state") == "$state" ]] || fail 'state-dir symlink/noncanonical path'
  ancestor=$parent
  while :; do
    [[ ! -e $ancestor/.git ]] || fail 'secrets state-dir must be outside any Git checkout'
    [[ $ancestor != / ]] || break
    ancestor=$(dirname "$ancestor")
  done
  [[ ! -e $state ]] || fail 'state-dir exists; never regenerate established cluster secrets'
  deployment_spec=$( { printf '%s\n' "$version" "$data" "$replica_set"; cat "$inventory"; } | sha256sum | cut -d' ' -f1)
  make_assets
  printf 'PREPARED bundles=%s; no servers contacted; no installation or replica initialization.\n' "${#ids[@]}"
}
if [[ ${BASH_SOURCE[0]} == "$0" ]]; then main "$@"; fi
