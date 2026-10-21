#!/usr/bin/env bash
# Local installation helpers; no SSH, credentials on argv or tracing.
set -euo pipefail
set +x
umask 077

fail() { printf 'ERROR: %s\n' "$*" >&2; exit 1; }
require_version() { [[ $1 =~ ^9\.0\.[0-9]+$ ]] && dpkg --compare-versions "$1" ge 9.0.4 || fail 'Ubuntu 26.04 requires MongoDB 9.0.4+ in the pinned 9.0 series'; }
require_data_path() {
  [[ $1 =~ ^/(srv|var/lib)/[A-Za-z0-9_/-]+$ && $1 != */../* && $1 != */.. && $1 != */./* && $1 != */. && $1 != */ && $1 != *//* ]] || fail 'unsafe data path'
  [[ $(realpath -m -- "$1") == "$1" ]] || fail 'data path has a symlink or is not canonical'
}
check_os() {
  local os_file=${1:-/etc/os-release}
  # OS file is system-owned in production; a fixture can be passed by local tests.
  local ID= VERSION_ID= VERSION_CODENAME=
  # shellcheck disable=SC1090
  source "$os_file"
  [[ $ID == ubuntu && $VERSION_ID == 26.04 && $VERSION_CODENAME == resolute ]] || fail 'only Ubuntu 26.04/resolute is supported by this installer'
}
check_kernel() {
  local release=$1 signature=${2:-} effective=$1 vendor build extra major minor patch
  if [[ -n $signature ]]; then
    read -r vendor build effective extra <<< "$signature"
    [[ $vendor == Ubuntu && -z ${extra:-} && $build == "${release%%-*}-"* ]] || fail 'unrecognized Ubuntu kernel signature'
  fi
  [[ $effective =~ ^([0-9]+)\.([0-9]+)\.([0-9]+)([-+.]|$) ]] || fail 'kernel version cannot be verified'
  major=$((10#${BASH_REMATCH[1]})); minor=$((10#${BASH_REMATCH[2]})); patch=$((10#${BASH_REMATCH[3]}))
  if (( (major == 6 && minor >= 19) || (major == 7 && minor == 0 && patch < 14) )); then
    fail 'MongoDB TCMalloc incompatibility: kernel 6.19 through 7.0.13; use a supported fixed stock kernel'
  fi
  printf 'KERNEL_PREFLIGHT reported=%s upstream=%s\n' "$release" "$effective"
}
precheck() {
  local version=$1 data=$2 ip=$3 dns=$4 resume=$5 parent available
  check_os; require_version "$version"; require_data_path "$data"
  [[ $(cat /proc/1/comm) == systemd ]] || fail 'systemd VM required'
  [[ -r /proc/version_signature ]] || fail 'Ubuntu stock kernel signature required; manual review for custom kernels'
  check_kernel "$(uname -r)" "$(cat /proc/version_signature)"
  [[ $(uname -m) == x86_64 ]] || fail 'x86_64 required'
  grep -qw avx /proc/cpuinfo || fail 'AVX is not exposed to this VM'
  ip -4 -o addr show | awk '{print $4}' | cut -d/ -f1 | grep -Fxq "$ip" || fail 'inventory IP is not assigned to this VM'
  parent=$data; while [[ ! -d $parent ]]; do parent=$(dirname "$parent"); done
  available=$(df -Pk -- "$parent" | awk 'NR==2 {print $4}')
  (( available >= 10*1024*1024 )) || fail 'less than 10 GiB free on data filesystem'
  awk '/MemTotal:/ {exit !($2 >= 2*1024*1024)}' /proc/meminfo || fail 'at least 2 GiB visible RAM required'
  if [[ -e /etc/team3-mongodb/spec.sha256 ]]; then
    [[ $resume == 1 ]] || fail 'managed installation exists: reuse the original state directory with --resume'
  else
    ! command -v mongod >/dev/null || fail 'existing mongod binary: refuse automatic replacement'
    ! dpkg-query -W -f='${db:Status-Status}\n' mongodb-org-server 2>/dev/null | grep -qx installed || fail 'existing MongoDB package'
    [[ ! -e /etc/mongod.conf ]] || fail 'existing mongod.conf'
    for parent in "$data" /var/lib/mongodb /data/db; do
      if [[ -d $parent ]] && [[ -n $(find "$parent" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null) ]]; then
        fail "existing data at $parent; do not overwrite or migrate automatically"
      fi
    done
    ! ss -ltnH | awk '{print $4}' | grep -Eq ':27017$' || fail 'port 27017 already in use'
  fi
  printf 'PREFLIGHT_OK host=%s ip=%s dns=%s version=%s\n' "$(hostname)" "$ip" "$dns" "$version"
}

prepare_runtime_dirs() {
  # install -d with a restrictive umask can create intermediate dirs as root:0700.
  # Set EVERY relevant parent explicitly so the mongodb service can traverse it.
  install -d -o root -g mongodb -m 0750 /etc/mongodb
  install -d -o mongodb -g mongodb -m 0700 /etc/mongodb/pki "$1"
  install -d -o root -g root -m 0755 /etc/systemd/system/mongod.service.d
}

install_node() {
  local bundle=$1 version data ip dns marker spec expected actual ca_pub key_pub line resolved
  (( EUID == 0 )) || fail 'run this phase with sudo'
  [[ -d $bundle && ! -L $bundle ]] || fail 'bundle directory missing'
  read -r version data ip dns < "$bundle/settings.txt"
  precheck "$version" "$data" "$ip" "$dns" 1
  install -d -m 0755 /etc/team3-mongodb
  exec 9>/run/lock/team3-mongodb-install.lock
  flock -n 9 || fail 'another MongoDB installer is running'
  # Every restart must use identical configuration, TLS identity and membership secrets.
  spec=$(cd "$bundle"; sha256sum settings.txt mongod.conf hosts.txt ca.crt node.pem cluster.key | sha256sum | cut -d' ' -f1)
  marker=/etc/team3-mongodb/spec.sha256
  if [[ -f $marker ]]; then
    [[ $(cat "$marker") == "$spec" ]] || fail 'deployment inputs differ from original; no overwrite'
  else
    printf '%s\n' "$spec" > "$marker"
    chmod 0644 "$marker"
  fi
  if [[ -f /etc/team3-mongodb/configured ]]; then
    cmp -s "$bundle/mongod.conf" /etc/mongod.conf || fail 'managed config changed; manual review required'
    cmp -s "$bundle/node.pem" /etc/mongodb/pki/node.pem || fail 'managed TLS identity changed'
    cmp -s "$bundle/ca.crt" /etc/mongodb/pki/ca.crt || fail 'managed CA changed'
    cmp -s "$bundle/cluster.key" /etc/mongodb/cluster.key || fail 'managed membership key changed'
    [[ $(dpkg-query -W -f='${Version}' mongodb-org-server) == "$version" ]] || fail 'installed version differs'
    systemctl is-active --quiet mongod || fail 'managed mongod is stopped; inspect logs before restarting manually'
    printf 'ALREADY_CONFIGURED host=%s\n' "$(hostname)"; return
  fi
  # Fail closed during package installation: never allow package scripts to start an unsecured service.
  systemctl mask mongod.service
  apt-get update
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates curl gnupg openssl
  local key_tmp
  key_tmp=$(mktemp -d /tmp/team3-mongo-key.XXXXXXXX)
  curl --proto '=https' --tlsv1.2 --fail --silent --show-error --location \
    --connect-timeout 10 --max-time 60 https://pgp.mongodb.com/server-9.asc -o "$key_tmp/server.asc"
  expected=a8ec953b159e65ee2e327dde5506008d7693c3c6c3841beed421ec429bd10162
  actual=$(sha256sum "$key_tmp/server.asc" | cut -d' ' -f1)
  [[ $actual == "$expected" ]] || fail 'official signing key changed; review before updating the pinned checksum'
  gpg --batch --yes --dearmor --output "$key_tmp/server.gpg" "$key_tmp/server.asc"
  install -m 0644 "$key_tmp/server.gpg" /usr/share/keyrings/mongodb-server-9.gpg
  rm -f -- "$key_tmp/server.asc" "$key_tmp/server.gpg"; rmdir -- "$key_tmp"
  line='deb [arch=amd64 signed-by=/usr/share/keyrings/mongodb-server-9.gpg] https://repo.mongodb.org/apt/ubuntu resolute/mongodb-org/9.0 multiverse'
  if [[ -f /etc/apt/sources.list.d/mongodb-org-9.0.list ]]; then
    [[ $(cat /etc/apt/sources.list.d/mongodb-org-9.0.list) == "$line" ]] || fail 'existing APT source differs'
  else printf '%s\n' "$line" > /etc/apt/sources.list.d/mongodb-org-9.0.list; fi
  apt-get update
  apt-cache madison mongodb-org-server | awk '{print $3}' | grep -Fxq "$version" || fail 'pinned package version is unavailable'
  DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "mongodb-org-server=$version" mongodb-mongosh
  apt-mark hold mongodb-org-server mongodb-mongosh
  # A bundle supplies names only; existing conflicting non-managed aliases are never silently changed.
  while read -r resolved line; do
    if getent ahostsv4 "$line" >/dev/null; then
      [[ $(getent ahostsv4 "$line" | awk '{print $1}' | sort -u) == "$resolved" ]] || fail "DNS/hosts conflict for $line"
    else printf '\n%s %s # team3-mongodb\n' "$resolved" "$line" >> /etc/hosts; fi
  done < "$bundle/hosts.txt"
  openssl verify -CAfile "$bundle/ca.crt" -purpose sslserver "$bundle/node.pem" >/dev/null
  openssl x509 -in "$bundle/node.pem" -checkend 86400 -noout >/dev/null || fail 'TLS certificate expires in under a day'
  openssl x509 -in "$bundle/node.pem" -checkhost "$dns" -noout >/dev/null
  ca_pub=$(openssl x509 -in "$bundle/node.pem" -pubkey -noout | openssl pkey -pubin -outform DER | sha256sum)
  key_pub=$(openssl pkey -in "$bundle/node.pem" -pubout -outform DER | sha256sum)
  [[ $ca_pub == "$key_pub" ]] || fail 'TLS certificate/private key mismatch'
  prepare_runtime_dirs "$data"
  install -o mongodb -g mongodb -m 0644 "$bundle/ca.crt" /etc/mongodb/pki/ca.crt
  install -o mongodb -g mongodb -m 0400 "$bundle/node.pem" /etc/mongodb/pki/node.pem
  install -o mongodb -g mongodb -m 0400 "$bundle/cluster.key" /etc/mongodb/cluster.key
  install -o root -g root -m 0644 "$bundle/mongod.conf" /etc/mongod.conf
  printf '[Service]\nLimitNOFILE=64000\n' > /etc/systemd/system/mongod.service.d/team3-limits.conf
  chmod 0644 /etc/systemd/system/mongod.service.d/team3-limits.conf
  systemctl unmask mongod.service
  systemctl daemon-reload
  systemctl enable --now mongod
  local attempt
  for attempt in {1..30}; do
    if mongosh --quiet --host localhost --tls --tlsCAFile /etc/mongodb/pki/ca.crt \
       --eval 'quit(db.adminCommand({ping:1}).ok === 1 ? 0 : 2)' >/dev/null 2>&1; then
      printf '%s\n' "$spec" > /etc/team3-mongodb/configured
      chmod 0644 /etc/team3-mongodb/configured
      dpkg-query -W -f='${Package} ${Version}\n' mongodb-org-server mongodb-mongosh > /etc/team3-mongodb/package-versions.txt
      chmod 0644 /etc/team3-mongodb/package-versions.txt
      printf 'CONFIGURED host=%s version=%s\n' "$(hostname)" "$version"; return
    fi
    sleep 2
  done
  fail 'mongod failed TLS ping; inspect sudo journalctl -u mongod; no rollback or data deletion was performed'
}

cluster_phase() {
  local bundle=$1 phase=$2
  (( EUID == 0 )) || fail 'run cluster bootstrap with sudo'
  [[ -f /etc/team3-mongodb/configured ]] || fail 'node is not configured by this installer'
  export TEAM3_MONGO_BUNDLE=$bundle TEAM3_MONGO_PHASE=$phase
  mongosh --quiet --host localhost --tls --tlsCAFile /etc/mongodb/pki/ca.crt --file "$bundle/bootstrap.js"
}

main() {
  case ${1:-} in
    check) shift; precheck "$@" ;;
    install) [[ $# == 2 ]] || fail 'install requires bundle'; install_node "$2" ;;
    init|admin|verify) [[ $# == 2 ]] || fail 'bootstrap requires bundle'; cluster_phase "$2" "$1" ;;
    *) fail 'internal usage: check VERSION DATA IP DNS RESUME | install/init/admin/verify BUNDLE' ;;
  esac
}
if [[ ${BASH_SOURCE[0]:-$0} == "$0" ]]; then main "$@"; fi
