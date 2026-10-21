#!/usr/bin/env bash
# Explicit, separate operation; installation never calls this script.
set -euo pipefail
set +x
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/node.sh"
[[ $# == 2 && $1 =~ ^(init|admin|verify)$ ]] || fail 'Usage: sudo bash cluster.sh init|admin|verify /secure/nodeID'
[[ -f $2/admin.json && -f $2/cluster.json ]] || fail 'explicit admin credential and cluster config required; installation bundle alone is insufficient'
cluster_phase "$2" "$1"
