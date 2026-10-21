#!/usr/bin/env bash
# Run this SAME script on each target VM. No SSH and no cluster bootstrap.
set -euo pipefail
set +x
umask 077
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
source "$SCRIPT_DIR/node.sh"
main_install() {
  local mode= bundle= arg version data ip dns extra
  while (( $# )); do
    arg=$1; shift
    case $arg in
      --check|--apply|--resume) [[ -z $mode ]] || fail 'choose exactly one action'; mode=$arg ;;
      --bundle) (( $# )) || fail 'missing bundle'; bundle=$1; shift ;;
      --help|-h) printf 'Usage: bash install.sh --bundle /secure/nodeID --check|--apply|--resume\nOnly this VM is affected. No replica initialization. --apply/--resume require sudo.\n'; return ;;
      *) fail "unknown argument: $arg" ;;
    esac
  done
  [[ -n $mode && -d $bundle && ! -L $bundle ]] || fail 'explicit action and existing bundle required'
  bundle=$(cd -- "$bundle" && pwd)
  [[ -f $bundle/bundle.sha256 ]] || fail 'bundle manifest missing'
  (cd "$bundle"; sha256sum --check --status bundle.sha256) || fail 'bundle checksum mismatch'
  read -r version data ip dns extra < "$bundle/settings.txt"
  [[ -n $dns && -z ${extra:-} ]] || fail 'invalid settings'
  [[ $ip =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ && $dns =~ ^[a-z][a-z0-9.-]+$ ]] || fail 'invalid node identity'
  local resume=0
  [[ $mode == --resume ]] && resume=1
  precheck "$version" "$data" "$ip" "$dns" "$resume"
  [[ $mode != --check ]] || { printf 'CHECK_COMPLETE: local VM only; no changes.\n'; return; }
  (( EUID == 0 )) || fail 'run sudo bash install.sh with the same arguments; no password is read by this script'
  install_node "$bundle"
  printf 'NODE_INSTALL_COMPLETE: replica set is NOT initialized by this command.\n'
}
if [[ ${BASH_SOURCE[0]} == "$0" ]]; then main_install "$@"; fi
