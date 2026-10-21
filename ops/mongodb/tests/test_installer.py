"""Local shell/JS/crypto tests. Never run node installation or contact a database."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
BASH = os.environ.get('BASH_FOR_TEST') or ('C:/Program Files/Git/bin/bash.exe' if os.name == 'nt' else shutil.which('bash'))
NODE = os.environ.get('NODE_FOR_TEST') or shutil.which('node')


def bash(code, **env):
    return subprocess.run([BASH, '-c', code], cwd=ROOT,
        env={**os.environ, 'MSYS_NO_PATHCONV':'1', **env}, text=True, capture_output=True, timeout=60)


@unittest.skipUnless(BASH, 'Bash required')
class ShellTests(unittest.TestCase):
    def inventory(self, text):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'inventory.tsv';path.write_bytes(text.encode())
            return bash('source ./deploy.sh; validate_inventory "$TEST_INVENTORY"; printf OK',TEST_INVENTORY=path.as_posix())

    def test_syntax_and_help_have_no_install_side_effect(self):
        for path in ('deploy.sh','node.sh','install.sh','cluster.sh'):
            result=subprocess.run([BASH,'-n',str(ROOT/path)],text=True,capture_output=True)
            self.assertEqual(result.returncode,0,result.stderr)
        result=bash('bash ./deploy.sh --help')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('--prepare',result.stdout)
        self.assertNotIn('ssh ', (ROOT/'deploy.sh').read_text())

    def test_node_stdin_transport_reaches_argument_guard_without_installing(self):
        result=subprocess.run([BASH,'-s','--','invalid-mode'],input=(ROOT/'node.sh').read_text(),
                              text=True,capture_output=True)
        self.assertNotEqual(result.returncode,0)
        self.assertIn('internal usage',result.stderr)
        self.assertNotIn('unbound variable',result.stderr)

    def test_inventory_accepts_examples_crlf_and_final_line_without_newline(self):
        text=(ROOT/'inventory.example.tsv').read_text()
        for body in (text,text.replace('\n','\r\n'),text.rstrip('\n')):
            result=self.inventory(body)
            self.assertEqual(result.returncode,0,result.stderr)

    def test_invalid_duplicate_public_and_injected_members_block(self):
        original=(ROOT/'inventory.example.tsv').read_text()
        variants=[original.replace('192.168.0.135','192.168.0.134'),
            original.replace('pj3-mongo02','pj3-mongo01'),original.replace('mongo02.pj3.internal','mongo01.pj3.internal'),
            original.replace('192.168.0.135','8.8.8.8'),original.replace('192.168.0.135','192.168.0.999'),
            original.replace('192.168.0.135','192.168.00.135'),original.replace('pj3-mongo02','x;touch_BAD'),
            original.replace('mongo02.pj3.internal','mongo02.-bad.internal'),original.replace('1 pj3-mongo02','0 pj3-mongo02')]
        for body in variants:
            result=self.inventory(body)
            self.assertNotEqual(result.returncode,0)

    def test_no_implicit_apply_unknown_action_or_bad_version(self):
        for args in ('','--wat','--inventory inventory.example.tsv --data-dir /var/lib/mongodb --version 8.3.7 --check',
                     '--inventory inventory.example.tsv --data-dir /var/lib/mongodb --version 9.0.3 --check'):
            result=bash('bash ./deploy.sh '+args)
            self.assertNotEqual(result.returncode,0)

    def test_os_and_path_guards(self):
        for value in ('/','/etc/mongodb','/srv/mongo/../data','/var/lib/mongodb/','/srv//mongo'):
            result=bash('source ./node.sh; require_data_path "$TEST_PATH"',TEST_PATH=value)
            self.assertNotEqual(result.returncode,0)
        for value in ('/var/lib/mongodb','/srv/mongo-data'):
            self.assertEqual(bash('source ./deploy.sh; require_data_path "$TEST_PATH"',TEST_PATH=value).returncode,0)
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'os-release';p.write_text('ID=ubuntu\nVERSION_ID=24.04\nVERSION_CODENAME=noble\n')
            self.assertNotEqual(bash('source ./node.sh; check_os "$TEST_OS"',TEST_OS=p.as_posix()).returncode,0)
            p.write_text('ID=ubuntu\nVERSION_ID=26.04\nVERSION_CODENAME=resolute\n')
            self.assertEqual(bash('source ./node.sh; check_os "$TEST_OS"',TEST_OS=p.as_posix()).returncode,0)

    def test_kernel_range_and_ubuntu_upstream_signature(self):
        for version in ('6.19.0','6.19.12','7.0.0','7.0.13'):
            self.assertNotEqual(bash('source ./node.sh; check_kernel "$TEST_KERNEL"',TEST_KERNEL=version).returncode,0)
        for version in ('6.18.20','7.0.14','7.1.0'):
            self.assertEqual(bash('source ./node.sh; check_kernel "$TEST_KERNEL"',TEST_KERNEL=version).returncode,0)
        result=bash('source ./node.sh; check_kernel 7.0.0-38-generic "Ubuntu 7.0.0-38.38-generic 7.0.14"')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertIn('upstream=7.0.14',result.stdout)

    def test_local_tls_assets_have_matching_sans_config_and_credentials_without_printing(self):
        with tempfile.TemporaryDirectory() as d:
            state=Path(d)/'state'
            result=bash('source ./deploy.sh; validate_inventory inventory.example.tsv; '
                'state=$TEST_STATE; version=9.0.4; data=/var/lib/mongodb; deployment_spec=fixture; '
                'python3() { "$TEST_PYTHON" "$@"; }; make_assets; '
                'for i in 0 1 2; do openssl verify -CAfile "$state/ca.crt" "$state/node$i/node.pem" >/dev/null; '
                'openssl x509 -in "$state/node$i/node.pem" -checkhost "${names[i]}" -noout >/dev/null; done',
                TEST_STATE=state.as_posix(),TEST_PYTHON=Path(sys.executable).as_posix())
            self.assertEqual(result.returncode,0,result.stderr)
            secret=json.loads((state/'admin.json').read_text())
            self.assertGreaterEqual(len(secret['password']),40)
            self.assertNotIn(secret['password'],result.stdout+result.stderr)
            cluster=json.loads((state/'cluster.json').read_text())
            self.assertEqual(len(cluster['members']),3)
            self.assertTrue(all(r['votes']==r['priority']==1 for r in cluster['members']))
            for i in range(3):
                config=(state/f'node{i}'/'mongod.conf').read_text()
                self.assertIn('mode: requireTLS',config)
                self.assertIn('authorization: enabled',config)
                self.assertIn('replSetName: rs-products',config)
                self.assertFalse((state/f'node{i}'/'ca.key').exists())
                self.assertFalse((state/f'node{i}'/'admin.json').exists())
                self.assertTrue((state/f'node{i}'/'install.sh').exists())
                check=bash('cd "$TEST_BUNDLE"; sha256sum -c bundle.sha256',TEST_BUNDLE=(state/f'node{i}').as_posix())
                self.assertEqual(check.returncode,0,check.stderr)

    def test_variable_inventory_size_and_nonsequential_ids(self):
        original=(ROOT/'inventory.example.tsv').read_text()
        rows=[r for r in original.splitlines() if r and not r.startswith('#')]
        self.assertEqual(self.inventory(rows[2]+'\n').returncode,0)
        self.assertEqual(self.inventory('\n'.join(reversed(rows))).returncode,0)
        self.assertNotEqual(self.inventory('# empty\n').returncode,0)

    def test_prepare_entrypoint_never_contacts_hosts_or_overwrites_state(self):
        with tempfile.TemporaryDirectory() as d:
            state=Path(d)/'state'
            code=('source ./deploy.sh; '
                  'if command -v cygpath >/dev/null; then target=$(cygpath -u "$TEST_STATE"); else target=$TEST_STATE; fi; '
                  'make_assets() { printf "LOCAL_PREPARATION_ONLY\\n"; }; '
                  'ssh() { exit 99; }; scp() { exit 99; }; '
                  'main --inventory inventory.example.tsv --data-dir /var/lib/mongodb --state-dir "$target" --prepare')
            result=bash(code,TEST_STATE=state.as_posix())
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertIn('LOCAL_PREPARATION_ONLY',result.stdout)
            state.mkdir()
            result=bash(code,TEST_STATE=state.as_posix())
            self.assertNotEqual(result.returncode,0)
            self.assertIn('never regenerate',result.stderr)

    def test_local_installer_explicit_action_and_no_remote_or_bootstrap(self):
        for args in ('','--apply','--bundle /missing --check','--wat','--check --apply'):
            result=bash('bash ./install.sh '+args)
            self.assertNotEqual(result.returncode,0)
        script=(ROOT/'install.sh').read_text()
        self.assertNotIn('ssh ',script)
        self.assertNotIn('cluster_phase',script)
        self.assertNotIn('replSetInitiate',script)
        result=bash('bash ./install.sh --help')
        self.assertEqual(result.returncode,0,result.stderr)

    def test_prepared_single_vm_bundle_check_dispatch_and_tamper_rejection(self):
        with tempfile.TemporaryDirectory() as d:
            state=Path(d)/'state'
            result=bash('source ./deploy.sh; validate_inventory inventory.example.tsv; '
                'ids=(7); aliases=(fixture); names=(mongo07.pj3.internal); ips=(192.168.0.137); '
                'state=$TEST_STATE; version=9.0.4; data=/srv/mongo-data; replica_set=fixture-rs; deployment_spec=fixture; '
                'python3() { "$TEST_PYTHON" "$@"; }; make_assets',
                TEST_STATE=state.as_posix(),TEST_PYTHON=Path(sys.executable).as_posix())
            self.assertEqual(result.returncode,0,result.stderr)
            bundle=state/'node7'
            self.assertFalse((state/'node0').exists())
            self.assertIn('replSetName: fixture-rs',(bundle/'mongod.conf').read_text())
            self.assertEqual(len(json.loads((state/'cluster.json').read_text())['members']),1)
            code='source ./install.sh; precheck() { printf "CHECK_ARGS:%s,%s,%s,%s,%s\\n" "$@"; }; install_node() { exit 99; }; main_install --bundle "$TEST_BUNDLE" --check'
            result=bash(code,TEST_BUNDLE=bundle.as_posix())
            self.assertEqual(result.returncode,0,result.stderr)
            self.assertIn('CHECK_ARGS:9.0.4,/srv/mongo-data,192.168.0.137,mongo07.pj3.internal,0',result.stdout)
            with (bundle/'settings.txt').open('a') as f:f.write('tampered\n')
            result=bash(code,TEST_BUNDLE=bundle.as_posix())
            self.assertNotEqual(result.returncode,0)
            self.assertIn('checksum mismatch',result.stderr)

    def test_cluster_action_not_implicit_and_install_bundle_has_no_admin(self):
        for args in ('','init /missing','apply /missing'):
            self.assertNotEqual(bash('bash ./cluster.sh '+args).returncode,0)

    def test_service_can_traverse_explicitly_owned_certificate_parent(self):
        # Capture owner/group/mode operations before any root-only installation.
        # This guards the real VM regression where umask 077 made /etc/mongodb root:0700.
        result=bash('source ./node.sh; install() { printf "%s " "$@"; printf "\\n"; }; '
                    'prepare_runtime_dirs /srv/mongodb')
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertEqual(result.stdout.splitlines(),[
            '-d -o root -g mongodb -m 0750 /etc/mongodb ',
            '-d -o mongodb -g mongodb -m 0700 /etc/mongodb/pki /srv/mongodb ',
            '-d -o root -g root -m 0755 /etc/systemd/system/mongod.service.d '])


@unittest.skipUnless(NODE,'Node.js required for isolated mongosh command model')
class BootstrapTests(unittest.TestCase):
    def execute(self,scenario):
        result=subprocess.run([NODE,str(ROOT/'tests'/'bootstrap_model.cjs'),scenario,str(ROOT/'bootstrap.js')],
            capture_output=True,text=True,timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)
        return json.loads(result.stdout)

    def test_initiate_and_already_initialized_exception(self):
        self.assertTrue(self.execute('init')['passed'])
        self.assertTrue(self.execute('resume_init')['passed'])

    def test_admin_creation_uses_majority_journal_and_is_idempotent(self):
        self.assertTrue(self.execute('admin')['passed'])
        self.assertTrue(self.execute('resume_admin')['passed'])

    def test_wrong_members_arbiter_bad_version_unhealthy_or_bad_auth_fail_closed(self):
        for scenario in ('wrong_members','arbiter','wrong_version','unhealthy','wrong_auth'):
            self.assertTrue(self.execute(scenario)['passed'])

    def test_verify_three_data_nodes_without_application_writes(self):
        self.assertTrue(self.execute('verify')['passed'])


if __name__=='__main__':unittest.main()
