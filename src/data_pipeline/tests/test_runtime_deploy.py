"""Validate source trust boundaries before any privileged deployment action."""
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest

from product_pipeline.contracts import digest

path=Path(__file__).parents[1]/'docs/server_setup/pipeline_runtime_deploy.py'
spec=importlib.util.spec_from_file_location('pipeline_runtime_deploy',path)
installer=importlib.util.module_from_spec(spec);spec.loader.exec_module(installer)


class RuntimeArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.archive=Path(self.tmp.name)/'source.tar.gz'

    def make(self, entries=None, *, duplicate=False, corrupt_proof=False, symlink=False):
        entries=entries or {'product_pipeline/__init__.py':b'# package\n'}
        proofs={name:dict(bytes=len(data),sha256=hashlib.sha256(data).hexdigest()) for name,data in entries.items()}
        if corrupt_proof:proofs[next(iter(proofs))]['sha256']='0'*64
        manifest=dict(kind='PIPELINE_SOURCE_RELEASE',contract_version=1,files=proofs)
        manifest['release_id']=digest(manifest)
        payload={**entries,'release.json':json.dumps(manifest).encode()}
        with tarfile.open(self.archive,'w:gz') as stream:
            for name,data in payload.items():
                member=tarfile.TarInfo(name);member.size=len(data)
                stream.addfile(member,io.BytesIO(data))
            if duplicate:
                name=next(iter(entries));member=tarfile.TarInfo(name);member.size=len(entries[name])
                stream.addfile(member,io.BytesIO(entries[name]))
            if symlink:
                member=tarfile.TarInfo('link');member.type=tarfile.SYMTYPE;member.linkname='/etc/passwd'
                stream.addfile(member)
        return hashlib.sha256(self.archive.read_bytes()).hexdigest(),manifest['release_id']

    def test_valid_archive_returns_exact_verified_files_without_extracting(self):
        sha,release=self.make()
        manifest,files=installer.inspect_archive(self.archive,sha,release)
        self.assertEqual(manifest['release_id'],release)
        self.assertEqual(files['product_pipeline/__init__.py'],b'# package\n')
        self.assertFalse((Path(self.tmp.name)/'product_pipeline').exists())

    def test_archive_hash_or_release_identity_mismatch_is_rejected(self):
        sha,release=self.make()
        for expected_sha,expected_release in [('0'*64,release),(sha,'0'*64)]:
            with self.assertRaises(ValueError):installer.inspect_archive(self.archive,expected_sha,expected_release)

    def test_traversal_absolute_and_backslash_members_are_rejected(self):
        for name in ['../escape.py','/absolute.py','dir\\escape.py']:
            sha,release=self.make({name:b'not executed'})
            with self.assertRaisesRegex(ValueError,'UNSAFE_SOURCE_ARCHIVE'):
                installer.inspect_archive(self.archive,sha,release)

    def test_duplicate_names_and_symlinks_are_rejected(self):
        for kwargs in [dict(duplicate=True),dict(symlink=True)]:
            sha,release=self.make(**kwargs)
            with self.assertRaisesRegex(ValueError,'UNSAFE_SOURCE_ARCHIVE'):
                installer.inspect_archive(self.archive,sha,release)

    def test_manifest_claim_is_not_a_substitute_for_file_digest(self):
        sha,release=self.make(corrupt_proof=True)
        with self.assertRaisesRegex(ValueError,'SOURCE_FILE_CHANGED'):
            installer.inspect_archive(self.archive,sha,release)


if __name__=='__main__':unittest.main()
