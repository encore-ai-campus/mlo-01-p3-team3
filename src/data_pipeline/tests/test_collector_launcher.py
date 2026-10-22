import importlib.util,json,tempfile,unittest,io
from pathlib import Path
from unittest.mock import patch
from contextlib import redirect_stdout

p=Path(__file__).resolve().parents[1]/'docs/server_setup/danawa_collector_launcher.py'
spec=importlib.util.spec_from_file_location('collector_launcher',p);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)


class CollectorLauncherTests(unittest.TestCase):
    def test_disabled_and_check_cannot_start_collector(self):
        with tempfile.TemporaryDirectory() as d:
            r=Path(d)
            for name in ('python','run_collector.py','categories.json','pagination.json'):(r/name).touch()
            config=dict(enabled=False,python=str(r/'python'),source=str(r),categories=str(r/'categories.json'),pagination_template=str(r/'pagination.json'),output=str(r/'runs'),source_commit='test')
            path=r/'control.json';path.write_text(json.dumps(config))
            with patch.object(m,'CONFIG',path),patch.object(m.subprocess,'run',side_effect=AssertionError('must not start')),redirect_stdout(io.StringIO()):
                self.assertEqual(m.main([]),0);self.assertEqual(m.main(['--check']),0)

    def test_existing_cli_and_gallery_download_are_reused(self):
        config=dict(python='/python',source='/source',categories='/config/categories.json',pagination_template='/config/pagination.json',output='/data/runs',workers=3,listing_interval=0,interval=1)
        args=m.command(config)
        self.assertEqual(args[:2],['/python',str(Path('/source')/'run_collector.py')])
        self.assertIn('--download-images',args)
        self.assertNotIn('--new-run',args)


if __name__=='__main__':unittest.main()
