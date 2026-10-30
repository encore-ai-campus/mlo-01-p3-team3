"""The server launcher (crawler-optimization/launcher.md) on synthetic runs.

The collector, prune and the listing check are a fake product_collector package run as real
subprocesses from a temporary release folder; supervise runs in this process and time is injected.
No network, NiFi or server."""
from datetime import datetime, timedelta, timezone
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import patch

from product_collector import launcher as launcher_module
from product_collector.checkpoint import abandon_run, writer_lock
from product_collector.launcher import Launcher, main as launcher_main
from product_collector.prune import prune_runs
from test_prune import seal, run as synthetic_run

FAKE_COLLECTOR = textwrap.dedent('''
    """Fake collector: does what scenario.json in the release folder says."""
    import json, sys
    from pathlib import Path

    scenario = json.loads((Path(__file__).resolve().parents[1] / 'scenario.json').read_text(encoding='utf-8'))
    args = sys.argv[1:]
    if '--check-pagination' in args:
        code = scenario.get('check', 0)
        print(json.dumps({'check': 'pagination', 'status': 'ok' if code == 0 else 'failed'}))
        raise SystemExit(code)
    output = Path(args[args.index('--output') + 1])
    output.mkdir(parents=True, exist_ok=True)
    calls = output.parent / 'collector_calls.jsonl'
    with calls.open('a', encoding='utf-8') as stream:
        stream.write(json.dumps(args) + '\\n')
    plan = scenario['collector']
    if plan.get('exit') == 3:
        print(json.dumps({'result': 'not_started', 'exit_code': 3, 'error': 'ValueError: settings differ'}))
        raise SystemExit(3)
    active = output / 'active_run.json'
    if '--new-run' in args or not active.exists():
        directory = output / plan.get('job', 'job-new')
        directory.mkdir(parents=True, exist_ok=True)
        config = {'total_limit': plan.get('total_limit'), 'collect_details': True,
                  'categories': [{'category_id': '1', 'start_page': 1, 'max_pages': None}]}
        (directory / 'run_config.json').write_text(json.dumps({'job_id': directory.name, 'config': config}))
    else:
        directory = Path(json.loads(active.read_text())['directory'])
    job = directory.name
    active.write_text(json.dumps({'directory': str(directory.resolve())}))
    status = plan.get('status', 'completed')
    (directory / 'checkpoint_state.json').write_text(json.dumps(
        {'job_id': job, 'status': status, 'updated_at': plan.get('updated_at', '2026-11-02T00:00:00+00:00'),
         'error': plan.get('error')}))
    report = {'job_id': job, 'result': plan.get('result', status), 'started_at': plan.get('started_at',
              '2026-11-01T00:00:00+00:00'), 'limits': {'requests': None}, 'errors': [], 'stop_reason': None}
    report.update(plan.get('report', {}))
    (directory / 'run_report.json').write_text(json.dumps(report))
    if plan.get('media', True):
        (directory / 'media').mkdir(exist_ok=True)
        (directory / 'media' / 'products_with_local_images.jsonl').write_text('')
        (directory / 'media' / 'download_report.json').write_text(json.dumps({'finished_at': '2026-11-02T00:00:00+00:00'}))
    if status == 'completed':
        active.unlink()
    if plan.get('line', True):
        code = plan.get('exit', 0)
        print(json.dumps({'output': str(directory.resolve()), 'job_id': plan.get('line_job', job),
                          'result': report['result'], 'exit_code': code, 'error': plan.get('error')}))
    raise SystemExit(plan.get('exit', 0))
''')

FAKE_PRUNE = textwrap.dedent('''
    import json, sys
    from pathlib import Path
    args = sys.argv[1:]
    output = Path(args[args.index('--output') + 1])
    with (output.parent / 'prune_calls.jsonl').open('a', encoding='utf-8') as stream:
        stream.write(json.dumps(args) + '\\n')
    planned = output.parent / 'prune_report.json'
    print(planned.read_text(encoding='utf-8') if planned.exists() else json.dumps({'deleted': [], 'kept': {}}))
''')

START = datetime(2026, 11, 1, tzinfo=timezone.utc)


class Clock:
    def __init__(self, value=START):
        self.value = value

    def __call__(self):
        return self.value

    def advance(self, **delta):
        self.value += timedelta(**delta)


class Harness:
    """A launcher root with a control file, a fake release and supervise run in this process."""

    def __init__(self, root, **control):
        self.root = Path(root)
        self.source = self.root / 'release' / 'commerce_crawler'
        package = self.source / 'product_collector'
        package.mkdir(parents=True)
        (package / '__init__.py').write_text('')
        (package / '__main__.py').write_text(FAKE_COLLECTOR, encoding='utf-8')
        (package / 'prune.py').write_text(FAKE_PRUNE, encoding='utf-8')
        (self.root / 'config').mkdir()
        (self.root / 'config' / 'danawa_pagination.local.json').write_text('{}')
        categories = self.source / 'config' / 'danawa_categories_71.json'
        categories.parent.mkdir()
        categories.write_text('[]')
        self.output = self.root / 'runs'
        self.control_path = self.root / 'config' / 'collector-control.json'
        value = json.loads(Path(__file__).resolve().parents[1].joinpath(
            'deploy', 'collector-control.example.json').read_text(encoding='utf-8'))
        value.update(enabled=True, source=str(self.source), source_commit='abc123', python=sys.executable,
                     categories=str(categories), pagination_template=str(self.root / 'config' / 'danawa_pagination.local.json'),
                     output=str(self.output), handoff_since='2026-10-01T00:00:00+00:00')
        value.update(control)
        self.write_control(value)
        self.clock = Clock()
        self.free = 100 * 1024 ** 3
        self.scenario({'collector': {}})

    def write_control(self, value):
        self.control = value
        self.control_path.write_text(json.dumps(value), encoding='utf-8')

    def scenario(self, value):
        (self.source / 'scenario.json').write_text(json.dumps(value), encoding='utf-8')

    def launcher(self):
        def spawn(kind, attempt):
            Launcher(self.control_path, now=self.clock, disk_free=lambda path: self.free).supervise(
                kind, attempt, poll=0.05)
        return Launcher(self.control_path, now=self.clock, spawner=spawn, disk_free=lambda path: self.free)

    def tick(self):
        with patch('builtins.print') as printed:
            code = self.launcher().tick()
        return code, json.loads(printed.call_args_list[-1].args[0])

    def state(self):
        return json.loads((self.root / 'launcher' / 'state.json').read_text(encoding='utf-8'))

    def calls(self, name='collector_calls.jsonl'):
        path = self.root / name
        return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []

    def handoff(self, job):
        path = self.output / 'handoff' / f'{job}.json'
        return json.loads(path.read_text(encoding='utf-8')) if path.exists() else None


class ConfigTests(unittest.TestCase):
    def test_contract_version_1_and_missing_keys_are_refused_with_exit_3(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            h.write_control({**h.control, 'contract_version': 1})
            code, line = h.tick()
            self.assertEqual((code, line['status']), (3, 'CONFIG_INVALID'))
            control = dict(h.control, contract_version=2)
            control.pop('image_store_config')
            h.write_control(control)
            self.assertEqual(h.tick()[0], 3)
            self.assertEqual(h.calls(), [])

    def test_disabled_does_nothing(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root, enabled=False)
            self.assertEqual(h.tick(), (0, h.tick()[1]))
            self.assertEqual(h.tick()[1]['status'], 'DISABLED')
            self.assertEqual(h.calls(), [])

    def test_check_reports_the_installation_without_network(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            with patch('builtins.print') as printed:
                self.assertEqual(launcher_main(['--control', str(h.control_path), '--check']), 0)
            line = json.loads(printed.call_args_list[-1].args[0])
            self.assertEqual((line['status'], line['network_requests']), ('CONFIGURED', 0))
            Path(h.control['pagination_template']).unlink()
            with patch('builtins.print') as printed:
                self.assertEqual(launcher_main(['--control', str(h.control_path), '--check']), 3)
            self.assertIn('pagination_template', json.loads(printed.call_args_list[-1].args[0])['missing'])

    def test_shim_runs_the_release_named_in_the_control_file(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            # The fake release has no launcher: point the shim at this source tree instead.
            h.write_control({**h.control, 'source': str(Path(__file__).resolve().parents[1])})
            shim = Path(__file__).resolve().parents[1] / 'deploy' / 'collector_launcher.py'
            done = subprocess.run([sys.executable, str(shim), '--control', str(h.control_path), 'status'],
                                  capture_output=True, text=True, timeout=60)
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertEqual(json.loads(done.stdout.splitlines()[-1])['status'], 'OK')


class ScheduleTests(unittest.TestCase):
    def test_first_run_is_handed_over_and_the_next_waits_seven_days_and_a_rest(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            h.scenario({'collector': {'job': 'w1'}})
            code, line = h.tick()
            self.assertEqual((code, line['status'], line['kind']), (0, 'STARTED', 'new'))
            self.assertIn('--new-run', h.calls()[0])
            self.assertIn('--download-images', h.calls()[0])
            self.assertNotIn('--image-store-config', h.calls()[0])
            handoff = h.handoff('w1')
            self.assertEqual((handoff['schema_version'], handoff['job_id'], handoff['crawler_commit']), (1, 'w1', 'abc123'))
            self.assertEqual(handoff['options'], {'detail_refresh': 'all', 'image_store': False, 'quality_gate': 'warn'})
            state = h.state()
            self.assertEqual(state['runs']['w1']['started_at'], '2026-11-01T00:00:00+00:00')
            self.assertEqual(state['attempt']['outcome'], 'completed')
            self.assertFalse((h.output / 'active_run.json').exists())
            h.clock.advance(days=3)
            line = h.tick()[1]
            self.assertEqual((line['status'], line['next_action_at']), ('WAIT', '2026-11-08T00:00:00+00:00'))
            self.assertEqual(len(h.calls('prune_calls.jsonl')), 2)  # prune runs on every tick
            h.clock.advance(days=4)
            h.scenario({'collector': {'job': 'w2'}})
            self.assertEqual(h.tick()[1]['status'], 'STARTED')

    def test_a_long_run_gets_a_rest_before_the_next(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            h.scenario({'collector': {'job': 'w1'}})
            launcher = h.launcher()
            launcher.load()
            with launcher.state() as state:
                state['last_completed'] = {'job_id': 'w0', 'started_at': '2026-10-20T00:00:00+00:00',
                                           'finished_at': '2026-10-31T12:00:00+00:00'}
            line = h.tick()[1]
            self.assertEqual((line['status'], line['next_action_at']), ('WAIT', '2026-11-01T12:00:00+00:00'))

    def test_release_start_now_skips_the_schedule_once(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            h.scenario({'collector': {'job': 'w1'}})
            h.tick()
            h.clock.advance(hours=1)
            self.assertEqual(h.tick()[1]['status'], 'WAIT')
            with patch('builtins.print'):
                h.launcher().release(start_now=True)
            h.scenario({'collector': {'job': 'w2'}})
            self.assertEqual(h.tick()[1]['status'], 'STARTED')
            self.assertNotIn('start_now', h.state())

    def test_hold_stops_starting_but_not_pruning(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            (h.root / 'HOLD').write_text('')
            self.assertEqual(h.tick()[1]['status'], 'HOLD')
            self.assertEqual(h.calls(), [])
            self.assertEqual(len(h.calls('prune_calls.jsonl')), 1)


class OutcomeTests(unittest.TestCase):
    def interrupted_run(self, h, job='r1', **plan):
        """A first attempt that leaves job r1 active with the given outcome."""
        h.scenario({'collector': {'job': job, **plan}})
        return h.tick()

    def test_blocked_backs_off_6_then_24_hours_and_then_waits_for_the_operator(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            blocked = {'status': 'partial', 'exit': 2, 'report': {'stop_reason': 'http_429'}}
            self.interrupted_run(h, **blocked)
            self.assertEqual(h.state()['runs']['r1']['next_at'], '2026-11-01T06:00:00+00:00')
            h.clock.advance(hours=5)
            self.assertEqual(h.tick()[1]['status'], 'WAIT')
            h.clock.advance(hours=1)
            h.scenario({'collector': blocked})
            self.assertEqual(h.tick()[1]['kind'], 'resume')
            self.assertNotIn('--new-run', h.calls()[-1])
            self.assertEqual(h.state()['runs']['r1']['next_at'], '2026-11-02T06:00:00+00:00')
            h.clock.advance(hours=24)
            h.tick()
            line = h.tick()[1]
            self.assertEqual((line['status'], line['reason']), ('OPERATOR_WAIT', 'blocked_3_times'))
            with patch('builtins.print'):
                h.launcher().release()
            h.scenario({'collector': {}})
            self.assertEqual(h.tick()[1]['kind'], 'resume')
            self.assertTrue(h.handoff('r1'))

    def test_image_partial_quality_failed_budget_and_not_started_wait_for_the_operator(self):
        cases = [({'status': 'partial', 'exit': 2, 'report': {'image_download': {'result': 'partial',
                                                                               'verification': 'completed'}}}, 'image'),
                 ({'status': 'partial', 'result': 'quality_failed', 'exit': 2}, 'quality_failed'),
                 ({'status': 'partial', 'exit': 2, 'report': {'stop_reason': 'request_limit'}}, 'budget'),
                 ({'exit': 3}, 'not_started'),
                 ({'status': 'partial', 'exit': 2, 'line_job': 'another'}, 'job_mismatch')]
        for plan, reason in cases:
            with self.subTest(reason=reason), tempfile.TemporaryDirectory() as root:
                h = Harness(root)
                self.interrupted_run(h, **plan)
                line = h.tick()[1]
                self.assertEqual((line['status'], line['reason']), ('OPERATOR_WAIT', reason))
                self.assertEqual(len(h.calls()), 1)

    def test_listing_errors_template_suspicion_and_detail_failures(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            listing = {'status': 'partial', 'exit': 2, 'report': {
                'errors': [{'stage': 'listing', 'reason': 'timeout'}],
                'categories': [{'category_id': '1', 'status': 'request_failed'}]}}
            self.interrupted_run(h, **listing)
            self.assertEqual(h.state()['runs']['r1']['next_at'], '2026-11-01T01:00:00+00:00')
            h.clock.advance(hours=1)
            h.scenario({'collector': listing})
            h.tick()
            self.assertEqual(h.tick()[1]['reason'], 'listing_after_retry')
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            self.interrupted_run(h, status='partial', exit=2, report={
                'errors': [{'stage': 'listing'}, {'stage': 'listing'}],
                'categories': [{'status': 'parse_failed'}, {'status': 'no_products'}]})
            self.assertEqual(h.tick()[1]['reason'], 'listing_template')
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            self.interrupted_run(h, status='partial', exit=2, report={'detail_failures': {'passed': False}})
            self.assertEqual(h.state()['runs']['r1']['last_cause'], 'detail')

    def test_a_resume_killed_while_restoring_is_a_crash_not_the_old_partial(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            self.interrupted_run(h, status='partial', exit=2, report={'stop_reason': 'http_429'})
            h.clock.advance(hours=6)
            # Killed before saving anything: the files still say partial/http_429 from the first attempt.
            h.scenario({'collector': {'status': 'partial', 'line': False, 'exit': 137,
                                      'report': {'stop_reason': 'http_429'}}})
            h.tick()
            runs = h.state()['runs']['r1']
            self.assertEqual((h.state()['attempt']['outcome'], runs['last_cause']), ('crashed', 'crashed'))

    def test_a_changed_output_root_does_not_hide_an_unfinished_run(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            self.interrupted_run(h, status='interrupted', result='interrupted', exit=130)
            h.write_control({**h.control, 'output': str(h.root / 'runs2')})
            line = h.tick()[1]
            self.assertEqual((line['status'], line['reason']), ('OPERATOR_WAIT', 'fixed_options_changed'))
            self.assertEqual(len(h.calls()), 1)

    def test_interrupted_resumes_at_the_next_tick_and_crashes_twice_wait(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            self.interrupted_run(h, status='interrupted', result='interrupted', exit=130)
            self.assertEqual(h.state()['attempt']['outcome'], 'interrupted')
            h.clock.advance(minutes=60)
            h.scenario({'collector': {'status': 'running', 'line': False, 'exit': 137}})
            self.assertEqual(h.tick()[1]['kind'], 'resume')
            self.assertEqual(h.state()['attempt']['outcome'], 'crashed')
            self.assertEqual(h.tick()[1]['status'], 'WAIT')
            h.clock.advance(hours=1)
            h.tick()
            self.assertEqual(h.tick()[1]['reason'], 'crashed_twice')

    def test_failed_retries_once_and_the_same_error_twice_waits(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            failed = {'status': 'failed', 'result': 'failed', 'exit': 1, 'error': 'OSError: disk'}
            self.interrupted_run(h, **failed)
            h.clock.advance(hours=1)
            h.scenario({'collector': failed})
            h.tick()
            self.assertEqual(h.tick()[1]['reason'], 'failed_twice_same_error')

    def test_lost_supervisor_attempt_is_judged_from_its_log_and_files(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            launcher = h.launcher()
            launcher.load()
            # The collector finished and printed its line, then the supervisor died before judging.
            env = dict(os.environ, PYTHONPATH=str(h.source))
            h.scenario({'collector': {'job': 'w1'}})
            log = h.root / 'logs' / 'new' / 'a.log'
            log.parent.mkdir(parents=True)
            with log.open('wb') as stream:
                subprocess.run(launcher.collector_argv('new'), cwd=h.source, env=env, stdout=stream, check=True)
            with launcher.state() as state:
                state['attempt'] = {'id': 'a', 'kind': 'new', 'started_at': '2026-11-01T00:00:00+00:00',
                                    'log': str(log), 'job_id': None, 'run_dir': None}
            # The pointer is gone (the collector dropped it) and the attempt never noted its folder:
            # the result line's output names the run.
            self.assertFalse((h.output / 'active_run.json').exists())
            line = h.tick()[1]
            state = h.state()
            self.assertEqual((state['attempt']['outcome'], state['attempt']['recovered']), ('completed', True))
            self.assertTrue(h.handoff('w1'))
            self.assertEqual(line['status'], 'WAIT')

    def test_a_completed_run_still_named_by_the_active_pointer_is_released(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            h.scenario({'collector': {'job': 'w1'}})
            h.tick()
            # As if the collector died after saving completed and before dropping the pointer.
            (h.output / 'active_run.json').write_text(json.dumps({'directory': str((h.output / 'w1').resolve())}))
            launcher = h.launcher()
            launcher.load()
            launcher.clear_active_pointer(h.output / 'w1')
            self.assertFalse((h.output / 'active_run.json').exists())

    def test_lock_helpers_treat_posix_contention_as_busy(self):
        from contextlib import contextmanager

        @contextmanager
        def held(path):
            raise BlockingIOError(11, 'Resource temporarily unavailable')
            yield
        with patch.object(launcher_module, 'writer_lock', held):
            self.assertTrue(launcher_module.lock_busy(Path('unused')))
            with launcher_module.try_lock(Path('unused')) as got:
                self.assertFalse(got)

    def test_fixed_options_change_and_ten_days_wait_for_the_operator(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            self.interrupted_run(h, status='interrupted', result='interrupted', exit=130)
            h.write_control({**h.control, 'detail_refresh': 'changed'})
            self.assertEqual(h.tick()[1]['reason'], 'fixed_options_changed')
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            self.interrupted_run(h, status='interrupted', result='interrupted', exit=130)
            h.clock.advance(days=10, minutes=1)
            self.assertEqual(h.tick()[1]['reason'], 'max_run_days')
            with patch('builtins.print'):
                h.launcher().release()
            self.assertEqual(h.state()['runs']['r1']['deadline'], '2026-11-14T00:01:00+00:00')
            h.scenario({'collector': {}})
            self.assertEqual(h.tick()[1]['kind'], 'resume')


class PreflightTests(unittest.TestCase):
    def test_listing_check_results(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            h.scenario({'check': 4, 'collector': {}})
            line = h.tick()[1]
            self.assertEqual((line['status'], line['next_action_at']), ('WAIT', '2026-11-01T06:00:00+00:00'))
            self.assertEqual(h.tick()[1]['reason'], 'backoff')
            h.clock.advance(hours=6)
            h.scenario({'check': 6, 'collector': {}})
            self.assertEqual(h.tick()[1]['reason'], 'template_expired')
            self.assertEqual(h.calls(), [])
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            h.scenario({'check': 5, 'collector': {}})
            self.assertEqual([h.tick()[1]['status'] for _ in range(3)], ['WAIT', 'WAIT', 'OPERATOR_WAIT'])

    def test_low_disk_and_a_missing_template_wait_for_the_operator(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            h.free = 5 * 1024 ** 3
            line = h.tick()[1]
            self.assertEqual((line['reason'], line['alerts'][0]['code']), ('disk_low', 'disk_low'))
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            Path(h.control['pagination_template']).unlink()
            self.assertEqual(h.tick()[1]['reason'], 'template_missing')


class LockTests(unittest.TestCase):
    def test_busy_tick_live_supervisor_and_orphan_collector(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            (h.root / 'launcher').mkdir()
            with writer_lock(h.root / 'launcher' / 'tick.lock'):
                self.assertEqual(h.tick()[1]['status'], 'TICK_BUSY')
            with writer_lock(h.root / 'launcher' / 'supervisor.lock'):
                self.assertEqual(h.tick()[1]['status'], 'RUNNING')
            h.output.mkdir(exist_ok=True)
            with writer_lock(h.output / '.collection.lock'):
                line = h.tick()[1]
            self.assertEqual((line['status'], line['alerts'][0]['code']), ('ORPHAN_RUNNING', 'orphan_collector'))
            self.assertEqual(h.calls('prune_calls.jsonl'), [])
            self.assertEqual(h.calls(), [])

    def test_a_supervisor_that_never_reports_ready_is_a_failed_start(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            launcher = Launcher(h.control_path, now=h.clock, spawner=lambda kind, attempt: None,
                                disk_free=lambda path: h.free)
            with patch.object(launcher_module, 'READY_SECONDS', 0.3), patch('builtins.print') as printed:
                launcher.tick()
            self.assertEqual(json.loads(printed.call_args_list[-1].args[0])['status'], 'START_FAILED')


class HandoffTests(unittest.TestCase):
    def completed_run(self, h, job, started='2026-10-20T00:00:00+00:00', **config):
        directory = h.output / job
        (directory / 'media').mkdir(parents=True)
        (directory / 'run_config.json').write_text(json.dumps({'job_id': job, 'config': {
            'total_limit': None, 'collect_details': True,
            'categories': [{'category_id': '1', 'start_page': 1, 'max_pages': None}], **config}}))
        (directory / 'checkpoint_state.json').write_text(json.dumps({'job_id': job, 'status': 'completed',
                                                                     'updated_at': '2026-10-21T00:00:00+00:00'}))
        (directory / 'run_report.json').write_text(json.dumps({'job_id': job, 'result': 'completed',
                                                               'started_at': started, 'limits': {'requests': None}}))
        (directory / 'media' / 'products_with_local_images.jsonl').write_text('')
        (directory / 'media' / 'download_report.json').write_text(json.dumps({'finished_at': started}))
        return directory

    def test_tick_hands_over_completed_runs_once_and_only_operating_ones(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            (h.root / 'HOLD').write_text('')
            self.completed_run(h, 'ok')
            self.completed_run(h, 'before', started='2026-09-01T00:00:00+00:00')
            self.completed_run(h, 'limited', total_limit=10)
            self.completed_run(h, 'locked')
            loaded = self.completed_run(h, 'loaded')
            seal(loaded, 'loaded')
            unfinished = self.completed_run(h, 'media')
            (unfinished / 'media' / 'download_report.json').write_text('{}')
            with writer_lock(h.output / 'locked' / 'writer.lock'):
                line = h.tick()[1]
            self.assertEqual(line['handoffs_created'], ['ok'])
            first = (h.output / 'handoff' / 'ok.json').read_bytes()
            handoff = json.loads(first)
            self.assertEqual(handoff['media_manifest'], 'media/products_with_local_images.jsonl')  # under run_dir
            self.assertEqual(Path(handoff['run_dir']), (h.output / 'ok').resolve())
            h.tick()
            self.assertEqual((h.output / 'handoff' / 'ok.json').read_bytes(), first)
            self.assertTrue(h.handoff('locked'))
            self.assertEqual(sorted(p.name for p in (h.output / 'handoff').iterdir()), ['locked.json', 'ok.json'])

    def test_a_different_existing_handoff_is_kept_and_reported_and_old_ones_alert(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            (h.root / 'HOLD').write_text('')
            directory = self.completed_run(h, 'ok')
            launcher = h.launcher()
            launcher.load()
            (h.output / 'handoff').mkdir()
            (h.output / 'handoff' / 'ok.json').write_text(json.dumps({'job_id': 'ok', 'run_dir': '/elsewhere'}))
            with launcher.state() as state:
                self.assertEqual(launcher.write_handoff(state, directory), 'mismatch')
            self.assertEqual(json.loads((h.output / 'handoff' / 'ok.json').read_text())['run_dir'], '/elsewhere')
            (h.output / 'handoff' / 'ok.json').write_text(json.dumps({'job_id': 'ok', 'created_at': '2026-10-29T00:00:00+00:00'}))
            (h.output / 'handoff' / 'ok.failed').write_text(json.dumps({'attempts': 3, 'state': 'EXHAUSTED'}))
            line = h.tick()[1]
            codes = {a['code']: a for a in line['alerts']}
            self.assertIn('handoff_not_claimed', codes)
            self.assertEqual(codes['handoff_failed']['failed'], {'attempts': 3, 'state': 'EXHAUSTED'})

    def test_a_loaded_run_changed_afterwards_is_reported_from_the_prune_report(self):
        with tempfile.TemporaryDirectory() as root:
            h = Harness(root)
            (h.root / 'HOLD').write_text('')
            (h.output.parent / 'prune_report.json').write_text(json.dumps(
                {'deleted': [], 'kept': {'a': 'loaded_inputs_changed', 'b': 'not_loaded'},
                 'images_kept': {'c': 'loaded_inputs_changed'}}))
            line = h.tick()[1]
            alert = [a for a in line['alerts'] if a['code'] == 'loaded_inputs_changed']
            self.assertEqual(alert, [{'code': 'loaded_inputs_changed', 'runs': ['a', 'c']}])


class CrawlerContractTests(unittest.TestCase):
    def test_a_handed_over_run_is_neither_abandoned_nor_pruned_until_loaded(self):
        with tempfile.TemporaryDirectory() as root:
            directory = synthetic_run(root, 'r1', '2026-10-01T00:00:00+00:00', status='partial', loaded=False)
            synthetic_run(root, 'w2', '2026-10-05T00:00:00+00:00')
            (Path(root) / 'handoff').mkdir()
            (Path(root) / 'handoff' / 'r1.json').write_text('{}')
            with self.assertRaisesRegex(ValueError, '인계'):
                abandon_run(root, directory)
            report = prune_runs(root, keep=1, apply=True, prune_abandoned=True)
            self.assertEqual(report['kept']['r1'], 'handoff_pending')
            self.assertTrue(directory.is_dir())
            seal(directory, 'r1')
            self.assertEqual(abandon_run(root, directory)['status'], 'abandoned')

    def test_sigterm_is_handled_like_ctrl_c_and_only_the_first_signal_counts(self):
        from product_collector import __main__ as collector
        with patch.object(collector.signal, 'signal') as installed, self.assertRaises(KeyboardInterrupt):
            collector.stop_on_signal(15, None)
        installed.assert_any_call(collector.signal.SIGINT, collector.signal.SIG_IGN)
        installed.assert_any_call(collector.signal.SIGTERM, collector.signal.SIG_IGN)
        with patch.object(collector.signal, 'signal') as installed, patch('builtins.print'), \
                patch.object(sys, 'argv', ['product_collector', '--workers', '0', '--listing-only']), \
                patch.object(sys, 'stderr', io.StringIO()), self.assertRaises(SystemExit):
            collector.main()
        installed.assert_any_call(collector.signal.SIGTERM, collector.stop_on_signal)
        installed.assert_any_call(collector.signal.SIGINT, collector.stop_on_signal)

    @unittest.skipUnless(os.name == 'posix', 'SIGTERM delivery to a child process needs POSIX')
    def test_a_terminated_collector_saves_its_run_as_interrupted(self):
        import signal
        import time
        script = textwrap.dedent('''
            import signal, sys, time
            from product_collector.__main__ import stop_on_signal
            signal.signal(signal.SIGTERM, stop_on_signal)
            print('ready', flush=True)
            try:
                time.sleep(30)
            except KeyboardInterrupt:
                print('interrupted', flush=True)
                raise SystemExit(130)
        ''')
        child = subprocess.Popen([sys.executable, '-c', script], stdout=subprocess.PIPE, text=True,
                                 cwd=Path(__file__).resolve().parents[1])
        self.assertEqual(child.stdout.readline().strip(), 'ready')
        child.send_signal(signal.SIGTERM)
        out, _ = child.communicate(timeout=10)
        self.assertEqual((child.returncode, out.strip()), (130, 'interrupted'))


if __name__ == '__main__':
    unittest.main()
