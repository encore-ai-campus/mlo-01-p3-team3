"""deploy/measure_run.py reads a synthetic run folder; nothing is collected."""
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

SCRIPT = Path(__file__).resolve().parents[1] / 'deploy' / 'measure_run.py'
spec = importlib.util.spec_from_file_location('measure_run', SCRIPT)
measure_run = importlib.util.module_from_spec(spec)
spec.loader.exec_module(measure_run)


def attempt(url, at, code=200, error=None):
    return {'method': 'GET', 'url': url, 'requested_at': at, 'status_code': code, 'error': error}


def raw(product_id, status='ok', fingerprint='v2:a', reused=False):
    row = {'product_id': product_id, 'detail_status': status, 'listing_fingerprint': fingerprint,
           'payload': {'listings': [], 'detail': None}}
    if reused:
        row['detail_reused_from'] = {'job_id': 'base', 'collected_at': '2026-10-20T00:00:00+00:00'}
    return row


def write_run(directory: Path, report: dict, rows=(), media=None):
    directory.mkdir(parents=True)
    (directory / 'run_report.json').write_text(json.dumps(report), encoding='utf-8')
    (directory / 'product_raw.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in rows), encoding='utf-8')
    (directory / 'details').mkdir()
    (directory / 'details' / 'a.json.gz').write_bytes(b'x' * 1000)
    if media is not None:
        (directory / 'media' / 'images').mkdir(parents=True)
        (directory / 'media' / 'download_report.json').write_text(json.dumps(media), encoding='utf-8')


def report(**changes):
    value = {'job_id': 'new', 'result': 'completed', 'started_at': '2026-11-01T00:00:00+00:00',
             'finished_at': '2026-11-01T10:00:00+00:00', 'product_count': 4, 'request_count': 5,
             'requests': [attempt('https://prod.danawa.com/list/?cate=1', '2026-11-01T00:00:00+00:00'),
                          attempt('https://prod.danawa.com/info/?pcode=2', '2026-11-01T01:00:00+00:00'),
                          attempt('https://prod.danawa.com/info/?pcode=3', '2026-11-01T02:00:00+00:00', 429),
                          attempt('https://prod.danawa.com/info/?pcode=4', '2026-11-01T03:00:00+00:00', None, 'timeout'),
                          attempt('https://prod.danawa.com/info/?pcode=5', '2026-11-01T05:00:00+00:00')],
             'categories': [{'category_id': '1', 'status': 'ok', 'reason': 'repeated_products'},
                            {'category_id': '2', 'status': 'request_failed'}],
             'detail_failures': {'requested': 100, 'counts': {'request_failed': 1, 'unrecognized': 0},
                                 'limits': {'request_failed': 0.01, 'unrecognized': 0.01}, 'passed': True},
             'quality': {'mode': 'warn', 'passed': False, 'judged': True, 'received_details': 99,
                         'metrics': {'no_images': {'rate': 0.4, 'limit': 0.3, 'count': 40, 'total': 99}},
                         'violations': [{'signal': 'no_images'}],
                         'alerts': [{'signal': 'listing_repeated_products'}, {'signal': 'listing_repeated_products'}]},
             'image_download': {'result': 'completed', 'verification': 'completed', 'downloaded': 7, 'stored': 3,
                                'failed': 1, 'gave_up': 0, 'store_checks': 10, 'store_errors': 0,
                                'failures': {'unique_urls': 11, 'rate': 0.009, 'limit': 0.01, 'external_rate': 0.02,
                                             'external_alert_limit': 0.05, 'by_kind': {'transient': 1}},
                                'retry': {'passes': [{}, {}]}},
             'detail_reuse': {'mode': 'changed', 'base_job_id': 'base', 'base_dir': None, 'sampled': 1, 'sample_size': 1000}}
    value.update(changes)
    return value


class MeasureRunTests(unittest.TestCase):
    def test_requests_quality_images_and_overlap_come_from_the_run_folder(self):
        with tempfile.TemporaryDirectory() as root:
            run = Path(root) / 'new'
            write_run(run, report(), media={'requests': 12, 'started_at': '2026-11-01T04:00:00+00:00',
                                            'finished_at': '2026-11-01T07:00:00+00:00'})
            result = measure_run.measure(run)
            detail = result['requests']['detail']
            self.assertEqual((detail['count'], detail['blocked'], detail['errors'], detail['span_hours']), (4, 1, 1, 4.0))
            self.assertEqual(result['requests']['listing']['count'], 1)
            quality = result['quality']
            self.assertEqual(quality['violations'], ['no_images'])
            self.assertEqual(quality['alerts'], {'listing_repeated_products': 2})
            self.assertEqual(quality['metrics']['detail_request_failed']['rate'], 0.01)
            self.assertEqual(quality['categories'], {'ok': 1, 'request_failed': 1})
            images = result['images']
            self.assertEqual((images['requests'], images['downloaded'], images['failure_rate'], images['retry_passes']),
                             (12, 7, 0.009, 2))
            # Details 01:00-05:00, images 04:00-07:00: one hour shared, 7 hours one after the other, 6 together.
            self.assertEqual((images['overlap_hours'], images['sequential_hours'], images['combined_hours']), (1.0, 7.0, 6.0))
            self.assertEqual((result['run']['collect_hours'], result['run']['hours']), (10.0, 10.0))
            self.assertIn('details_gb', result['disk'])
            self.assertFalse(result['disk']['pipeline_loaded'])

    def test_a_later_image_retry_wins_over_the_copy_in_run_report_and_extends_the_run(self):
        with tempfile.TemporaryDirectory() as root:
            run = Path(root) / 'new'
            write_run(run, report(image_download={**report()['image_download'], 'stop_reason': 'http_429'}),
                      media={'result': 'completed', 'downloaded': 9, 'requests': 3,
                                            'failures': {'rate': 0.0, 'unique_urls': 11},
                                            'started_at': '2026-11-01T09:00:00+00:00',
                                            'finished_at': '2026-11-01T12:30:00+00:00'})
            result = measure_run.measure(run)
            images = result['images']
            self.assertEqual((images['result'], images['delivered_result'], images['downloaded'], images['failure_rate']),
                             ('completed', 'completed', 9, 0.0))
            self.assertEqual(images['retry_passes'], 2)  # not in the step copy here: kept from run_report
            self.assertIsNone(images['stop_reason'])  # the block was the earlier attempt's
            self.assertEqual((result['run']['collect_hours'], result['run']['hours']), (10.0, 12.5))

    def test_requested_details_are_split_into_new_changed_and_same_fingerprint(self):
        with tempfile.TemporaryDirectory() as root:
            base = Path(root) / 'base'
            write_run(base, report(job_id='base'), [raw('1', fingerprint='v2:a'), raw('2', fingerprint='v2:b'),
                                                    raw('3', fingerprint='v2:c')])
            run = Path(root) / 'new'
            rows = [raw('1', fingerprint='v2:a', reused=True), raw('2', fingerprint='v2:changed'),
                    raw('3', fingerprint='v2:c'), raw('4', status='request_failed'), raw('5', status='not_needed')]
            write_run(run, report(detail_reuse={'mode': 'changed', 'base_dir': str(base), 'sampled': 1}), rows)
            reuse = measure_run.measure(run)['reuse']
            self.assertEqual((reuse['reused'], reuse['detail_requested'], reuse['sampled']), (1, 3, 1))
            self.assertEqual(reuse['requested_because'], {'changed': 1, 'same_fingerprint': 1, 'new': 1})
            self.assertEqual(reuse['detail_status'], {'ok': 3, 'request_failed': 1, 'not_needed': 1})

    def test_rss_log_peak_and_a_two_run_table(self):
        with tempfile.TemporaryDirectory() as root:
            first, second = Path(root) / 'a', Path(root) / 'b'
            write_run(first, report())
            write_run(second, report(product_count=9, image_download=None))
            log = Path(root) / 'rss.log'
            # 2026-11-01T01:00Z and 01:00:30Z fall inside the run; the last sample is a later run's.
            log.write_text('1793494800 41 102400 204800\nnot a sample\n1793494830 41 307200 307200\n'
                           '1793600000 77 900000 900000\n', encoding='utf-8')
            self.assertEqual(measure_run.measure(first, log)['rss'], {'samples': 2, 'pids': 1, 'peak_rss_mb': 300.0})
            stream = io.StringIO()
            with redirect_stdout(stream):
                self.assertEqual(measure_run.main([str(first), str(second), '--rss-log', str(log)]), 0)
            lines = dict(line.split('\t', 1) for line in stream.getvalue().splitlines())
            self.assertEqual(lines['run.products'], '4\t9')
            self.assertEqual(lines['rss.peak_rss_mb'], '300.0\tnull')
            self.assertIn('quality.no_images (limit 0.3)', lines)
            off = Path(root) / 'off'
            write_run(off, report(quality=None, detail_failures=None))
            stream = io.StringIO()
            with redirect_stdout(stream):  # a run with the gate off first: the other run's rates still show
                measure_run.main([str(off), str(first)])
            lines = dict(line.split('\t', 1) for line in stream.getvalue().splitlines())
            self.assertEqual(lines['quality.no_images (limit 0.3)'], 'null\t0.4')

    def test_a_folder_without_a_report_is_an_error_not_a_crash(self):
        with tempfile.TemporaryDirectory() as root:
            with redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    measure_run.main([])
                self.assertEqual(measure_run.main([root]), 2)


if __name__ == '__main__':
    unittest.main()
