"""Quality gate and live identity check: synthetic rows and pages, no network."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from product_collector import quality
from product_collector.core import Category, RequestClient, collect, save_run
from product_collector.danawa import DanawaAdapter, rejected_api_products, validate_identity
from product_collector.reprocess import reprocess_run
from product_collector.storage import save_detail_html
from tests.test_product_collector import FakeSession, detail, listing


def script(*nodes):
    return '<script type="application/ld+json">' + json.dumps({"@graph": list(nodes)}) + '</script>'


def product(code=None, **extra):
    node = {"@type": "Product", **extra}
    if code:
        node["url"] = f"https://prod.danawa.com/info/?pcode={code}"
    return node


def row(status="ok", images=1, specs=1, priced=1, category="1"):
    return {"detail_status": status, "categories": [{"category_id": category}],
            "images": [{"url": "x"}] * images or None,
            "specifications": [{"name": "크기", "value": "10"}] * specs or None,
            "offers": [{"price": 1000}] * priced or None}


class IdentityTests(unittest.TestCase):
    def test_recommendations_in_a_separate_script_are_not_a_mismatch(self):
        validate_identity('<link rel="canonical" href="https://prod.danawa.com/info/?pcode=11">'
                          + script(product("11")) + script(product("12"), product("13")), "11")

    def test_confirmed_canonical_allows_recommendations_when_the_primary_has_no_url(self):
        validate_identity('<link rel="canonical" href="https://prod.danawa.com/info/?pcode=11">'
                          + script(product(name="상품 11", sku="SELLER-1"), product("12")), "11")

    def test_seller_sku_is_not_a_product_id(self):
        validate_identity('<link rel="canonical" href="https://prod.danawa.com/info/?pcode=11">'
                          + script(product(sku="SELLER-1")), "11")

    def test_without_canonical_a_recommended_match_does_not_vouch_for_a_foreign_page(self):
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            validate_identity(script(product("12")) + script(product("11")), "11")

    def test_foreign_id_only_in_at_id_is_a_mismatch(self):
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            validate_identity(script(product(**{"@id": "https://prod.danawa.com/info/?pcode=12"})), "11")

    def test_foreign_canonical_is_still_a_mismatch(self):
        with self.assertRaisesRegex(ValueError, "identity mismatch"):
            validate_identity('<link rel="canonical" href="https://prod.danawa.com/info/?pcode=12">'
                              + script(product("11")), "11")


class LiveIdentityTests(unittest.TestCase):
    def test_another_products_page_keeps_only_its_reference_and_reprocess_rechecks_it(self):
        adapter = DanawaAdapter()
        url = "https://prod.danawa.com/info/?pcode=11"
        pages = {adapter.category_url("1"): listing("11"), url: detail("12")}
        raw, normal, report = collect(adapter, RequestClient(5, interval=0, session=FakeSession(pages)),
                                      [Category("1")], 10)
        self.assertEqual(raw[0]["detail_status"], "identity_mismatch")
        self.assertEqual(set(raw[0]["payload"]["detail"]), {"url", "collected_at", "status"})
        self.assertIsNone(normal[0]["images"])
        self.assertEqual(normal[0]["field_status"]["manufacturer"], "detail_identity_mismatch")
        self.assertEqual(report["result"], "partial")
        with tempfile.TemporaryDirectory() as directory:
            output = save_run(Path(directory), raw, normal, report)
            at = raw[0]["payload"]["detail"]["collected_at"]
            save_detail_html(output, "11", url, detail("12"), at)
            again, _, _ = reprocess_run(output)
            self.assertEqual(again[0]["detail_status"], "identity_mismatch")
            # A page that turns out to be right (e.g. a corrected check) is extracted from the same reference.
            save_detail_html(output, "11", url, detail("11"), at)
            fixed, _, fixed_report = reprocess_run(output, quality_mode="enforce")
            self.assertEqual((fixed[0]["detail_status"], fixed_report["result"]), ("ok", "completed"))
            self.assertFalse(fixed_report["quality"]["judged"])


class GateTests(unittest.TestCase):
    def report(self, **category):
        return {"result": "completed", "categories": [{"category_id": "1", "selected": 1, "pages": [], **category}]}

    def test_small_runs_record_rates_without_judging(self):
        summary = quality.evaluate([row(images=0)] * 10, self.report())
        self.assertEqual((summary["judged"], summary["passed"]), (False, True))
        self.assertEqual(summary["metrics"]["no_images"]["rate"], 1.0)

    def test_rates_use_received_pages_and_final_fields(self):
        rows = [row()] * 80 + [row(specs=0, priced=0)] * 20 + [row("request_failed", 0, 0, 0)] * 500
        summary = quality.evaluate(rows, self.report())
        self.assertEqual(summary["received_details"], 100)
        self.assertEqual(summary["metrics"]["no_specifications"]["rate"], 0.2)
        self.assertTrue(summary["passed"])
        self.assertEqual(summary["gallery_count_median"], 1)

    def test_enforce_fails_only_a_completed_run(self):
        rows = [row()] * 60 + [row(images=0)] * 40
        warned = quality.apply(self.report(), rows, "warn")
        self.assertEqual((warned["result"], warned["quality"]["passed"]), ("completed", False))
        self.assertEqual({v["signal"] for v in warned["quality"]["violations"]}, {"no_images"})
        self.assertEqual(quality.apply(self.report(), rows, "enforce")["result"], "quality_failed")
        partial = {**self.report(), "result": "partial"}
        self.assertEqual(quality.apply(partial, rows, "enforce")["result"], "partial")
        self.assertNotIn("quality", quality.apply(self.report(), rows, "off"))

    def test_detail_failures_are_judged_by_rate_and_small_runs_stay_strict(self):
        def raw(status, i):
            return {"product_id": str(i), "detail_status": status}
        ok = [raw("ok", i) for i in range(200)]
        within = quality.detail_failures(ok + [raw("request_failed", 900), raw("unrecognized", 901)])
        self.assertEqual((within["passed"], within["failed_product_ids"]), (True, ["900", "901"]))
        over = quality.detail_failures(ok + [raw("request_failed", 900 + i) for i in range(3)])
        self.assertFalse(over["passed"])
        self.assertFalse(quality.detail_failures(ok[:10] + [raw("unrecognized", 900)])["passed"])
        # Budget stops and listing-only products are completeness, not a failure rate.
        self.assertEqual(quality.detail_failures(ok + [raw("budget_exhausted", 1), raw("disabled", 2)])["requested"], 200)

    def test_a_few_failed_details_keep_a_large_run_completed(self):
        adapter = DanawaAdapter()
        codes = [str(i) for i in range(100, 220)]
        pages = {adapter.category_url("1"): listing(*codes),
                 **{f"https://prod.danawa.com/info/?pcode={c}": detail(c) for c in codes}}
        pages["https://prod.danawa.com/info/?pcode=100"] = (500, "error")
        raw, _, report = collect(adapter, RequestClient(200, interval=0, session=FakeSession(pages)),
                                 [Category("1", limit=None)], 200)
        self.assertEqual(raw[0]["detail_status"], "request_failed")
        self.assertEqual(report["errors"][0]["stage"], "detail")
        self.assertEqual((report["result"], report["detail_failures"]["failed_product_ids"]), ("completed", ["100"]))

    def test_one_broken_category_alerts_even_when_the_total_passes(self):
        rows = [row(category="1")] * 200 + [row(images=0, category="2")] * 25
        summary = quality.evaluate(rows, self.report())
        self.assertTrue(summary["passed"])
        self.assertIn({"signal": "category_no_images", "category_id": "2"},
                      [{k: a[k] for k in ("signal", "category_id")} for a in summary["alerts"]])

    def test_listing_rejections_are_counted_before_filtering(self):
        products = [{"productCode": str(i), "name": {"full": f"상품 {i}"}} for i in range(10)]
        products += [{"productCode": str(i), "name": {"renamed": "x"}} for i in range(10, 100)]
        rejected = rejected_api_products(products)
        self.assertEqual(rejected, {"invalid_code": 0, "missing_name": 90})
        self.assertEqual(len(DanawaAdapter().parse_api_products(products, None)), 10)
        page = {"page": 1, "status": "ok", "diagnostics": {"response_product_count": 100, "rejected_products": rejected}}
        summary = quality.evaluate([], self.report(pages=[page]))
        self.assertEqual(summary["metrics"]["listing_rejected"]["rate"], 0.9)
        self.assertFalse(summary["passed"])  # judged on its own denominator, even with no details

    def test_structure_alerts_for_repeated_and_empty_first_pages(self):
        summary = quality.evaluate([], self.report(reason="repeated_products"))
        self.assertEqual([a["signal"] for a in summary["alerts"]], ["listing_repeated_products"])
        empty = {"result": "completed", "categories": [{"category_id": "1", "selected": 0,
                                                         "pages": [{"page": 1, "status": "empty"}]}]}
        self.assertEqual([a["signal"] for a in quality.evaluate([], empty)["alerts"]], ["listing_first_page_empty"])


if __name__ == "__main__":
    unittest.main()
