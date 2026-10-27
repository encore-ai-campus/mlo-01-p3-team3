"""Synthetic saved HTML: no network calls or other shopping-site adapters."""
from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

from product_collector.core import Category, RequestClient, BudgetExhausted, collect, load_categories, save_run
from product_collector.danawa import DanawaAdapter, amount
from product_collector.reprocess import reprocess_run


def card(code, legacy=False, price="최저가 12,000원"):
    tag = "li" if legacy else "div"
    cls = "prod_item" if legacy else "dnw-catalog-item"
    return f'''<{tag} class="{cls}"><a href="/info/?pcode={code}">상품 {code}</a>
    <img data-src="//img.danawa.com/{code}.jpg" src="data:image/gif;base64,x">
    <span>배송비 3,000원</span><div class="price_sect"><strong class="price">{price}</strong></div>
    <div class="spec_list">가구 / 원목</div><span>등록월 2024.03.</span></{tag}>'''


def listing(*codes, legacy=False):
    return '<html><title>테스트 카테고리</title><div id="productListArea">' + ''.join(
        card(c, legacy) for c in codes) + '</div></html>'


def detail(code, extra=None):
    product = {"@type": "Product", "url": f"https://prod.danawa.com/info/?pcode={code}",
               "name": f"상품 {code}", "manufacturer": {"name": "테스트 제조사"},
               "brand": "별도 브랜드", "model": "M-1", "description": "상품 설명",
               "offers": {"@type": "AggregateOffer", "lowPrice": "12000", "highPrice": "15000",
                          "priceCurrency": "KRW", "offerCount": 7,
                          "offers": [{"@type": "Offer", "price": "13000", "priceCurrency": "KRW",
                                      "seller": {"name": "테스트 판매처"}}]}}
    product.update(extra or {})
    return '<html><script type="application/ld+json">' + json.dumps(
        {"@graph": [{"@type": "WebSite"}, product]}, ensure_ascii=False) + '</script></html>'


class FakeSession:
    def __init__(self, pages):
        self.pages = pages
        self.headers = {}
        self.calls = []

    def mount(self, *_):
        pass

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        value = self.pages[url]
        if isinstance(value, Exception):
            raise value
        response = requests.Response()
        response.status_code, body = value if isinstance(value, tuple) else (200, value)
        response._content = body.encode("utf-8")
        response.encoding = "utf-8"
        response.url = url
        return response

    def close(self):
        pass


class CollectorTests(unittest.TestCase):
    def setUp(self):
        self.adapter = DanawaAdapter()

    def pipeline(self, pages, categories, total=10, budget=15):
        session = FakeSession(pages)
        client = RequestClient(budget, interval=0, session=session)
        raw, normalized, report = collect(self.adapter, client, categories, total)
        return raw, normalized, report, session

    def test_five_categories_ten_products_fifteen_requests(self):
        categories = [Category(c) for c in ("15241194", "15240504", "1523647", "15241240", "15227279")]
        pages = {}
        for i, category in enumerate(categories):
            codes = (str(i * 2 + 1), str(i * 2 + 2))
            pages[self.adapter.category_url(category.category_id)] = listing(*codes, "999")
            pages.update({f"https://prod.danawa.com/info/?pcode={code}": detail(code) for code in codes})
        raw, normal, report, session = self.pipeline(pages, categories)
        self.assertEqual((len(raw), len(normal), report["request_count"]), (10, 10, 15))
        self.assertTrue(all(c["selected"] == 2 for c in report["categories"]))
        self.assertEqual(report["result"], "completed")
        self.assertTrue(all('/list/' in u for u, _ in session.calls[:5]))
        self.assertTrue(all(not kw["allow_redirects"] and kw["timeout"] == 20 for _, kw in session.calls))

    def test_duplicate_keeps_memberships_and_one_detail_request(self):
        pages = {self.adapter.category_url("1"): listing("11", "12", "11"),
                 self.adapter.category_url("2"): listing("11", "13"),
                 **{f"https://prod.danawa.com/info/?pcode={c}": detail(c) for c in ("11", "12", "13")}}
        raw, normal, report, _ = self.pipeline(pages, [Category("1"), Category("2")])
        self.assertEqual(len(raw), 3)
        self.assertEqual(len(raw[0]["categories"]), 2)
        self.assertEqual(len(raw[0]["payload"]["listings"]), 2)
        self.assertEqual(report["request_count"], 5)
        self.assertEqual(normal[0]["raw_payload_hash"], raw[0]["payload_hash"])

    def test_total_limit_stops_additional_listings(self):
        pages = {self.adapter.category_url("1"): listing("11", "12"),
                 "https://prod.danawa.com/info/?pcode=11": detail("11")}
        raw, _, report, _ = self.pipeline(pages, [Category("1"), Category("2")], total=1)
        self.assertEqual(len(raw), 1)
        self.assertEqual(report["categories"][1]["reason"], "total_limit")
        self.assertEqual(report["request_count"], 2)

    def test_request_budget_retains_unenriched_listings(self):
        pages = {self.adapter.category_url("1"): listing("11", "12")}
        raw, normal, report, session = self.pipeline(pages, [Category("1")], budget=1)
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(len(raw), 2)
        self.assertTrue(all(r["detail_status"] == "budget_exhausted" for r in raw))
        self.assertEqual(normal[0]["field_status"]["manufacturer"], "detail_budget_exhausted")
        self.assertEqual(report["result"], "partial")

    def test_category_error_does_not_prevent_next_category(self):
        pages = {self.adapter.category_url("1"): (500, "Server error"),
                 self.adapter.category_url("2"): listing("11"),
                 "https://prod.danawa.com/info/?pcode=11": detail("11")}
        raw, _, report, _ = self.pipeline(pages, [Category("1"), Category("2")])
        self.assertEqual(len(raw), 1)
        self.assertEqual(report["request_count"], 3)
        self.assertEqual(report["categories"][0]["status"], "request_failed")

    def test_detail_timeout_preserves_listing(self):
        pages = {self.adapter.category_url("1"): listing("11"),
                 "https://prod.danawa.com/info/?pcode=11": requests.Timeout("timeout")}
        raw, normal, report, _ = self.pipeline(pages, [Category("1")])
        self.assertEqual(normal[0]["name"], "상품 11")
        self.assertEqual(raw[0]["detail_status"], "request_failed")
        self.assertEqual(report["requests"][-1]["error"], "Timeout: timeout")

    def test_both_selectors_lazy_image_and_price_not_shipping(self):
        for legacy in (True, False):
            rows, _ = self.adapter.parse_listing(listing("11", "11", legacy=legacy), 2)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["price_text"], "최저가 12,000원")
            self.assertEqual(rows[0]["image_url"], "https://img.danawa.com/11.jpg")
            self.assertEqual(rows[0]["registered_month"], "2024.03.")

    def test_shipping_only_is_not_a_product_price(self):
        rows, _ = self.adapter.parse_listing(listing("11").replace('최저가 12,000원', ''), 2)
        self.assertEqual(rows[0]["price_text"], "")

    def test_current_card_name_and_price_follow_primary_pcode(self):
        html = '''<div id="productListArea"><div class="dnw-catalog-item">
        <a href="/info/?pcode=11"><img src="//img.danawa.com/x.jpg"><button>이미지 보기</button></a>
        <div data-testid="ProductListTitle"><a href="/info/?pcode=11">실제 상품명</a></div>
        <div data-testid="ProductListSpecs">침대 / 철재</div><span>18.02. 등록</span>
        <div class="dnw-product-price"><a href="/info/?pcode=99">1,000 원</a>
        <a href="/info/?pcode=11">12,000 원</a></div></div></div>'''
        rows, _ = self.adapter.parse_listing(html, 2)
        self.assertEqual(rows[0]["product_name"], "실제 상품명")
        self.assertEqual(rows[0]["price_text"], "12,000 원")
        self.assertEqual(rows[0]["spec_text"], "침대 / 철재")
        self.assertEqual(len(rows[0]["variant_prices"]), 2)
        self.assertIsNone(rows[0]["registered_month"])
        self.assertEqual(rows[0]["registered_month_text"], "18.02. 등록")

    def test_detail_sku_maker_month_and_html_seller_offer(self):
        html = '''<script type="application/ld+json">{"@type":"Product","sku":"11",
        "name":"실제 상품명","brand":"브랜드","offers":{"@type":"AggregateOffer","lowPrice":"12000"}}</script>
        <div class="made_info"><span class="txt">등록월: 2018.02.</span>
        <span id="makerTxtArea">제조사: <a>제조업체</a></span></div>
        <dl class="spec_set"><div class="spec_list">침대 / 크기: 110x200cm / 무게: 15kg</div></dl>
        <ul class="list__mall-price"><li><div class="box__logo"><img alt="판매몰"></div>
        <div class="sell-price" data-base-price="12000" data-delivery-price="0">12,000 원</div>
        <div class="box__delivery">무료배송</div><a class="link__full-cover" href="/bridge/?pcode=11&cmpnyc=ABC"></a>
        </li></ul>'''
        pages = {self.adapter.category_url("1"): listing("11"), "https://prod.danawa.com/info/?pcode=11": html}
        raw, normal, _, _ = self.pipeline(pages, [Category("1")])
        self.assertEqual(raw[0]["payload"]["detail"]["matched_by"], "sku")
        self.assertEqual(normal[0]["manufacturer"], "제조업체")
        self.assertEqual(normal[0]["brand"], "브랜드")
        self.assertEqual(normal[0]["registered_month"], "2018.02.")
        self.assertEqual(normal[0]["size_text"], "110x200cm")
        self.assertEqual(normal[0]["weight_text"], "15kg")
        self.assertEqual(normal[0]["offers"][1]["shipping_fee"], 0)
        self.assertEqual(normal[0]["offers"][1]["seller_id"], "ABC")

    def test_offline_reprocess_issues_no_http_and_keeps_collection_time(self):
        pages = {self.adapter.category_url("1"): listing("11"),
                 "https://prod.danawa.com/info/?pcode=11": detail("11")}
        raw, normal, report, _ = self.pipeline(pages, [Category("1")])
        with tempfile.TemporaryDirectory() as directory:
            output = save_run(Path(directory), raw, normal, report)
            with patch('requests.Session.get', side_effect=AssertionError("network forbidden")) as get:
                reparsed, normalized, replay_report = reprocess_run(output)
                get.assert_not_called()
            self.assertEqual(replay_report["request_count"], 0)
            self.assertEqual(replay_report["original_request_count"], 2)
            self.assertEqual(reparsed[0]["collected_at"], raw[0]["collected_at"])
            self.assertEqual(normalized[0]["name"], normal[0]["name"])

    def test_json_ld_nan_rejected_with_html_fallback(self):
        row = self.adapter.parse_listing(listing("11"), 2)[0][0]
        data = self.adapter.parse_detail('<script type="application/ld+json">{"rating":NaN}</script>', row)
        self.assertEqual(len(data["json_ld_errors"]), 1)
        self.assertEqual(data["json_ld"], [])

    def test_json_ld_offer_meaning_and_brand_separation(self):
        pages = {self.adapter.category_url("1"): listing("11"),
                 "https://prod.danawa.com/info/?pcode=11": detail("11")}
        _, normal, _, _ = self.pipeline(pages, [Category("1")])
        row = normal[0]
        self.assertEqual(row["manufacturer"], "테스트 제조사")
        self.assertEqual(row["brand"], "별도 브랜드")
        self.assertEqual(row["offers"][0]["low_price"], 12000)
        self.assertEqual(row["offers"][0]["offer_count"], 7)
        self.assertEqual(row["offers"][1]["seller_name"], "테스트 판매처")
        self.assertNotIn("inventory", row)
        self.assertNotIn("dateReg", row)

    def test_invalid_json_ld_html_spec_fallback(self):
        row = self.adapter.parse_listing(listing("11"), 2)[0][0]
        html = '''<script type="application/ld+json">broken</script><table class="spec_tbl">
        <tr><th>제조사</th><td>A</td><th>모델명</th><td>B</td></tr></table>'''
        data = self.adapter.parse_detail(html, row)
        self.assertEqual(data["status"], "ok")
        self.assertEqual(len(data["json_ld_errors"]), 1)
        self.assertEqual(data["specifications"], [{"name": "제조사", "value": "A"}, {"name": "모델명", "value": "B"}])
        self.assertEqual(data["json_ld_scripts"], ["broken"])

    def test_unrelated_json_ld_product_not_used(self):
        row = self.adapter.parse_listing(listing("11"), 2)[0][0]
        data = self.adapter.parse_detail(detail("99"), row)
        self.assertIsNone(data["selected_product"])
        self.assertEqual(data["status"], "unrecognized")
        self.assertIsNotNone(data["unrecognized_html"])

    def test_json_ld_array_types_and_reference_offers(self):
        row = self.adapter.parse_listing(listing("11"), 2)[0][0]
        graph = [{"@type": ["Thing", "Product"], "productID": "11", "offers": {"@id": "#offers"}},
                 {"@id": "#offers", "@type": "AggregateOffer", "lowPrice": "123.50", "offers": {"@id": "#offers"}}]
        pages = {self.adapter.category_url("1"): listing("11"),
                 row["product_url"]: '<script type="application/ld+json">' + json.dumps(graph) + '</script>'}
        _, normal, _, _ = self.pipeline(pages, [Category("1")])
        self.assertEqual(normal[0]["offers"][0]["low_price"], "123.50")
        self.assertEqual(len(normal[0]["offers"]), 1)

    def test_empty_listing_report_and_empty_output(self):
        raw, normal, report, _ = self.pipeline({self.adapter.category_url("1"): '<title>확인 필요</title>'}, [Category("1")])
        self.assertEqual(raw, [])
        self.assertEqual(report["result"], "partial")
        with tempfile.TemporaryDirectory() as directory:
            output = save_run(Path(directory), raw, normal, report)
            self.assertEqual((output / "product_raw.jsonl").read_text(), '')
            self.assertTrue((output / "run_report.json").exists())

    def test_save_linkage_and_offline_renormalization(self):
        pages = {self.adapter.category_url("1"): listing("11"),
                 "https://prod.danawa.com/info/?pcode=11": detail("11")}
        raw, normal, report, _ = self.pipeline(pages, [Category("1")])
        with tempfile.TemporaryDirectory() as directory:
            output = save_run(Path(directory), raw, normal, report)
            loaded = json.loads((output / "product_raw.jsonl").read_text(encoding="utf-8"))
            self.assertEqual(self.adapter.normalize(loaded), normal[0])
            with self.assertRaises(FileExistsError):
                save_run(Path(directory), raw, normal, report)

    def test_category_config_accepts_seven_digits_and_rejects_duplicates(self):
        self.assertEqual(Category("1523647").category_id, "1523647")
        for value in ("", "1x", 123, "１２３"):
            with self.assertRaises(ValueError):
                Category(value)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "categories.json"
            path.write_text('[{"category_id":"1"},{"category_id":"1"}]')
            with self.assertRaises(ValueError):
                load_categories(path)

    def test_request_failures_count_redirects_not_followed_and_host_restricted(self):
        url = self.adapter.category_url("1")
        session = FakeSession({url: (302, "redirect")})
        client = RequestClient(max_requests=1, interval=0, session=session)
        with self.assertRaises(requests.HTTPError):
            client.get(url)
        with self.assertRaises(BudgetExhausted):
            client.get(url)
        with self.assertRaises(ValueError):
            client.get("https://other.example/")
        self.assertEqual(len(session.calls), 1)

    def test_interval_and_timeout_validation(self):
        url = self.adapter.category_url("1")
        moments = iter([0.0, 0.25, 1.0])
        sleeps = []
        client = RequestClient(interval=1, session=FakeSession({url: "ok"}),
                               clock=lambda: next(moments), sleep=sleeps.append)
        client.get(url)
        client.get(url)
        self.assertEqual(sleeps, [0.75])
        for interval, timeout in ((-1, 20), (float('nan'), 20), (1, 0)):
            with self.assertRaises(ValueError):
                RequestClient(interval=interval, timeout=timeout)

    def test_amount_does_not_infer_ranges_or_invalid_values(self):
        self.assertEqual(amount("12,000"), 12000)
        self.assertEqual(amount("0"), 0)
        for value in ("1~10", "가격문의", True, "NaN", "-10"):
            self.assertIsNone(amount(value))


if __name__ == "__main__":
    unittest.main()
