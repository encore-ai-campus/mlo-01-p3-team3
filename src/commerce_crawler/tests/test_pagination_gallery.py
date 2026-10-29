"""Bounded POST pagination and gallery-only image collection; no network."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from product_collector.core import Category, RequestClient, collect, save_run
from product_collector.danawa import DanawaAdapter, extract_gallery
from product_collector.pagination import ListingResponseError, PaginationTemplates, decode_products
from product_collector.reprocess import reprocess_run
from test_product_collector import FakeSession, detail, listing


def product(code):
    return {"productCode": int(code), "makerCode": 999,
            "name": {"full": f"의자 {code}", "maker": "제조사", "brand": "", "model": ""},
            "description": {"pc": {"raw": "의자 / 좌판가로: 50.8cm", "segments": [{"text": "의자"}]}}}


def response(*codes):
    return '0:{"a":"$@1","q":"?cate=1523647"}\n1:' + json.dumps(
        {"success": True, "data": {"products": [product(code) for code in codes]}}, ensure_ascii=False)


def templates():
    return PaginationTemplates({"headers": {"Next-Action": "TEST_ONLY_ACTION"}, "categories": {
        "1523647": {"payload": [{"categoryCode": "3647", "page": 5, "listCount": 30,
                                 "physicsCate1": "1826", "physicsCate2": "32303", "physicsCate3": "32346",
                                 "searchMaker": "$undefined", "sortMethod": "BEST"}]}}})


class PagingSession(FakeSession):
    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        page = json.loads(kwargs["data"])[0]["page"]
        value = self.pages[(url, page)]
        if isinstance(value, Exception):
            raise value
        result = requests.Response()
        result.status_code, body = value if isinstance(value, tuple) else (200, value)
        result._content = body.encode("utf-8")
        result.encoding = "utf-8"
        result.url = url
        return result


class PaginationGalleryTests(unittest.TestCase):
    def pipeline(self, pages, limit=10, max_pages=3, budget=15, start=1):
        adapter = DanawaAdapter(templates())
        session = PagingSession(pages)
        client = RequestClient(budget, interval=0, session=session)
        raw, normal, report = collect(adapter, client, [Category("1523647", limit=limit,
                                                                  start_page=start, max_pages=max_pages)],
                                       collect_details=False)
        return raw, normal, report, session

    def test_post_pages_overlap_and_empty_termination(self):
        url = DanawaAdapter().category_url("1523647")
        raw, normal, report, session = self.pipeline({(url, 1): response("11", "12"),
                                                      (url, 2): response("12", "13"), (url, 3): response()})
        self.assertEqual([r["product_id"] for r in raw], ["11", "12", "13"])
        self.assertEqual([json.loads(k["data"])[0]["page"] for _, k in session.calls], [1, 2, 3])
        self.assertTrue(all(u == url for u, _ in session.calls))
        self.assertEqual(report["categories"][0]["reason"], "empty_page")
        self.assertEqual(report["request_count"], 3)
        self.assertEqual(report["result"], "completed")
        self.assertEqual(normal[0]["manufacturer"], "제조사")
        self.assertEqual(normal[0]["detail_status"], "disabled")
        self.assertEqual(normal[0]["field_status"]["images"], "detail_disabled")
        self.assertEqual(raw[2]["payload"]["listings"][0]["page"], 2)

    def test_changes_only_page_and_does_not_mutate_template(self):
        template = templates()
        original = copy.deepcopy(template.categories)
        payload, headers = template.request("1523647", 8)
        self.assertEqual(payload[0]["page"], 8)
        self.assertEqual(payload[0]["categoryCode"], "3647")
        self.assertEqual(payload[0]["physicsCate2"], "32303")
        self.assertEqual(payload[0]["searchMaker"], "$undefined")
        self.assertEqual(headers["Next-Action"], "TEST_ONLY_ACTION")
        self.assertEqual(template.categories, original)

    def test_request_budget_applies_to_post_and_retains_results(self):
        url = DanawaAdapter().category_url("1523647")
        raw, _, report, session = self.pipeline({(url, 1): response("11")}, budget=1)
        self.assertEqual(len(raw), 1)
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(report["stop_reason"], "request_limit")
        self.assertEqual(report["result"], "partial")

    def test_start_page_and_max_pages(self):
        url = DanawaAdapter().category_url("1523647")
        raw, _, report, session = self.pipeline({(url, 8): response("11"), (url, 9): response("12")},
                                                start=8, max_pages=2)
        self.assertEqual(len(raw), 2)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(report["categories"][0]["reason"], "max_pages")

    def test_repeated_page_stops_without_duplicates(self):
        url = DanawaAdapter().category_url("1523647")
        raw, _, report, session = self.pipeline({(url, 1): response("11"), (url, 2): response("11")})
        self.assertEqual(len(raw), 1)
        self.assertEqual(len(session.calls), 2)
        self.assertEqual(report["categories"][0]["reason"], "repeated_products")

    def test_category_limit_stops_before_next_page(self):
        url = DanawaAdapter().category_url("1523647")
        raw, _, report, session = self.pipeline({(url, 1): response("11", "12", "13")}, limit=2)
        self.assertEqual(len(raw), 2)
        self.assertEqual(len(session.calls), 1)
        self.assertEqual(report["categories"][0]["reason"], "category_limit")

    def test_http_200_bad_action_is_not_empty_last_page(self):
        url = DanawaAdapter().category_url("1523647")
        raw, _, report, _ = self.pipeline({(url, 1): response("11"), (url, 2): '<html>error</html>'})
        self.assertEqual(len(raw), 1)
        self.assertEqual(report["categories"][0]["status"], "parse_failed")
        self.assertEqual(report["result"], "partial")

    def test_missing_template_rejected_before_any_network_request(self):
        session = FakeSession({})
        with self.assertRaises(ValueError):
            collect(DanawaAdapter(), RequestClient(session=session), [Category("1523647", max_pages=2)])
        self.assertEqual(session.calls, [])
        with self.assertRaises(ValueError):
            DanawaAdapter(templates()).validate_categories([Category("15241194", max_pages=2)])

    def test_malformed_success_and_failure_response(self):
        for body in ('1:{"success":false}', '1:{"success":true,"data":{"productList":{}}}',
                     '1:{"success":true,"data":{"products":[12]}}'):
            with self.assertRaises(ListingResponseError):
                decode_products(body)
        rows, _ = decode_products('{"success":true,"data":{"products":[]}}')
        self.assertEqual(rows, [])

    def test_post_raw_offline_reprocessing(self):
        url = DanawaAdapter().category_url("1523647")
        raw, normal, report, _ = self.pipeline({(url, 1): response("11")}, max_pages=1)
        with tempfile.TemporaryDirectory() as directory:
            output = save_run(Path(directory), raw, normal, report)
            reparsed, normalized, replay = reprocess_run(output)
        self.assertEqual(normalized[0]["name"], "의자 11")
        self.assertEqual(reparsed[0]["payload"]["listings"][0]["api_product"]["makerCode"], 999)
        self.assertEqual(replay["request_count"], 0)

    def test_gallery_matches_observed_rendition_and_excludes_description(self):
        html = '''<img id="baseImage" src="//img.danawa.com/a.jpg?shrink=330:330">
        <ul id="basicThumbSlideArea"><li><img src="//img.danawa.com/a.jpg?shrink=53:53"></li>
        <li><img data-src="//img.danawa.com/b.jpg?shrink=53:53" src="data:image/gif,x"></li>
        <li><img src="//img.danawa.com/a.jpg?shrink=53:53"></li></ul>
        <div id="productDescription"><img src="//img.danawa.com/explanation.jpg"></div>'''
        product_node = {"image": ["https://img.danawa.com/a.jpg?shrink=500:500",
                                  "https://img.danawa.com/b.jpg?shrink=500:500",
                                  "https://img.danawa.com/explanation.jpg"]}
        images = extract_gallery(BeautifulSoup(html, 'html.parser'), product_node, "https://prod.danawa.com/info/?pcode=11")
        self.assertEqual(len(images), 2)
        self.assertEqual(images[0]["url"], "https://img.danawa.com/a.jpg?shrink=500:500")
        self.assertEqual(images[0]["role"], "representative")
        self.assertEqual(images[1]["order"], 1)
        self.assertTrue(all("explanation" not in entry["url"] for entry in images))

    def test_no_gallery_does_not_use_unverified_json_ld_or_listing_images(self):
        adapter = DanawaAdapter()
        client = RequestClient(interval=0, session=FakeSession({adapter.category_url("1"): listing("11"),
                                                            "https://prod.danawa.com/info/?pcode=11": detail("11", {"image": ["https://img.danawa.com/explanation.jpg"]})}))
        raw, normal, _ = collect(adapter, client, [Category("1")])
        self.assertIsNone(normal[0]["images"])
        self.assertNotIn("image_urls", normal[0])
        self.assertIsNotNone(normal[0]["listing_image_url"])
        self.assertEqual(raw[0]["payload"]["detail"]["gallery_status"], "not_found_in_html")

    def test_thumbnail_url_is_not_rewritten_to_guessed_original(self):
        images = extract_gallery(BeautifulSoup('<ul id="basicThumbSlideArea"><img src="//img.danawa.com/b.jpg?shrink=53:53"></ul>', 'html.parser'), {},
                                 'https://prod.danawa.com/info/?pcode=11')
        self.assertEqual(images[0]["url"], "https://img.danawa.com/b.jpg?shrink=53:53")
        self.assertEqual(images[0]["resolution_status"], "observed_thumbnail")

    def test_observed_api_price_image_and_short_date_mapping(self):
        node = product("11")
        node.update(price={"min": 1950500}, image={"url": "https://img.danawa.com/a.jpg?shrink=130:130"},
                    dates={"register": "22.01.", "make": "2022.01.25"})
        row = DanawaAdapter().parse_api_products([node], 1)[0]
        self.assertEqual(row["price_text"], "1,950,500원")
        self.assertEqual(row["image_url"], "https://img.danawa.com/a.jpg?shrink=130:130")
        self.assertIsNone(row["registered_month"])
        self.assertEqual(row["registered_month_text"], "22.01.")


if __name__ == '__main__':
    unittest.main()
