"""Danawa-specific extraction, adapted from the attached danawa_sample.py.

Retains Session workflow, both catalog selectors, pcode identity, lazy image
attributes, URL joining and per-listing deduplication. No seller sites fetched.
"""
from __future__ import annotations

import json
import re
from decimal import Decimal, InvalidOperation
from urllib.parse import parse_qs, parse_qsl, urljoin, urlparse

from bs4 import BeautifulSoup
from .pagination import PaginationTemplates, decode_products, pagination_metadata

BASE_URL = "https://prod.danawa.com/list/"


def image_identity(url: str) -> tuple:
    """Match observed renditions; never invent a full-resolution URL."""
    parsed = urlparse(url)
    query = tuple(sorted((k, v) for k, v in parse_qsl(parsed.query)
                         if k not in {"shrink", "width", "height", "w", "h", "_v"}))
    return parsed.netloc.lower(), parsed.path, query


def image_url(value, base_url: str) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    resolved = urljoin(base_url, value.strip())
    parsed = urlparse(resolved)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or "noimg" in parsed.path.lower():
        return None
    return resolved


def extract_gallery(soup: BeautifulSoup, product: dict | None, product_url: str) -> list[dict]:
    """Only the observed product gallery; description images are never enumerated."""
    renditions = (product or {}).get("image", [])
    if not isinstance(renditions, list):
        renditions = [renditions]
    observed = {}
    for value in renditions:
        candidate = value.get("contentUrl") or value.get("url") if isinstance(value, dict) else value
        url = image_url(candidate, product_url)
        if url:
            observed.setdefault(image_identity(url), url)
    base = soup.select_one("#baseImage")
    def node_url(node):
        for attribute in ("data-original", "data-src", "data-lazy-src", "src"):
            url = image_url(node.get(attribute), product_url)
            if url:
                return url
        return None
    base_url = node_url(base) if base else None
    base_identity = image_identity(base_url) if base_url else None
    nodes = list(soup.select("#basicThumbSlideArea img"))
    if base:
        nodes.append(base)
    gallery, seen = [], set()
    for node in nodes:
        original = node_url(node)
        if not original:
            continue
        identity = image_identity(original)
        if identity in seen:
            continue
        seen.add(identity)
        resolved = observed.get(identity) or (base_url if identity == base_identity else original)
        gallery.append({"url": resolved, "role": "representative" if identity == base_identity else "gallery",
                        "order": len(gallery),
                        "origin": "detail_gallery_matched_json_ld" if identity in observed else "detail_gallery_html",
                        "selector": "#baseImage" if node.get("id") == "baseImage" else "#basicThumbSlideArea img",
                        "observed_url": original,
                        "resolution_status": "observed_json_ld_rendition" if identity in observed
                                             else "observed_display_rendition" if identity == base_identity else "observed_thumbnail"})
    return gallery


def validate_identity(html: str, code: str) -> None:
    soup = BeautifulSoup(html, "html.parser")
    page_codes = set()
    for node in soup.select('link[rel="canonical"][href], meta[property="og:url"][content]'):
        url = node.get("href") or node.get("content")
        observed = product_id_from_url(url)
        if observed and observed != code:
            raise ValueError(f"detail identity mismatch: requested {code}, observed {observed}")
        page_codes.add(observed)
    # The page's own URL confirming the product settles it: other Products are recommendations, and the
    # gallery (#baseImage) belongs to the page. Without it nothing tells the primary Product apart, so an
    # explicit foreign Product identity rejects the page. A seller sku is no ID.
    if code in page_codes:
        return
    codes = set()
    for script in soup.select('script[type="application/ld+json"]'):
        try:
            products = [p for p in walk_objects(json.loads(script.get_text())) if has_type(p, "Product")]
        except (ValueError, TypeError):
            continue
        for product in products:
            urls = [product.get(key) for key in ("url", "@id") if isinstance(product.get(key), str)]
            codes.add(next((c for c in map(product_id_from_url, urls) if c), "") or str(product.get("productID") or ""))
    codes.discard("")
    if codes and codes != {code}:
        raise ValueError(f"detail Product identity mismatch: {code}")


def api_product_identity(product: dict) -> tuple[str, str | None, dict]:
    code = str(product.get("productCode", ""))
    name = product.get("name") if isinstance(product.get("name"), dict) else {}
    return code, text_value(name.get("full")) or text_value(name.get("product")), name


def rejected_api_products(products: list[dict]) -> dict:
    """Counted before filtering: a renamed field drops products before any saved record could show it."""
    counts = {"invalid_code": 0, "missing_name": 0}
    for product in products:
        code, full_name, _ = api_product_identity(product)
        if not re.fullmatch(r"[0-9]+", code):
            counts["invalid_code"] += 1
        elif not full_name:
            counts["missing_name"] += 1
    return counts


def product_id_from_url(url: str) -> str:
    parsed = urlparse(url)
    query_id = parse_qs(parsed.query).get("pcode", [""])[0]
    if query_id:
        return query_id
    match = re.search(r"/(\d+)(?:/|$)", parsed.path)
    return match.group(1) if match else ""


def first_text(root, selectors: tuple[str, ...]) -> str:
    for selector in selectors:
        element = root.select_one(selector)
        if element:
            return element.get_text(" ", strip=True)
    return ""


def text_value(value) -> str | None:
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, dict):
        return text_value(value.get("name"))
    return None


def amount(value) -> int | str | None:
    if isinstance(value, bool) or value is None:
        return None
    token = str(value).strip().replace(",", "")
    if not re.fullmatch(r"\d+(?:\.\d+)?", token):
        return None
    try:
        number = Decimal(token)
    except InvalidOperation:
        return None
    return int(number) if number == number.to_integral_value() else str(number)


def walk_objects(value):
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from walk_objects(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from walk_objects(nested)


def has_type(node: dict, expected: str) -> bool:
    types = node.get("@type", [])
    if isinstance(types, str):
        types = [types]
    return isinstance(types, list) and any(
        isinstance(t, str) and t.rstrip("/").split("/")[-1] == expected for t in types)


def select_product(blocks: list, listing: dict) -> tuple[dict | None, str | None]:
    candidates = [n for block in blocks for n in walk_objects(block) if has_type(n, "Product")]
    for node in candidates:
        offer_nodes = list(walk_objects(node.get("offers")))
        urls = [node.get("url"), node.get("@id"), *[n.get("url") for n in offer_nodes]]
        codes = [product_id_from_url(u) for u in urls if isinstance(u, str)]
        if listing["product_id"] in codes:
            return node, "pcode"
        if any(codes):
            continue  # explicitly points at a different Danawa product
        if str(node.get("productID", "")) == listing["product_id"]:
            return node, "productID"
        if str(node.get("sku", "")) == listing["product_id"]:
            return node, "sku"
    matches = [n for n in candidates if not any(
        product_id_from_url(u) for u in (n.get("url"), n.get("@id")) if isinstance(u, str)
    ) and text_value(n.get("name")) == listing["product_name"]]
    return (matches[0], "name") if len(matches) == 1 else (None, None)


def listing_products(soup: BeautifulSoup, limit: int | None) -> list[dict]:
    rows, seen = [], set()
    items = soup.select("#productListArea .dnw-catalog-item") or soup.select("li.prod_item")
    for item in items:
        name_link = None
        # Current cards put an image-view button inside the first pcode anchor.
        preferred = item.select('[data-testid="ProductListTitle"] a[href], .prod_name a[href]')
        for anchor in preferred or item.select("a[href]"):
            text = anchor.get_text(" ", strip=True)
            if not text or "관심상품" in text or text.startswith("판매가") or text in {"가격비교", "이미지 보기"}:
                continue
            if parse_qs(urlparse(anchor.get("href", "")).query).get("pcode"):
                name_link = anchor
                break
        if name_link is None:
            continue
        product_id = product_id_from_url(urljoin(BASE_URL, name_link.get("href", "")))
        if not re.fullmatch(r"[0-9]+", product_id) or product_id in seen:
            continue
        seen.add(product_id)
        # Canonical origin/path prevents listing links from requesting third-party hosts.
        detail_url = f"https://prod.danawa.com/info/?pcode={product_id}"
        image_url = None
        image = item.select_one(".dnw-product-image-item img, img")
        if image is not None:
            for attribute in ("data-original", "data-src", "data-lazy-src", "src"):
                value = image.get(attribute)
                if value and not value.strip().startswith("data:"):
                    resolved = urljoin(detail_url, value.strip())
                    if urlparse(resolved).scheme in {"http", "https"}:
                        image_url = resolved
                        break
        # Do not match an arbitrary amount (e.g. shipping) in the whole card.
        variants = []
        price_text = ""
        for anchor in item.select('.dnw-product-price a[href*="pcode="]'):
            candidate = anchor.get_text(" ", strip=True)
            code = product_id_from_url(anchor.get("href", ""))
            if not re.fullmatch(r"[0-9][0-9,]*\s*원", candidate):
                continue
            variants.append({"product_id": code, "label": anchor.get("aria-label"), "price_text": candidate})
            if code == product_id and not price_text:
                price_text = candidate
        if not price_text:
            price_text = first_text(item, (".price_sect .price", ".price_sect strong", ".dnw-price"))
        if not price_text:
            explicit = re.search(r"(?:판매가|최저가)\s*([0-9][0-9,]*)\s*원", item.get_text(" ", strip=True))
            price_text = explicit.group(0) if explicit else ""
        month = re.search(r"등록월\s*:?\s*([0-9]{4}\.[0-9]{2}\.?)", item.get_text(" ", strip=True))
        short_month = re.search(r"[0-9]{2}\.[0-9]{2}\.\s*등록", item.get_text(" ", strip=True))
        rows.append({"product_id": product_id, "product_name": name_link.get_text(" ", strip=True),
                     "product_url": detail_url, "image_url": image_url, "price_text": price_text,
                     "price_kind": "comparison_lowest" if "최저가" in price_text or variants else "listing_display",
                     "variant_prices": variants,
                     "spec_text": first_text(item, ('[data-testid="ProductListSpecs"]', ".spec_list", ".dnw-product-spec", ".dnw-product-spec-list")),
                     "registered_month": month.group(1) if month else None,
                     "registered_month_text": month.group(0) if month else short_month.group(0) if short_month else None,
                     "html": str(item)})
        if limit is not None and len(rows) >= limit:
            break
    return rows


class DanawaAdapter:
    source = "danawa"

    def __init__(self, pagination_templates: PaginationTemplates | None = None, discover_categories: bool = False):
        self.pagination_templates = pagination_templates
        self.discover_categories = discover_categories

    def restore_pagination(self, report):
        """Reuse observed category arguments after restart; keep fresh local headers."""
        if not self.pagination_templates or not self.pagination_templates.categories:
            return
        seed = next(iter(self.pagination_templates.categories))
        _, headers = self.pagination_templates.request(seed, 1)
        names = {c['category_id']: c.get('observed_category_name') for c in report.get('categories', [])}
        for attempt in report.get('requests', []):
            url = attempt.get('url', '')
            code = parse_qs(urlparse(url).query).get('cate', [None])[0]
            if attempt.get('method') == 'POST' and code and not self.pagination_templates.supports(code) and attempt.get('payload'):
                self.pagination_templates.categories[code] = {'payload': attempt['payload'],
                    'headers': {**headers, 'Referer': url}, 'metadata': {'category_name': names.get(code)}}

    def validate_categories(self, categories) -> None:
        for category in categories:
            if (category.start_page > 1 or category.max_pages is None or category.max_pages > 1) and not (
                self.pagination_templates and self.pagination_templates.supports(category.category_id)
            ):
                if self.discover_categories and self.pagination_templates and self.pagination_templates.categories:
                    continue
                raise ValueError(f"cate={category.category_id}: pagination requires its own captured POST template")

    def fetch_listing_page(self, client, category, page: int) -> tuple[list[dict], dict]:
        url = self.category_url(category.category_id)
        if self.discover_categories and self.pagination_templates and not self.pagination_templates.supports(category.category_id):
            # Only common arguments/Next-Action come from the seed. All category
            # parameters must be observed on THIS category's page before POST.
            seed = next(iter(self.pagination_templates.categories))
            payload, headers = self.pagination_templates.request(seed, page)
            metadata = pagination_metadata(client.get(url), category.category_id)
            for key in ('categoryCode', 'physicsCate1', 'physicsCate2', 'physicsCate3', 'physicsCate4',
                        'group', 'depth', 'bSearchOptionMultiCategoryYN'):
                payload[0][key] = metadata[key]
            self.pagination_templates.categories[category.category_id] = {
                'payload': payload, 'headers': {**headers, 'Referer': url}, 'metadata': metadata}
        if self.pagination_templates and self.pagination_templates.supports(category.category_id):
            payload, headers = self.pagination_templates.request(category.category_id, page)
            response = client.post(url, payload, headers)
            products, _ = decode_products(response)
            rows = self.parse_api_products(products, category.limit)
            return rows, {"format": "action_products", "response_product_count": len(products),
                          "parsed_product_count": len(rows), "rejected_products": rejected_api_products(products),
                          "observed_category_name":
                          self.pagination_templates.categories[category.category_id].get('metadata', {}).get('category_name'),
                          "public_category_id": category.category_id,
                          "payload_category_code": payload[0]["categoryCode"], "page": page}
        if page != 1:
            raise ValueError("GET listing supports only page 1")
        return self.parse_listing(client.get(url), category.limit)

    def parse_api_products(self, products: list[dict], limit: int | None) -> list[dict]:
        rows, seen = [], set()
        for product in products:
            code, full_name, name = api_product_identity(product)
            if not re.fullmatch(r"[0-9]+", code) or not full_name or code in seen:
                continue
            seen.add(code)
            description = product.get("description") if isinstance(product.get("description"), dict) else {}
            pc = description.get("pc") if isinstance(description.get("pc"), dict) else {}
            prices = product.get("price") if isinstance(product.get("price"), dict) else {}
            minimum = amount(prices.get("min"))
            source_image = product.get("image") if isinstance(product.get("image"), dict) else {}
            dates = product.get("dates") if isinstance(product.get("dates"), dict) else {}
            month_text = text_value(dates.get("register"))
            exact_month = re.fullmatch(r"[0-9]{4}\.[0-9]{2}\.?", month_text or "")
            canonical_url = f"https://prod.danawa.com/info/?pcode={code}"
            rows.append({"product_id": code, "product_name": full_name,
                         "product_url": canonical_url,
                         "manufacturer": text_value(name.get("maker")), "brand": text_value(name.get("brand")),
                         "model": text_value(name.get("model")), "description": text_value(pc.get("raw")),
                         "spec_text": text_value(pc.get("raw")),
                         "price_text": f"{minimum:,}원" if isinstance(minimum, int) else f"{minimum}원" if minimum else "",
                         "price_kind": "comparison_lowest",
                         "image_url": image_url(source_image.get("url"), canonical_url),
                         "registered_month": month_text if exact_month else None, "registered_month_text": month_text,
                         "variant_prices": [], "api_product": product, "format": "action_product"})
            if limit is not None and len(rows) >= limit:
                break
        return rows

    def category_url(self, category_id: str) -> str:
        return f"{BASE_URL}?cate={category_id}"

    def parse_listing(self, html: str, limit: int | None) -> tuple[list[dict], dict]:
        soup = BeautifulSoup(html, "html.parser")
        diagnostics = {"page_title": soup.title.get_text(" ", strip=True) if soup.title else None,
                       "current_cards": len(soup.select("#productListArea .dnw-catalog-item")),
                       "legacy_cards": len(soup.select("li.prod_item"))}
        name = re.fullmatch(r"(.+?)\s*:\s*다나와 가격비교", diagnostics["page_title"] or "")
        diagnostics["observed_category_name"] = name.group(1).strip() if name else None
        return listing_products(soup, limit), diagnostics

    def identity_error(self, html: str, listing: dict) -> str | None:
        try:
            validate_identity(html, str(listing["product_id"]))
        except ValueError as exc:
            return str(exc)
        return None

    def needs_detail(self, listing: dict) -> bool:
        # Gallery URLs require a detail response even when POST supplies basic fields.
        return True

    def parse_detail(self, html: str, listing: dict) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        blocks, scripts, errors = [], [], []
        for script in soup.select('script[type="application/ld+json"]'):
            original = script.get_text()
            scripts.append(original)
            try:
                def reject_constant(value):
                    raise ValueError(f"non-JSON constant: {value}")
                blocks.append(json.loads(original, parse_constant=reject_constant))
            except (ValueError, TypeError) as exc:
                errors.append(str(exc))
        product, matched_by = select_product(blocks, listing)
        gallery = extract_gallery(soup, product, listing["product_url"])
        # Keep all parsed JSON-LD and script strings even if selection/decoding fails.
        # The full page is local evidence (storage.save_detail_html); its HTML fragments are not kept.
        specs = []
        for table in soup.select(".spec_tbl, .spec_table, .prod_spec table, #productDescription table"):
            for row in table.select("tr"):
                pending = None
                for cell in row.find_all(["th", "td"], recursive=False):
                    value = cell.get_text(" ", strip=True)
                    if cell.name == "th":
                        pending = value
                    elif pending and value:
                        specs.append({"name": pending, "value": value})
                        pending = None
        description_node = soup.select_one("#productDescription, .prod_description, .product_description")
        description_html = str(description_node) if description_node else None
        spec_node = soup.select_one(".spec_set .spec_list")
        spec_text = spec_node.get_text(" ", strip=True) if spec_node else None
        # Explicit label/value segments, preserving units and composite dimensions.
        if spec_text:
            for segment in re.split(r"\s+/\s+", re.sub(r"\[[^]]*\]", "", spec_text)):
                for match in re.finditer(r"([^,:]+?)\s*:\s*([^,]+)", segment):
                    specs.append({"name": match.group(1).strip(), "value": match.group(2).strip()})
        made_node = soup.select_one(".made_info")
        made_text = made_node.get_text(" ", strip=True) if made_node else ""
        manufacturer = first_text(soup, ("#makerTxtArea a",))
        if not manufacturer and made_node:
            for node in made_node.select(".txt"):
                match = re.fullmatch(r"제조사\s*:\s*(.+)", node.get_text(" ", strip=True))
                if match:
                    manufacturer = match.group(1)
                    break
        month = re.search(r"등록월\s*:\s*([0-9]{4}\.[0-9]{2}\.?)", made_text)
        seller_offers = []
        for item in soup.select(".list__mall-price > li"):
            logo = item.select_one(".box__logo img[alt]")
            price = item.select_one(".sell-price")
            link = item.select_one("a.link__full-cover[href]")
            url = urljoin(listing["product_url"], link.get("href", "")) if link else None
            seller_id = parse_qs(urlparse(url).query).get("cmpnyc", [None])[0] if url else None
            seller_offers.append({"seller_name": logo.get("alt") if logo else None, "seller_id": seller_id,
                                  "url": url, "price_text": price.get_text(" ", strip=True) if price else None,
                                  "price_attribute": price.get("data-base-price") if price else None,
                                  "shipping_fee_attribute": price.get("data-delivery-price") if price else None,
                                  "shipping_text": first_text(item, (".box__delivery",)) or None})
        metadata = {m.get("name") or m.get("property"): m.get("content") for m in soup.select('meta[content]')
                    if (m.get("name") or "").lower() in {"keywords", "description", "title"}
                    or (m.get("property") or "").startswith("og:")}
        title = soup.title.get_text(" ", strip=True) if soup.title else None
        recognized = bool(product or specs or spec_text or manufacturer or description_node or seller_offers)
        # description_html stays: normalize() falls back to its text.
        return {"status": "ok" if recognized else "unrecognized",
                "page_title": title, "json_ld_scripts": scripts, "json_ld": blocks,
                "json_ld_errors": errors, "selected_product": product, "matched_by": matched_by,
                "specifications": specs,
                "description_html": description_html,
                "spec_text": spec_text, "manufacturer": manufacturer or None,
                "registered_month": month.group(1) if month else None,
                "seller_offers": seller_offers,
                "metadata": metadata,
                "gallery_images": gallery,
                "gallery_status": "observed" if gallery else "not_found_in_html"}

    def normalize(self, raw: dict) -> dict:
        listings = raw["payload"]["listings"]
        listing = listings[0]
        detail = raw["payload"]["detail"] or {}
        product = detail.get("selected_product") or {}
        fields, provenance = {}, {}

        def put(field, value, origin):
            fields[field] = value if value not in ("", [], {}) else None
            provenance[field] = origin if fields[field] is not None else None

        put("name", text_value(product.get("name")) or listing["product_name"],
            "detail.json_ld.Product.name" if text_value(product.get("name")) else "listing.product_name")
        put("product_url", listing["product_url"], "listing.product_url")
        put("registered_month", detail.get("registered_month") or listing["registered_month"],
            "detail.made_info" if detail.get("registered_month") else "listing.registered_month")
        for field, key, labels in (
            ("manufacturer", "manufacturer", {"제조사", "제조자"}),
            ("model", "model", {"모델명", "모델"}),
            ("country_of_origin", "countryOfOrigin", {"원산지", "제조국"}),
            ("size_text", "size", {"크기", "크기(가로x세로x높이)", "크기(가로x세로x깊이)"}),
            ("weight_text", "weight", {"무게", "중량"}),
        ):
            value = text_value(product.get(key))
            origin = f"detail.json_ld.Product.{key}"
            if not value:
                value = next((s["value"] for s in detail.get("specifications", []) if s["name"] in labels), None)
                origin = "detail.specifications"
            if field == "manufacturer" and not value:
                value, origin = detail.get("manufacturer"), "detail.made_info"
            if field == "size_text" and not value:
                value = next((s["value"] for s in detail.get("specifications", [])
                              if "크기" in s["name"] or "가로x세로" in s["name"]), None)
                origin = "detail.specifications"
            if not value and field in {"manufacturer", "model"}:
                value, origin = listing.get(field), f"listing.api_product.name.{ 'maker' if field == 'manufacturer' else 'model'}"
            put(field, value, origin)
        put("brand", text_value(product.get("brand")) or listing.get("brand"),
            "detail.json_ld.Product.brand" if text_value(product.get("brand")) else "listing.api_product.name.brand")
        description = text_value(product.get("description"))
        if description:
            put("description", description, "detail.json_ld.Product.description")
        else:
            description = BeautifulSoup(detail.get("description_html") or "", "html.parser").get_text(" ", strip=True)
            put("description", description or listing.get("description"),
                "detail.description_html" if description else "listing.api_product.description.pc.raw")
        specs = list(detail.get("specifications", []))
        additional = product.get("additionalProperty", [])
        if isinstance(additional, dict):
            additional = [additional]
        if isinstance(additional, list):
            for entry in additional:
                if isinstance(entry, dict) and text_value(entry.get("name")):
                    specs.append({"name": entry["name"], "value": entry.get("value"),
                                  "unit_text": entry.get("unitText")})
        put("specifications", specs, "detail.specifications_or_json_ld")
        put("spec_text", detail.get("spec_text") or listing.get("spec_text"),
            "detail.spec_text" if detail.get("spec_text") else "listing.spec_text")
        keyword_text = text_value(product.get("keywords")) or next(
            (v for k, v in detail.get("metadata", {}).items() if k.lower() == "keywords"), None)
        put("keywords_text", keyword_text, "detail.json_ld_or_metadata.keywords")
        gallery = detail.get("gallery_images", [])
        put("images", gallery, "detail.gallery_images")
        put("listing_image_url", listing.get("image_url"), "listing.image_url")

        offers = []
        references = {n["@id"]: n for block in detail.get("json_ld", []) for n in walk_objects(block)
                      if isinstance(n.get("@id"), str) and len(n) > 1}
        def resolve(value):
            return references.get(value.get("@id"), value) if isinstance(value, dict) else value
        visited_offers = set()
        def extract_offer(value):
            if isinstance(value, list):
                for item in value:
                    extract_offer(item)
                return
            value = resolve(value)
            if not isinstance(value, dict):
                return
            if id(value) in visited_offers:
                return
            visited_offers.add(id(value))
            if has_type(value, "AggregateOffer"):
                # Aggregate seller count is not inventory or minimum order quantity.
                offers.append({"kind": "aggregate", "low_price": amount(value.get("lowPrice")),
                               "high_price": amount(value.get("highPrice")),
                               "currency": text_value(value.get("priceCurrency")),
                               "offer_count": amount(value.get("offerCount")),
                               "provenance": "detail.json_ld.Product.offers"})
                children = value.get("offers")
                if children is not value:
                    extract_offer(children)
            elif has_type(value, "Offer") or "price" in value:
                seller = resolve(value.get("seller"))
                offers.append({"kind": "seller_offer", "price": amount(value.get("price")),
                               "currency": text_value(value.get("priceCurrency")),
                               "seller_name": text_value(seller),
                               "seller_id": seller.get("@id") if isinstance(seller, dict) else None,
                               "url": text_value(value.get("url")),
                               "availability": text_value(value.get("availability")),
                               "shipping": value.get("shippingDetails"),
                               "return_policy": value.get("hasMerchantReturnPolicy"),
                               "provenance": "detail.json_ld.Product.offers"})
        extract_offer(product.get("offers"))
        for entry in detail.get("seller_offers", []):
            offers.append({"kind": "seller_offer", "seller_name": entry["seller_name"],
                           "seller_id": entry["seller_id"], "url": entry["url"],
                           "price": amount(entry["price_attribute"]), "currency": "KRW",
                           "shipping_fee": amount(entry["shipping_fee_attribute"]),
                           "shipping_text": entry["shipping_text"],
                           "provenance": "detail.seller_offers"})
        put("offers", offers, "detail.json_ld.Product.offers_or_seller_offers")
        prices = []
        for entry in listings:
            # Numeric conversion only for a single, clearly expressed amount.
            token = entry.get("price_text", "").strip()
            match = re.fullmatch(r"(?:판매가\s*|최저가\s*)?([0-9][0-9,]*)(?:\s*원)?", token)
            prices.append({"category_id": entry["category"]["category_id"], "text": token or None,
                           "amount": amount(match.group(1)) if match else None,
                           "currency": "KRW" if match else None, "kind": entry["price_kind"],
                           "provenance": "listing.price_text"})
        fields["listing_prices"] = prices
        provenance["listing_prices"] = "payload.listings.price_text"
        field_status = {}
        for field, value in fields.items():
            if value is not None:
                field_status[field] = "observed"
            elif raw["detail_status"] in {"request_failed", "budget_exhausted", "not_requested", "disabled"}:
                field_status[field] = "detail_" + raw["detail_status"]
            elif raw["detail_status"] == "unrecognized":
                field_status[field] = "parser_unrecognized"
            elif raw["detail_status"] == "identity_mismatch":
                field_status[field] = "detail_identity_mismatch"
            else:
                field_status[field] = "not_found_in_extracted_content"
        return {"source": self.source, "product_id": raw["product_id"], "job_id": raw["job_id"],
                "schema_version": "1.2", "collected_at": raw["collected_at"],
                "categories": raw["categories"], "raw_payload_hash": raw["payload_hash"],
                **fields, "provenance": provenance, "field_status": field_status,
                "detail_status": raw["detail_status"]}
