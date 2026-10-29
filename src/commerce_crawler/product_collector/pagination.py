"""Replay captured category-specific POST arguments; do not infer category codes."""
from __future__ import annotations

import copy
import json
import re
from pathlib import Path
from bs4 import BeautifulSoup


class ListingResponseError(ValueError):
    pass


def pagination_metadata(html: str, public_category_id: str) -> dict:
    """Read the current category view model from the server-rendered page.

    UI category codes come from uiCategoryHierarchyCodes, never by slicing cate.
    Only decoded JSON string chunks and explicit metadata are used.
    """
    decoder = json.JSONDecoder()
    chunks = []
    for script in BeautifulSoup(html, "html.parser").select("script"):
        text = script.get_text()
        for match in re.finditer(r"self\.__next_f\.push\(", text):
            try:
                value, _ = decoder.raw_decode(text[match.end():].lstrip())
            except ValueError:
                continue
            if isinstance(value, list) and len(value) > 1 and isinstance(value[1], str):
                chunks.append(value[1])
    stream = "".join(chunks)
    for match in re.finditer(r'"viewModel"\s*:\s*', stream):
        try:
            model, _ = decoder.raw_decode(stream[match.end():])
        except ValueError:
            continue
        if not isinstance(model, dict):
            continue
        current = model.get("currentCategory") or {}
        hierarchy = model.get("uiCategoryHierarchyCodes")
        physical = model.get("physicsCategory")
        if not isinstance(current, dict) or str(current.get("code")) != public_category_id:
            continue
        if not isinstance(hierarchy, list) or not hierarchy or not isinstance(physical, dict):
            continue
        code = str(hierarchy[-1])
        if not re.fullmatch(r"[0-9]+", code):
            continue
        fields = {key: physical.get(key) for key in ("physicsCate1", "physicsCate2", "physicsCate3", "physicsCate4")}
        if any(not isinstance(value, str) for value in fields.values()):
            continue
        if type(current.get("group")) is not int or type(current.get("depth")) is not int:
            continue
        multi = model.get("multiCategoryYN")
        if not isinstance(multi, str) or multi not in {"Y", "N"}:
            continue
        return {"public_category_id": public_category_id, "category_name": current.get("name"),
                "categoryCode": code, **fields, "group": current["group"], "depth": current["depth"],
                "bSearchOptionMultiCategoryYN": multi, "metadata_origin": "server_rendered.viewModel"}
    raise ValueError(f"cate={public_category_id}: complete pagination metadata not found in rendered page")


class PaginationTemplates:
    def __init__(self, config: dict):
        if not isinstance(config, dict) or not isinstance(config.get("categories"), dict):
            raise ValueError("pagination templates require a categories object")
        self.categories = copy.deepcopy(config["categories"])
        self.headers = config.get("headers", {})
        for headers in [self.headers, *[entry.get("headers", {}) for entry in self.categories.values() if isinstance(entry, dict)]]:
            if not isinstance(headers, dict) or any(not isinstance(k, str) or not isinstance(v, str) for k, v in headers.items()):
                raise ValueError("request headers must be string pairs")
            if any(k.lower() in {"cookie", "authorization", "host", "content-length"} for k in headers):
                raise ValueError("do not store Cookie/Authorization or computed Host/Content-Length in templates")
        for category_id, entry in self.categories.items():
            if not re.fullmatch(r"[0-9]+", category_id) or not isinstance(entry, dict):
                raise ValueError("template category keys must be numeric strings")
            payload = entry.get("payload")
            if not isinstance(payload, list) or not payload or not isinstance(payload[0], dict):
                raise ValueError(f"{category_id}: captured payload must be a nonempty array beginning with an object")
            arguments = payload[0]
            if not isinstance(arguments.get("categoryCode"), str) or not re.fullmatch(r"[0-9]+", arguments["categoryCode"]):
                raise ValueError(f"{category_id}: captured categoryCode must be a numeric string")
            for field in ("page", "listCount"):
                if type(arguments.get(field)) is not int or arguments[field] < 1:
                    raise ValueError(f"{category_id}: captured {field} must be positive")

    @classmethod
    def load(cls, path: Path):
        return cls(json.loads(path.read_text(encoding="utf-8-sig")))

    def supports(self, category_id: str) -> bool:
        return category_id in self.categories

    def request(self, category_id: str, page: int) -> tuple[list, dict]:
        if not self.supports(category_id):
            raise ValueError(f"no captured POST template for cate={category_id}")
        entry = self.categories[category_id]
        payload = copy.deepcopy(entry["payload"])
        payload[0]["page"] = page
        headers = {"Content-Type": "text/plain;charset=UTF-8", **self.headers, **entry.get("headers", {})}
        return payload, headers


def decode_products(response: str) -> tuple[list[dict], dict]:
    """Handle captured JSON records (0:/1:), not arbitrary React Flight records."""
    nodes = []
    try:
        nodes.append(json.loads(response))
    except ValueError:
        for line in response.splitlines():
            match = re.match(r"^[0-9a-f]+:(.*)$", line.strip(), re.IGNORECASE)
            if not match:
                continue
            try:
                nodes.append(json.loads(match.group(1)))
            except ValueError:
                continue
    for node in nodes:
        if not isinstance(node, dict):
            continue
        if node.get("success") is False:
            raise ListingResponseError("listing action reported success=false")
        data = node.get("data")
        if node.get("success") is True and isinstance(data, dict) and isinstance(data.get("products"), list):
            if any(not isinstance(p, dict) for p in data["products"]):
                raise ListingResponseError("products contains non-object records")
            return data["products"], node
    raise ListingResponseError("no success=true/data.products array; action headers or response format may have changed")
