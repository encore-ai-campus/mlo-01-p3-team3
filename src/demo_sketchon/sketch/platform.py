"""Application DTOs and replaceable Ad provider. No browser-to-Ad-server connection.

The default provider is a clearly labelled offline mock, not a FAISS search.
Set settings.ROOMPICK_RECOMMENDATION_PROVIDER to an importable class implementing
recommend(*, scene_object, request_id, context) and resolve_link(*, item, link_kind).
A real provider must call the team's authenticated Ad API with bounded timeouts,
retain the request_id on retry, and return validated catalog data, never AI prices.
"""
from hashlib import sha256
from uuid import UUID, uuid5
from urllib.parse import urlsplit
from django.conf import settings
from django.utils.module_loading import import_string
from .catalog import CATEGORIES


def generation_context(payload):
    text = payload.get("text_requirements", "")
    preferences = payload.get("style_preferences", [])
    if not isinstance(text, str) or len(text) > 1000:
        raise ValueError("공간 설명은 1,000자 이내로 입력해 주세요.")
    if not isinstance(preferences, list) or len(preferences) > 1 or any(not isinstance(x, str) or x not in {"크림 패브릭", "오크 우드", "세이지 그린"} for x in preferences):
        raise ValueError("분위기 선택값을 확인해 주세요.")
    result = {}
    if text.strip(): result["text_requirements"] = text.strip()
    if preferences: result["style_preferences"] = sorted(set(preferences))
    return result


def scene_objects(result):
    from .models import SceneObjectRevision
    revisions = {}
    for revision in SceneObjectRevision.objects.filter(composition=result, base_version__lte=result.version).order_by("-created_at", "-id"):
        revisions.setdefault(revision.placement_id, revision)
    rows = []
    for row in result.placements:
        revision = revisions.get(row["id"])
        current = revision and revision.base_version == result.version
        category = revision.category_id if revision else row["category"]
        identifier = revision.id if current else uuid5(revision.id if revision else result.id, f"object:{result.version}:{row['id']}")
        rows.append({"id": str(identifier), "placement_id": row["id"], "scene_generation_id": str(result.id),
            "category_id": category, "label": CATEGORIES[category], "change_type": "ADDED",
            "classification_basis": "DEMO_ASSUMPTION", "user_corrected": bool(revision),
            "bbox": revision.bbox if current else {k: row[k] for k in ("x", "y", "width", "height")},
            "preview_url": f"/api/results/{result.id}/sprites/{row['id']}/?v={result.version}", "version": result.version})
    return rows


class DemoRecommendationProvider:
    def recommend(self, *, scene_object, request_id, context):
        label = scene_object["label"]
        return {"mode": "mock", "items": [
            {"sample_key": f"sample-{i}", "product_id": None,
             "name": f"{label} · {style}", "brand": "SketchOn 예시",
             "image_url": f"/api/results/{scene_object['scene_generation_id']}/samples/{scene_object['placement_id']}/{variant}/?category={scene_object['category_id']}", "price": None,
             "price_note": "실제 판매 상품·가격이 아닙니다",
             "link_available": False, "source": "demo"}
            for i, (style, variant) in enumerate((("크림 패브릭", "cream"), ("웜 내추럴", "wood"), ("세이지 그린", "sage")), 1)],
            "notice": "샘플 카드입니다. 실제 유사도 검색이나 판매 상품 추천은 수행하지 않았습니다.",
            "index_version_id": None}

    def resolve_link(self, *, item, link_kind):
        raise ValueError("예시 상품에는 판매 링크가 없습니다.")


def provider():
    cls = getattr(settings, "ROOMPICK_RECOMMENDATION_PROVIDER", "sketch.platform.DemoRecommendationProvider")
    return import_string(cls)()


def safe_media_url(value):
    if not isinstance(value, str): raise ValueError("이미지 주소가 필요합니다.")
    if any(ord(char) <= 32 for char in value) or "\\" in value:
        raise ValueError("이미지 주소에 잘못된 문자가 있습니다.")
    parsed = urlsplit(value)
    if value.startswith("/") and not value.startswith("//") and not parsed.netloc and not parsed.scheme and "\\" not in value:
        return value
    if parsed.scheme == "https" and parsed.hostname and not parsed.username and not parsed.password:
        return value
    raise ValueError("안전한 이미지 주소가 필요합니다.")


def normalize_recommendations(data, request_id):
    if not isinstance(data, dict) or data.get("mode") not in {"mock", "live"}:
        raise ValueError("추천 제공자의 응답 형식을 확인해 주세요.")
    rows = data.get("items")
    if not isinstance(rows, list) or len(rows) > 20: raise ValueError("추천 결과 수가 올바르지 않습니다.")
    items = []
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or not isinstance(row.get("name"), str) or not 1 <= len(row["name"]) <= 200:
            raise ValueError("추천 상품명이 올바르지 않습니다.")
        product_id = str(UUID(str(row.get("product_id")))) if data["mode"] == "live" else None
        price = row.get("price")
        if price is not None and (type(price) is not int or price < 0): raise ValueError("상품 가격이 올바르지 않습니다.")
        items.append({"id": str(uuid5(request_id, f"item:{index}:{product_id or row.get('sample_key', index)}")),
            "product_id": product_id, "name": row["name"], "brand": str(row.get("brand", ""))[:100],
            "image_url": safe_media_url(row.get("image_url")), "price": price if data["mode"] == "live" else None,
            "price_note": str(row.get("price_note", ""))[:200], "source": str(row.get("source", ""))[:80],
            "link_available": data["mode"] == "live" and row.get("link_available") is True})
    return items


def validated_link(url):
    if not isinstance(url, str) or any(ord(char) <= 32 for char in url) or "\\" in url:
        raise ValueError("상품 주소가 올바르지 않습니다.")
    parsed = urlsplit(url)
    hosts = getattr(settings, "ROOMPICK_PRODUCT_LINK_HOSTS", ("prod.danawa.com", "shop.danawa.com"))
    if parsed.scheme != "https" or parsed.hostname not in hosts or parsed.username or parsed.password or parsed.port not in (None, 443) or "\\" in url:
        raise ValueError("확인된 상품 연결 주소가 아닙니다.")
    return url
