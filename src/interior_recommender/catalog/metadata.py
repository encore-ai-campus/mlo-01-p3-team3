"""Conservative, deterministic normalization of collected product specifications."""
from collections import defaultdict
import math
import re


COLORS = {
    "black": ("black", "블랙", "검정", "검정색", "검은색"),
    "white": ("white", "화이트", "흰색", "백색", "무광화이트", "스노우화이트", "소프트화이트"),
    "gray": ("gray", "grey", "그레이", "회색", "다크그레이", "라이트그레이"),
    "brown": ("brown", "브라운", "갈색"), "beige": ("beige", "베이지"),
    "ivory": ("ivory", "아이보리"), "navy": ("navy", "네이비"),
    "blue": ("blue", "블루", "파랑", "파란색"), "pink": ("pink", "핑크", "분홍"),
    "green": ("green", "그린", "초록"), "red": ("red", "레드", "빨강"),
    "yellow": ("yellow", "옐로우", "노랑"),
}
MATERIALS = {
    "wood": ("wood", "원목", "목재"), "metal": ("metal", "금속", "철제", "스틸"),
    "glass": ("glass", "유리", "강화유리"), "ceramic": ("ceramic", "세라믹"),
    "fabric": ("fabric", "패브릭", "직물"), "mesh": ("mesh", "메쉬", "메시"),
    "synthetic_leather": ("인조가죽", "합성가죽", "synthetic_leather"),
    "leather": ("천연가죽", "leather"), "plastic": ("plastic", "플라스틱"),
    "mdf": ("mdf", "목재(mdf)"),
}
SHAPES = {"rectangle": ("사각형", "직사각형", "rectangle"), "square": ("정사각형", "square"),
          "round": ("원형", "round"), "oval": ("타원형", "oval"),
          "l_shape": ("ㄱ자형", "l자형", "l_shape"), "straight": ("일자형", "straight")}
AXES = {"가로": "width_mm", "폭": "width_mm", "너비": "width_mm",
        "깊이": "depth_mm", "세로": "depth_mm", "높이": "height_mm"}
UNITS = {"mm": 1, "cm": 10, "m": 1000}
NUMBER = r"\d+(?:\.\d+)?"
MEASUREMENT = re.compile(rf"\s*({NUMBER})\s*(mm|cm|m)?\s*(?:[~～–-]\s*({NUMBER})\s*(mm|cm|m)?)?\s*")

GROUPS = {
    "CHAIR": ("학생/사무용의자", "게이밍의자", "식탁의자", "간이/접이식의자", "다용도/야외의자",
              "상업용의자", "아동/유아의자", "안락의자/빈백", "좌식/자세교정의자", "회의실/학원의자", "스툴"),
    "SOFA": ("소파", "가죽소파", "패브릭소파", "좌식소파", "유아소파", "소파스툴", "소파베드"),
    "TABLE": ("식탁", "거실테이블", "상/테이블", "책상", "컴퓨터/게이밍책상", "학원/사무용책상", "모션데스크"),
    "BED": ("침대", "침대프레임", "2층/벙커침대", "모션베드"),
    "MATTRESS": ("매트리스/토퍼", "토퍼매트리스"),
    "CABINET": ("수납장", "주방수납장", "신발장", "TV거실장", "서랍장", "렌지대"),
    "WARDROBE": ("옷장", "붙박이장", "드레스룸"), "SHELF": ("선반", "책장"),
    "MIRROR": ("거울", "벽/전신거울"), "RUG": ("러그/카페트",), "HANGER": ("행거",),
}
CATEGORIES = {name: code for code, names in GROUPS.items() for name in names}


def compact(text):
    return re.sub(r"\s+", "", str(text)).casefold().replace("×", "x").replace("*", "x")


def measurement(value, default_unit=None):
    value = str(value).replace(",", "").replace("㎜", "mm").replace("㎝", "cm")
    match = MEASUREMENT.fullmatch(value)
    if not match:
        return None
    first, first_unit, second, second_unit = match.groups()
    first_unit = first_unit or second_unit or default_unit
    second_unit = second_unit or first_unit
    if first_unit not in UNITS or second_unit not in UNITS:
        return None
    low = float(first) * UNITS[first_unit]
    high = float(second if second is not None else first) * UNITS[second_unit]
    if not all(math.isfinite(n) and n > 0 for n in (low, high)) or low > high:
        return None
    return low, high


def specification_fields(row):
    fields = defaultdict(set)
    for item in row.get("specifications") or []:
        if item.get("name") and item.get("value") is not None:
            fields[str(item["name"]).strip()].add(str(item["value"]).strip())
    # The structured extractor may keep only the first option. Preserve the
    # complete matching field from spec_text as well, rather than choosing it.
    for section in re.split(r"\s+/\s+", row.get("spec_text") or ""):
        section = re.sub(r"^\s*\[[^]]+\]\s*", "", section)
        if ":" in section:
            name, value = section.split(":", 1)
            if name.strip() and value.strip():
                fields[name.strip()].add(value.strip())
    return fields


def normalize_specifications(row, category):
    raw = specification_fields(row)
    dimensions, components = defaultdict(set), defaultdict(set)
    attributes, part_attributes = defaultdict(set), defaultdict(set)
    issues = []

    def put_dimension(scope, axis, value):
        if value is None:
            issues.append(f"{scope}.{axis}: unsupported_measurement")
            return
        # Preserve obvious source anomalies in raw_attributes without guessing
        # a corrected endpoint (e.g. a desk height of 625~12750 mm).
        if axis == "height_mm" and category in {"TABLE", "CHAIR"} and value[1] > 3000:
            issues.append(f"{scope}.{axis}: source_value_requires_review")
            return
        target = dimensions if scope == "product" else components
        target[axis if scope == "product" else scope + "_" + axis].add(value)

    for label, values in raw.items():
        name = compact(label)
        scope = next((part for token, part in (("좌판", "seat"), ("등판", "back"),
                     ("포장", "package"), ("서랍", "drawer")) if token in name), "product")
        if "매트" in name and category != "MATTRESS":
            scope = "mattress"
        for value in sorted(values):
            axis_match = re.search(r"(가로|폭|너비|깊이|세로|높이)x(가로|폭|너비|깊이|세로|높이)x(가로|폭|너비|깊이|세로|높이)", name)
            if axis_match:
                axes = [AXES[a] for a in axis_match.groups()]
                pieces = re.split(r"[x×X*]", value)
                if len(pieces) == 3 and len(set(axes)) == 3:
                    unit_match = re.search(r"(mm|cm|m|㎜|㎝)\s*$", value)
                    unit = unit_match[1].replace("㎜", "mm").replace("㎝", "cm") if unit_match else None
                    for axis, piece in zip(axes, pieces):
                        put_dimension(scope, axis, measurement(piece, unit))
                else:
                    issues.append(f"{scope}: unsupported_dimension_tuple")
                continue
            # Explicit single-axis labels; a seat width must never become the
            # overall chair width used by the hard installation filter.
            single = re.sub(r"^(?:좌판|등판|매트리스|매트|포장|서랍)", "", name)
            single = re.sub(r"^(?:제품|전체|총)", "", single)
            if single in AXES:
                put_dimension(scope, AXES[single], measurement(value))

            attribute = ("color" if name == "색상" else
                         "material" if name in {"재질", "소재", "주요재질"} else
                         "shape" if name in {"형태", "모양", "상판형태"} else None)
            part_key = {"상판": "top_material", "상판재질": "top_material", "프레임재질": "frame_material",
                        "하부프레임형태": "frame_shape"}.get(name)
            if attribute or part_key:
                attribute_type = attribute or ("shape" if part_key.endswith("shape") else "material")
                aliases = {compact(alias): code for code, names in
                           {"color": COLORS, "material": MATERIALS, "shape": SHAPES}[attribute_type].items() for alias in names}
                for token in re.split(r"[,/|]", value):
                    token = token.strip()
                    if token:
                        normalized = aliases.get(compact(token), token)
                        (attributes[attribute] if attribute else part_attributes[part_key]).add(normalized)

    # Only exact, observed component-material descriptions, not visual inference.
    for section in re.split(r"\s+/\s+", row.get("spec_text") or ""):
        match = re.fullmatch(r"(메쉬|메시|패브릭|인조가죽|합성가죽|천연가죽)(등판|좌판)", section.strip())
        if match:
            code = next(code for code, names in MATERIALS.items() if match[1] in names)
            part_attributes[("back" if match[2] == "등판" else "seat") + "_material"].add(code)

    result = {"dimension_ranges_mm": {}, "component_dimensions_mm": {},
              "attribute_options": {}, "component_attributes": {k: sorted(v) for k, v in part_attributes.items()},
              "raw_attributes": {k: sorted(v) for k, v in raw.items()}, "metadata_issues": issues}
    for field in ("width_mm", "depth_mm", "height_mm"):
        result[field] = None
    for key, values in dimensions.items():
        if len(values) == 1 and not any(i.startswith("product." + key + ":") for i in issues):
            low, high = next(iter(values))
            result["dimension_ranges_mm"][key] = {"minimum": low, "maximum": high}
            result[key] = low if low == high else None
        else:
            result["metadata_issues"].append(f"product.{key}: conflicting_or_invalid_values")
    for key, values in components.items():
        if len(values) == 1 and not any(i.startswith(key.replace("_", ".", 1) + ":") for i in issues):
            low, high = next(iter(values))
            result["component_dimensions_mm"][key] = {"minimum": low, "maximum": high}
        else:
            result["metadata_issues"].append(f"{key}: conflicting_or_invalid_values")
    for field, vocabulary in (("color", COLORS), ("material", MATERIALS), ("shape", SHAPES)):
        options = attributes[field]
        result["attribute_options"][field] = sorted(options)
        result[field] = next(iter(options)) if len(options) == 1 and options <= vocabulary.keys() else None
    return result


def product_metadata(row, source):
    codes = {CATEGORIES[c["category_name"]] for c in row.get("categories") or []
             if c.get("category_name") in CATEGORIES}
    category = next(iter(codes)) if len(codes) == 1 else None
    prices = {p["amount"] for p in row.get("listing_prices") or []
              if p.get("currency") == "KRW" and p.get("kind") == "comparison_lowest"
              and isinstance(p.get("amount"), (int, float)) and not isinstance(p["amount"], bool)
              and math.isfinite(p["amount"]) and p["amount"] >= 0}
    return {"product_id": "danawa:" + str(row["product_id"]), "name": row["name"],
            "category_code": category, "price_krw": next(iter(prices)) if len(prices) == 1 else None,
            **normalize_specifications(row, category),
            "metadata_source": source, "observed_at": row.get("collected_at")}
