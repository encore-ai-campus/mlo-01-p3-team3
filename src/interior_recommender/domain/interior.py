"""Versioned, explainable soft interior preferences; no inference or measurements."""
from pydantic import Field, model_validator

from ..schemas import Contract


INTERIOR_VERSION = "room-compatibility-v1"
FEATURE_WEIGHTS = {"palette": .5, "materials": .25, "shapes": .15, "styles": .1}
ALIASES = {
    "palette": {
        "white": "white", "화이트": "white", "흰색": "white",
        "ivory": "ivory", "cream": "ivory", "아이보리": "ivory", "크림": "ivory",
        "beige": "beige", "베이지": "beige", "brown": "brown", "갈색": "brown", "브라운": "brown",
        "black": "black", "블랙": "black", "검정": "black", "gray": "gray", "grey": "gray", "그레이": "gray",
        "green": "green", "그린": "green", "blue": "blue", "블루": "blue", "navy": "navy",
        "pink": "pink", "red": "red", "yellow": "yellow",
    },
    "materials": {
        "wood": "wood", "원목": "wood", "목재": "wood", "metal": "metal", "금속": "metal", "스틸": "metal",
        "fabric": "fabric", "패브릭": "fabric", "leather": "leather", "천연가죽": "leather",
        "synthetic_leather": "synthetic_leather", "인조가죽": "synthetic_leather",
        "glass": "glass", "유리": "glass", "plastic": "plastic", "플라스틱": "plastic",
        "mesh": "mesh", "메쉬": "mesh", "ceramic": "ceramic", "세라믹": "ceramic", "mdf": "mdf",
    },
    "shapes": {k: k for k in ("round", "oval", "rectangle", "square", "l_shape", "straight")},
    "styles": {k: k for k in ("minimal", "modern", "scandinavian", "natural", "industrial", "classic", "vintage")},
}
# Explicit manual palette affinities, not learned probabilities or design guarantees.
COLOR_FAMILIES = ({"white", "ivory", "beige"}, {"beige", "brown"}, {"black", "gray"}, {"blue", "navy"})


def canonical_values(field, values):
    return sorted({ALIASES[field][str(v).strip().casefold()] for v in values
                   if str(v).strip().casefold() in ALIASES[field]})


class InteriorProfile(Contract):
    palette: list[str] = Field(default_factory=list, max_length=12)
    materials: list[str] = Field(default_factory=list, max_length=12)
    shapes: list[str] = Field(default_factory=list, max_length=12)
    styles: list[str] = Field(default_factory=list, max_length=12)
    evidence: list[str] = Field(default_factory=list, max_length=12)
    uncertainties: list[str] = Field(default_factory=list, max_length=12)

    @model_validator(mode="after")
    def bounded(self):
        if any(len(v) > 500 for values in self.model_dump().values() for v in values):
            raise ValueError("Interior profile text is too long")
        if any(getattr(self, key) for key in FEATURE_WEIGHTS) and not self.evidence:
            raise ValueError("Interior features require observation/source evidence")
        return self


def product_profile(product):
    """Use sourced scalar metadata and confirmed whole-product, unambiguous attributes.

    Multiple options/component values are retained by Product but never promoted to
    a verified whole-product feature. A source code alone is not confirmation.
    """
    values = {key: list(getattr(product.interior_profile, key)) for key in FEATURE_WEIGHTS}
    evidence = list(product.interior_profile.evidence)
    for field, key in (("color", "palette"), ("material", "materials"), ("shape", "shapes")):
        value = getattr(product, field)
        if value is not None:
            values[key].append(value)
    grouped = {}
    invalid = set()
    supplied = set()
    for attr in product.attributes_json:
        key = {"COLOR": "palette", "MATERIAL": "materials", "SHAPE": "shapes", "STYLE": "styles"}.get(attr.get("attribute_name"))
        if key is None:
            continue
        supplied.add(key)
        proof = attr.get("source_evidence_key")
        # An unresolved option cannot contribute its codes as a certain match.
        if (attr.get("attribute_status") != "CONFIRMED"
                or attr.get("measurement_target") != "PRODUCT" or attr.get("component")
                or attr.get("option_key") or attr.get("option_status") != "NO_OPTION_CONFIRMED"
                or not isinstance(proof, str) or not proof.strip() or len(proof) > 500):
            invalid.add(key)
            continue
        codes = attr.get("codes", [])
        if not isinstance(codes, list):
            invalid.add(key)
            continue
        grouped.setdefault(key, []).append(canonical_values(key, codes))
        if proof:
            evidence.append(proof)
    # Detailed source status takes precedence over a legacy scalar.
    for key in supplied:
        values[key] = []
    for key, observations in grouped.items():
        unique = {v for codes in observations for v in codes}
        if key not in invalid and len(unique) == 1 and all(codes for codes in observations):
            values[key].extend(unique)
    normalized = {key: canonical_values(key, val) for key, val in values.items()}
    source = product.metadata_source
    if source and len(source) > 500:
        source = "metadata_source (full value in product metadata)"
    # The full provenance remains in Product; the compact feature snapshot is bounded.
    return InteriorProfile(**normalized, evidence=sorted(set(evidence + ([source] if source else [])))[:12])


def compatibility(room, product, *, actual=None, feature_weights=None, palette_family_affinity=.7):
    """Weights depend on the request only; missing candidate data earns no bonus."""
    actual = actual if actual is not None else product_profile(product)
    feature_weights = FEATURE_WEIGHTS if feature_weights is None else feature_weights
    requested = {key: canonical_values(key, getattr(room, key)) for key in FEATURE_WEIGHTS}
    denominator = sum(feature_weights[k] for k, values in requested.items() if values)
    details, reasons, score = {}, [], 0.0
    for key, desired in requested.items():
        if not desired or not denominator or not feature_weights[key]:
            continue
        observed = getattr(actual, key)
        affinity = 0.0
        for left in desired:
            for right in observed:
                value = 1.0 if left == right else palette_family_affinity if key == "palette" and any({left, right} <= family for family in COLOR_FAMILIES) else 0.0
                affinity = max(affinity, value)
        weight = feature_weights[key] / denominator
        score += weight * affinity
        details[key] = {"requested": desired, "product": observed, "state": "UNKNOWN" if not observed else "SIMILAR" if affinity else "DIFFERENT",
                        "affinity": affinity if observed else None, "weight": weight}
        if affinity:
            label = {"palette": "색조", "materials": "재질", "shapes": "형태", "styles": "스타일"}[key]
            reasons.append(f"방에서 관찰한 {label}({', '.join(desired)})와 상품 특징({', '.join(observed)})의 유사성에 가점")
    return {"version": INTERIOR_VERSION, "applicable": bool(denominator), "score": score,
            "checks": details, "reasons": reasons,
            "room_features": room.model_dump(), "product_features": actual.model_dump()}


def query_terms(profile):
    terms = [f"{key}: {', '.join(canonical_values(key, getattr(profile, key)))}" for key in FEATURE_WEIGHTS
             if canonical_values(key, getattr(profile, key))]
    return " | ".join(terms)
