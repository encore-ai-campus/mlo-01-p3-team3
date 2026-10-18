"""Label-free CPU recommendation. Embeddings are supplied, never generated here."""
import heapq
import math
import re
from pathlib import Path
from typing import Literal

import numpy as np
import sklearn
from pydantic import Field, model_validator
from sklearn.metrics.pairwise import cosine_similarity

from ..assets import checksum, read_json
from ..schemas import Box, Contract

RANKING_VERSION = "weighted-content-v2"
CONTEXT_VERSION = "direct-context-v2"


class RankingPolicy(Contract):
    version: Literal["weighted-content-v1", "weighted-content-v2"] = RANKING_VERSION
    similarity_weight: float = Field(default=.85, ge=0, le=1, allow_inf_nan=False)
    color_weight: float = Field(default=.15, ge=0, le=1, allow_inf_nan=False)

    @model_validator(mode="after")
    def total(self):
        if not math.isclose(self.similarity_weight + self.color_weight, 1, abs_tol=1e-8):
            raise ValueError("Ranking weights must sum to one")
        return self


class Condition(Contract):
    field: Literal["color", "material", "shape", "price_krw", "width_mm", "depth_mm", "height_mm"]
    operator: Literal["EQ", "NE", "LE"]
    value: str | float
    source_quote: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def compatible(self):
        numeric = self.field.endswith(("_krw", "_mm"))
        if numeric:
            if self.operator != "LE" or not isinstance(self.value, (int, float)) or not math.isfinite(self.value) or self.value <= 0:
                raise ValueError("Numeric conditions require a positive finite upper bound")
        elif self.operator == "LE" or not isinstance(self.value, str) or not self.value.strip():
            raise ValueError("Attribute conditions require EQ/NE and a nonempty string")
        return self


class SizeEvidence(Contract):
    basis: Literal["TYPICAL_OBJECT_SIZE", "RELATIVE_ONLY", "NONE"] = "NONE"
    explanation: str = "실측 근거 없음"
    # Visual estimates are retained as context; this MVP never hard-filters by them.
    estimated_width_mm: float | None = Field(default=None, gt=0, allow_inf_nan=False)


class RecommendationContext(Contract):
    decision: Literal["ACCEPTED", "AMBIGUOUS", "UNSUPPORTED"]
    category_code: str | None
    search_terms: list[str] = Field(default_factory=list, max_length=20)
    required: list[Condition] = Field(default_factory=list, max_length=20)
    preferred_color: str | None = None
    room_context: str = Field(default="", max_length=4000)
    placement_intent: str = Field(default="", max_length=2000)
    placement_region: Box | None = None
    size_estimate: SizeEvidence = Field(default_factory=SizeEvidence)
    unapplied_conditions: list[str] = Field(default_factory=list, max_length=20)
    reason: str = ""

    @model_validator(mode="after")
    def accepted(self):
        if self.decision == "ACCEPTED" and not self.category_code:
            raise ValueError("Accepted context requires a category")
        if self.decision != "ACCEPTED" and self.category_code is not None:
            raise ValueError("Unresolved category must be null")
        return self


def validate_context(payload, text, categories):
    context = RecommendationContext.model_validate(payload)
    if context.category_code and context.category_code not in categories:
        raise ValueError("Unsupported category")
    for condition in context.required:
        if condition.source_quote not in text:
            raise ValueError("Required condition must quote original user text")
        if condition.field.endswith(("_krw", "_mm")):
            units = {"mm": 1, "cm": 10, "m": 1000, "밀리미터": 1, "센티미터": 10,
                     "미터": 1000} if condition.field.endswith("_mm") else {"원": 1, "만원": 10000, "만 원": 10000}
            pattern = r"([0-9][0-9,]*(?:\.[0-9]+)?)\s*(" + "|".join(sorted(units, key=len, reverse=True)) + r")"
            amounts = [float(n.replace(",", "")) * units[u] for n, u in re.findall(pattern, condition.source_quote)]
            if not any(math.isclose(condition.value, amount) for amount in amounts):
                raise ValueError("Required number/unit is not supported by the quote")
    return context


def parse_context(payload, text, categories):
    """Convert only explicitly declared provider coordinates; retain the raw receipt."""
    import json
    data = json.loads(payload["text"])
    space = payload.get("coordinate_space", "normalized_1_xyxy")
    if space not in {"normalized_1_xyxy", "normalized_1000_xyxy"}:
        raise ValueError("Unsupported provider coordinate space")
    if space == "normalized_1000_xyxy" and data.get("placement_region") is not None:
        for field in ("x_min", "y_min", "x_max", "y_max"):
            value = data["placement_region"][field]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                raise ValueError("Provider coordinates must be finite numbers")
            data["placement_region"][field] = value / 1000
    return validate_context(data, text, categories)


def context_prompt(request):
    import json
    return ("Interpret ONE requested furniture item from room photo then separate sketch (when present). "
            "Return the provided schema. Use only allowed categories. Never generate an image. "
            "Separate explicit requirements from observed room style. Black sketch lines are not a color request. "
            "required contains ONLY explicit mandatory conditions from user TEXT, with exact source_quote. "
            "Here user TEXT means only INPUT DATA.text. Never copy image/sketch OCR into source_quote. "
            "If INPUT DATA.text is empty, required MUST be an empty list. "
            "Use readable sketch labels as search_terms or preferences, not quoted TEXT-only hard conditions. "
            "Use EQ/NE for canonical color/material/shape, LE for upper price_krw or width/depth/height_mm. "
            "Convert explicit units to KRW/mm. Do not infer missing conditions. Treat preferences as search_terms "
            "or preferred_color. Retain negated/unsupported constraints in search_terms and unapplied_conditions. "
            "room_context describes the room, not mandatory product properties. A placement rectangle is not a product shape. "
            "No verified measurement references are supplied: size basis may only be TYPICAL_OBJECT_SIZE, "
            "RELATIVE_ONLY or NONE. Any estimated width is uncertain, not a hard condition. Never invent "
            "product price, measured specifications or URLs. If category is ambiguous, use AMBIGUOUS and null. INPUT DATA:\n" +
            json.dumps(request, ensure_ascii=False))


class DimensionRange(Contract):
    minimum: float = Field(gt=0, allow_inf_nan=False)
    maximum: float = Field(gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def ordered(self):
        if self.minimum > self.maximum:
            raise ValueError("Dimension range must be ordered")
        return self


class Product(Contract):
    image_id: str = Field(min_length=1, max_length=180)
    product_id: str = Field(min_length=1, max_length=180)
    name: str
    category_code: str | None = None
    color: str | None = None
    material: str | None = None
    shape: str | None = None
    price_krw: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    width_mm: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    depth_mm: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    height_mm: float | None = Field(default=None, gt=0, allow_inf_nan=False)
    dimension_ranges_mm: dict[Literal["width_mm", "depth_mm", "height_mm"], DimensionRange] = Field(default_factory=dict)
    component_dimensions_mm: dict[str, DimensionRange] = Field(default_factory=dict)
    attribute_options: dict[Literal["color", "material", "shape"], list[str]] = Field(default_factory=dict)
    component_attributes: dict[str, list[str]] = Field(default_factory=dict)
    raw_attributes: dict[str, list[str]] = Field(default_factory=dict)
    metadata_issues: list[str] = Field(default_factory=list)
    metadata_source: str | None = None
    observed_at: str | None = None
    comparison_url: str | None = None
    image: str
    image_checksum: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def sourced(self):
        from .product_links import comparison_url
        if self.comparison_url is not None and (not self.metadata_source or comparison_url(self.comparison_url, self.product_id) is None):
            raise ValueError("Invalid or unsourced product comparison URL")
        has_attributes = any(getattr(self, key) is not None for key in ("category_code", "color", "material", "shape", "price_krw", "width_mm", "depth_mm", "height_mm"))
        if (has_attributes or self.dimension_ranges_mm or self.component_dimensions_mm or self.attribute_options
                or self.component_attributes or self.raw_attributes or self.metadata_issues) and not self.metadata_source:
            raise ValueError("Product metadata needs a source")
        for field, interval in self.dimension_ranges_mm.items():
            scalar = getattr(self, field)
            if scalar is not None and (scalar != interval.minimum or scalar != interval.maximum):
                raise ValueError("Scalar dimension conflicts with its recorded range")
        return self


class ProductIndex:
    """Immutable startup snapshot. Large catalogs can use an mmap float32 .npy matrix."""
    def __init__(self, products, vectors, *, model, preprocessing, version):
        self.products = tuple(Product.model_validate(p) for p in products)
        if len({p.image_id for p in self.products}) != len(self.products):
            raise ValueError("Duplicate image_id")
        self.by_image = {p.image_id: p for p in self.products}
        # Multiple images of one product must refer to the same metadata.
        metadata = {}
        for p in self.products:
            values = p.model_dump(exclude={"image_id", "image", "image_checksum"})
            if p.product_id in metadata and metadata[p.product_id] != values:
                raise ValueError("Conflicting metadata for one product")
            metadata[p.product_id] = values
        self.vectors = np.asarray(vectors)
        if self.vectors.ndim != 2 or self.vectors.shape[0] != len(self.products) or not self.vectors.shape[1]:
            raise ValueError("Expected one vector per image")
        if self.vectors.dtype != np.float32:
            self.vectors = self.vectors.astype(np.float32)
        for start in range(0, len(self.products), 2048):
            block = self.vectors[start:start + 2048]
            if not np.isfinite(block).all() or np.any(np.linalg.norm(block, axis=1) == 0):
                raise ValueError("Vectors must be finite and nonzero")
        self.vectors.flags.writeable = False
        self.model, self.preprocessing, self.version = model, preprocessing, version
        self.dimension = self.vectors.shape[1]

    @classmethod
    def load(cls, path):
        path = Path(path).resolve()
        data = read_json(path)
        def inside(name):
            target = (path.parent / name).resolve()
            if not target.is_relative_to(path.parent):
                raise ValueError("Catalog path escapes snapshot")
            return target
        products = []
        for row in data["products"]:
            product = Product.model_validate(row)
            image = inside(product.image)
            if checksum(image.read_bytes()) != product.image_checksum:
                raise ValueError("Catalog image checksum mismatch")
            products.append(product.model_copy(update={"image": str(image)}))
        if "vectors_file" in data:
            vector_path = inside(data["vectors_file"])
            # Stream checksum to avoid reading another full matrix into RAM.
            import hashlib
            with vector_path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            if digest != data["vectors_checksum"]:
                raise ValueError("Catalog vector checksum mismatch")
            vectors = np.load(vector_path, mmap_mode="r", allow_pickle=False)
            if vectors.dtype != np.float32:
                raise ValueError("mmap catalog requires float32")
            vector_version = digest
        else:
            vectors = np.asarray(data["vectors"], dtype=np.float32)
            vector_version = "inline"
        result = cls(products, vectors, model=data["embedding_model"], preprocessing=data["preprocessing_version"],
                     version=checksum(path.read_bytes() + vector_version.encode()))
        if result.dimension != data["dimension"]:
            raise ValueError("Catalog dimension mismatch")
        return result

    def validate_settings(self, settings):
        if (self.model, self.dimension, self.preprocessing) != (settings.embedding_model, settings.dimension, settings.preprocessing_version):
            raise ValueError("Catalog embedding model/dimension/preprocessing mismatch")

    def rank(self, query, context, policy=None, limit=6):
        policy = policy or RankingPolicy()
        if not 1 <= limit <= 50:
            raise ValueError("Recommendation limit must be 1..50")
        query = np.asarray(query, dtype=np.float32).reshape(1, -1)
        if query.shape[1] != self.dimension or not np.isfinite(query).all() or np.linalg.norm(query) == 0:
            raise ValueError("Invalid query vector")
        if context.decision != "ACCEPTED":
            raise ValueError("Resolve category before ranking")
        best = {"MATCH": {}, "UNKNOWN": {}}
        excluded = 0
        for start in range(0, len(self.products), 2048):
            eligible = []
            for offset, product in enumerate(self.products[start:start + 2048]):
                preferences = {}
                checks = {"category_code": "UNKNOWN" if product.category_code is None else
                          "MATCH" if product.category_code == context.category_code else "DIFFERENT"}
                if context.unapplied_conditions:
                    checks["unapplied_conditions"] = "UNKNOWN"
                for n, condition in enumerate(context.required):
                    actual = getattr(product, condition.field)
                    interval = product.dimension_ranges_mm.get(condition.field)
                    if actual is None and interval is not None and condition.operator == "LE":
                        state = ("MATCH" if interval.maximum <= condition.value else
                                 "DIFFERENT" if interval.minimum > condition.value else "UNKNOWN")
                    elif actual is None:
                        state = "UNKNOWN"
                    else:
                        left = actual.strip().casefold() if isinstance(actual, str) else actual
                        right = condition.value.strip().casefold() if isinstance(condition.value, str) else condition.value
                        match = left == right if condition.operator == "EQ" else left != right if condition.operator == "NE" else left <= right
                        state = "MATCH" if match else "DIFFERENT"
                    # Sparse material metadata is search context in v2, never an eligibility gate.
                    target = preferences if condition.field == "material" and policy.version == "weighted-content-v2" else checks
                    target[f"required:{n}:{condition.field}"] = state
                if "DIFFERENT" in checks.values():
                    excluded += 1
                    continue
                eligible.append((offset, product, checks, preferences))
            if not eligible:
                continue
            # Filter before similarity. Only [1, eligible chunk], never NxN.
            rows = [row[0] for row in eligible]
            scores = cosine_similarity(query, self.vectors[start:start + 2048][rows])[0]
            for (_, product, checks, preferences), similarity in zip(eligible, scores):
                group = "UNKNOWN" if "UNKNOWN" in checks.values() else "MATCH"
                color_state = "UNSPECIFIED" if not context.preferred_color else "UNKNOWN" if product.color is None else (
                    "MATCH" if product.color.casefold().strip() == context.preferred_color.casefold().strip() else "DIFFERENT")
                # No requested preference: similarity alone. Missing product data never renormalizes the weights.
                sim_weight = policy.similarity_weight if context.preferred_color else 1.0
                color_weight = policy.color_weight if context.preferred_color else 0.0
                components = {"similarity": sim_weight * float(similarity), "preferred_color": color_weight * (color_state == "MATCH")}
                item = {"product_id": product.product_id, "image_id": product.image_id, "name": product.name,
                        "score": sum(components.values()), "similarity": float(similarity), "score_components": components,
                        "condition_status": group, "checks": checks, "preferred_color_status": color_state,
                        "size_fit": "UNKNOWN", "metadata": product.model_dump(exclude={"image", "image_checksum"})}
                if policy.version == "weighted-content-v2":
                    item["preference_checks"] = preferences
                previous = best[group].get(product.product_id)
                key = lambda x: (-x["score"], -x["similarity"], x["product_id"], x["image_id"])
                if previous is None or key(item) < key(previous):
                    best[group][product.product_id] = item
        order = lambda x: (-x["score"], -x["similarity"], x["product_id"], x["image_id"])
        return {"ranking_version": policy.version, "policy": policy.model_dump(), "weight_source": "MANUAL_INITIAL",
                "sklearn_version": sklearn.__version__, "index_version": self.version, "score_is_probability": False,
                "searched_images": len(self.products), "excluded_images": excluded,
                "items": heapq.nsmallest(limit, best["MATCH"].values(), key=order),
                "unverified_items": heapq.nsmallest(limit, best["UNKNOWN"].values(), key=order),
                "unapplied_conditions": context.unapplied_conditions}
