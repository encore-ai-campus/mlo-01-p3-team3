"""Deterministic processing of validated semantic outputs; no model clients."""
import copy
import heapq
import math

from .scale import assess_scale
from .schemas import Box

LOCAL_PROCESSING_VERSION = "semantic-results-v1"


def search_query(plan):
    """Compose a query from interpreted requirements, never raw drawing labels."""
    terms = [plan.get(key) for key in ("category_code", "color", "material", "shape")]
    terms.extend(plan.get("search_terms", []))
    unique = {}
    for term in terms:
        if term and (cleaned := " ".join(term.split())):
            unique.setdefault(cleaned.casefold(), cleaned)
    return " ".join(unique.values())


def map_region(box, mapping):
    """Map a validated original-room box into its aspect-preserving padded copy."""
    if box is None:
        return None
    original = Box.model_validate(box).model_dump()
    mapped = {field: (original[field] * mapping["original_size"][axis] * mapping["scale"][axis]
                      + mapping["offset"][axis]) / mapping["size"][axis]
              for field, axis in (("x_min", 0), ("x_max", 0), ("y_min", 1), ("y_max", 1))}
    return Box.model_validate(mapped).model_dump()


def rank_candidates(query, templates, vectors, reused, plan, limit=None):
    """Cosine ranking with a stable ID tie break; attributes are diagnostics only.

    Inputs are already validated and normalized by the embedding boundary. Unknown
    attributes must not be silently treated as mismatches or used as hard filters.
    """
    def candidates():
        for template in templates:
            vector = vectors[template["id"]]
            if len(vector) != len(query):
                raise ValueError("Candidate embedding dimension mismatch")
            score = math.fsum(a * b for a, b in zip(query, vector))
            if not math.isfinite(score):
                raise ValueError("Candidate similarity must be finite")
            differences = []
            for attribute in ("color", "material", "shape"):
                requested = plan.get(attribute)
                actual = template.get("attributes", {}).get(attribute)
                if requested and (not actual or actual.casefold() != requested.casefold()):
                    differences.append({"attribute": attribute, "requested": requested, "template": actual,
                                        "status": "UNKNOWN" if not actual else "DIFFERENT"})
            yield {"template_id": template["id"], "version": template["version"], "image": template["image"],
                   "category_code": template["category_code"], "score": score,
                   "differences": differences, "vector_reused": reused[template["id"]],
                   "attributes": template.get("attributes", {})}

    ordering = lambda candidate: (-candidate["score"], candidate["template_id"])
    return (heapq.nsmallest(limit, candidates(), key=ordering) if limit is not None
            else sorted(candidates(), key=ordering))


def assess_comparison(raw, scale_plan, bounds, category):
    """Apply numeric and eligibility rules without replacing visual judgments."""
    result = copy.deepcopy(raw)
    result["scale_check"] = assess_scale(result, scale_plan, bounds, category)
    gates = {"structure": result["reference_fidelity"]["status"] == "MATCH",
             "annotations": result["annotation_check"]["status"] == "CLEAR",
             "scale": result["scale_check"]["status"] == "APPROPRIATE"}
    for obj in result["objects"]:
        obj["eligible_for_selection"] = (obj["change_type"] in {"ADDED", "REPLACED"}
                                         and obj["category_code"] == category and all(gates.values()))
    status = ("NEEDS_ANNOTATION_REVIEW" if not gates["annotations"] else
              "NEEDS_STRUCTURE_REVIEW" if not gates["structure"] else
              "NEEDS_SCALE_REVIEW" if not gates["scale"] else
              "COMPLETED" if result["objects"] else "NO_OBJECTS")
    result["local_processing"] = {"version": LOCAL_PROCESSING_VERSION,
                                  "visual_scale_status": raw["scale_check"]["status"],
                                  "gates": gates, "status": status}
    return result
