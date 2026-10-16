import math
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Box(Contract):
    x_min: float = Field(ge=0, le=1)
    y_min: float = Field(ge=0, le=1)
    x_max: float = Field(ge=0, le=1)
    y_max: float = Field(ge=0, le=1)

    @model_validator(mode="after")
    def ordered(self):
        if self.x_min >= self.x_max or self.y_min >= self.y_max:
            raise ValueError("Bounding box must have positive area")
        return self


class InputRequest(Contract):
    room_image: str | None = None
    sketch_image: str | None = None
    text: str = Field(default="", max_length=10000)
    requested_region: Box | None = None
    categories: list[str] = Field(default_factory=lambda: ["CHAIR", "SOFA", "TABLE"], min_length=1)
    template_id: str | None = None

    @model_validator(mode="after")
    def meaningful(self):
        if not self.room_image and not self.sketch_image and not self.text.strip():
            raise ValueError("Provide an image or text")
        if self.requested_region and not self.room_image:
            raise ValueError("requested_region requires a reference room image")
        if len(set(self.categories)) != len(self.categories) or any(not c.strip() for c in self.categories):
            raise ValueError("Categories must be unique, non-empty codes")
        return self


class Interpretation(Contract):
    decision: Literal["ACCEPTED", "NEEDS_CATEGORY_SELECTION", "UNSUPPORTED"]
    category_code: str | None
    candidates: list[str]
    color: str | None
    material: str | None
    shape: str | None
    reason: str

    @model_validator(mode="after")
    def accepted_category(self):
        if self.decision == "ACCEPTED" and not self.category_code:
            raise ValueError("ACCEPTED requires category_code")
        if self.decision != "ACCEPTED" and self.category_code is not None:
            raise ValueError("Unresolved category must be null")
        return self


class DetectedObject(Contract):
    category_code: str
    box: Box


class ObjectAnalysis(Contract):
    objects: list[DetectedObject] = Field(max_length=50)


class SceneUnderstanding(Interpretation):
    search_terms: list[str] = Field(default_factory=list, max_length=20)
    placement_intent: str
    keep_elements: list[str]
    sketch_role: Literal["PLACEMENT_ONLY", "FURNITURE_SHAPE", "NONE", "UNCERTAIN"] = "UNCERTAIN"
    shape_evidence: Literal["TEXT", "SKETCH_DETAIL", "NONE"] = "NONE"
    placement_region: Box | None = None
    explicit_size_intent: str | None = None

    @model_validator(mode="after")
    def evidenced_shape(self):
        if self.shape_evidence == "NONE" or (self.shape_evidence == "SKETCH_DETAIL" and self.sketch_role != "FURNITURE_SHAPE"):
            self.shape = None
        return self


class ScenePlan(SceneUnderstanding):
    # Persisted local output, also accepted when inspecting historical plan.json.
    search_text: str


class ComparedObject(DetectedObject):
    change_type: Literal["EXISTING", "ADDED", "REPLACED", "UNCERTAIN"]
    reason: str


class ReferenceStructure(Contract):
    description: str
    support_structure: str | None
    components: list[str] = Field(max_length=20)
    connections: list[str] = Field(max_length=20)
    preserve: list[str] = Field(max_length=20)
    uncertain_details: list[str] = Field(max_length=20)
    exact_leg_count: int | None = Field(ge=0, le=30)
    exact_caster_count: int | None = Field(ge=0, le=30)


class ReferenceFidelity(Contract):
    status: Literal["MATCH", "MISMATCH", "UNCERTAIN"]
    differences: list[str] = Field(max_length=20)
    reason: str


class AnnotationCheck(Contract):
    status: Literal["CLEAR", "DETECTED", "UNCERTAIN"]
    findings: list[str] = Field(max_length=20)
    reason: str

    @model_validator(mode="after")
    def consistent(self):
        if self.status == "CLEAR" and self.findings:
            raise ValueError("CLEAR annotation check cannot contain findings")
        return self


class SizeRange(Contract):
    minimum: float = Field(gt=0, allow_inf_nan=False)
    maximum: float = Field(gt=0, allow_inf_nan=False)

    @model_validator(mode="after")
    def ordered(self):
        if self.minimum > self.maximum:
            raise ValueError("Size range minimum exceeds maximum")
        return self


class ScaleAnchor(Contract):
    label: str
    box: Box
    dimension: Literal["WIDTH", "DEPTH", "HEIGHT"]
    typical_size_mm: SizeRange
    depth_relation: str
    uncertainty: str


class ScalePlan(Contract):
    basis: Literal["EXPLICIT", "CONTEXT", "CATEGORY_ONLY", "UNKNOWN"]
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    anchors: list[ScaleAnchor] = Field(max_length=10)
    furniture_size_prior: str
    placement_depth: str
    contact_point: str
    perspective_reason: str
    explicit_size_intent: str | None
    width_ratio: SizeRange | None
    height_ratio: SizeRange | None
    reason: str

    @model_validator(mode="after")
    def supported_bounds(self):
        for bounds in (self.width_ratio, self.height_ratio):
            if bounds and bounds.maximum > 1:
                raise ValueError("Projected size ratios must be within original room image")
        if self.basis == "CONTEXT" and not self.anchors:
            raise ValueError("Context scale requires original-room anchors")
        if self.basis == "EXPLICIT" and not self.explicit_size_intent:
            raise ValueError("Explicit scale requires a quoted size intent")
        if self.confidence != "LOW" and (self.basis in {"UNKNOWN", "CATEGORY_ONLY"}
                                         or not self.width_ratio or not self.height_ratio):
            raise ValueError("Reliable scale requires supported width and height bounds")
        return self


class ScaleCheck(Contract):
    status: Literal["APPROPRIATE", "TOO_SMALL", "TOO_LARGE", "OUT_OF_RANGE", "UNCERTAIN"]
    room_geometry: Literal["PRESERVED", "CHANGED", "UNCERTAIN"]
    findings: list[str] = Field(max_length=20)
    reason: str


class SceneComparison(ObjectAnalysis):
    objects: list[ComparedObject] = Field(max_length=50)
    reference_fidelity: ReferenceFidelity = Field(default_factory=lambda: ReferenceFidelity(
        status="UNCERTAIN", differences=[], reason="참조 구조 검증 결과가 없습니다."))
    annotation_check: AnnotationCheck = Field(default_factory=lambda: AnnotationCheck(
        status="UNCERTAIN", findings=[], reason="새로 생성된 주석 검사 결과가 없습니다."))
    scale_check: ScaleCheck = Field(default_factory=lambda: ScaleCheck(
        status="UNCERTAIN", room_geometry="UNCERTAIN", findings=[], reason="공간 대비 크기 검사 결과가 없습니다."))


class Template(Contract):
    id: str
    version: int = Field(ge=1)
    category_code: str
    image: str
    review_status: Literal["APPROVED", "PENDING", "REJECTED"]
    active: bool
    provenance: str = Field(min_length=1)
    attributes: dict[str, str] = Field(default_factory=dict)


def validate_categories(result: Interpretation | ObjectAnalysis, allowed: list[str]):
    values = ([obj.category_code for obj in result.objects] if isinstance(result, ObjectAnalysis)
              else result.candidates + ([result.category_code] if result.category_code else []))
    if any(value not in allowed for value in values):
        raise ValueError("Model returned an unsupported category")


def normalized_vector(values, dimension):
    if len(values) != dimension or any(isinstance(v, bool) or not isinstance(v, (int, float))
                                       or not math.isfinite(v) for v in values):
        raise ValueError("Invalid embedding dimension or non-finite values")
    norm = math.sqrt(sum(v * v for v in values))
    if not math.isfinite(norm) or norm <= 0:
        raise ValueError("Embedding norm must be finite and positive")
    return [v / norm for v in values]
