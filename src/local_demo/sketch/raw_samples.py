"""RoomPick raw contract: drawing history is separate from the editor document."""
import copy
import json
import os
import re
from pathlib import Path
from uuid import UUID, uuid4

from django.utils.dateparse import parse_datetime

from . import validation

MAX_RAW_BYTES = 4 * 1024 * 1024
SAMPLE_KEYS = {"schema_version", "sample_id", "project_id", "writer_id", "created_at", "label_id", "label_status",
               "quality_status", "coordinate_system", "time_origin", "canvas_width", "canvas_height",
               "device_pixel_ratio", "project_width", "project_height", "strokes", "operations", "initial_document",
               "final_document", "final_active_stroke_ids", "duration_ms", "undo_count", "submission_reason", "result_id"}


def strict_constant(value):
    raise ValueError("NaN·Infinity는 허용되지 않습니다.")


def identifier(value):
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("정규 UUID 문자열이 필요합니다.")
    return value


def timestamp(value):
    if not isinstance(value, str) or len(value) > 40:
        raise ValueError("시각 형식이 올바르지 않습니다.")
    parsed = parse_datetime(value)
    if parsed is None or parsed.utcoffset() is None:
        raise ValueError("시간대가 있는 ISO 8601 시각이 필요합니다.")


def identifiers(value):
    if not isinstance(value, list) or len(value) > 1100:
        raise ValueError("요소 ID 목록이 올바르지 않습니다.")
    values = [identifier(item) for item in value]
    if len(set(values)) != len(values):
        raise ValueError("요소 ID가 중복됩니다.")
    return values


def apply_patch(document, patch):
    if not isinstance(patch, dict) or set(patch) - {"strokes", "items"}:
        raise ValueError("편집 변경분 형식이 올바르지 않습니다.")
    targets = {"strokes": [], "items": []}
    for kind, changes in patch.items():
        if not isinstance(changes, dict) or set(changes) != {"upsert", "remove", "order"}:
            raise ValueError("편집 변경분의 추가·삭제·순서가 필요합니다.")
        removed, order = identifiers(changes["remove"]), identifiers(changes["order"])
        upserts = changes["upsert"]
        if not isinstance(upserts, list) or len(upserts) > 1000 or any(not isinstance(row, dict) for row in upserts):
            raise ValueError("편집 요소 목록이 올바르지 않습니다.")
        upsert_ids = identifiers([row.get("id") for row in upserts])
        if set(removed) & set(upsert_ids):
            raise ValueError("추가와 삭제에 같은 요소가 있습니다.")
        rows = {row["id"]: row for row in document[kind]}
        for item_id in removed:
            if item_id not in rows:
                raise ValueError("존재하지 않는 요소를 삭제했습니다.")
            del rows[item_id]
        rows.update(zip(upsert_ids, copy.deepcopy(upserts)))
        if set(order) != set(rows):
            raise ValueError("요소 순서와 편집 결과가 다릅니다.")
        document[kind] = [rows[item_id] for item_id in order]
        targets[kind] = upsert_ids + removed
    validation.document(document)
    return targets


def validate_sample(sample, project):
    if not isinstance(sample, dict) or set(sample) - {"capture_status"} != SAMPLE_KEYS:
        raise ValueError("원본 샘플 필드가 올바르지 않습니다.")
    identifier(sample["sample_id"])
    if sample["project_id"] != str(project.id) or sample["writer_id"] != project.owner:
        raise ValueError("작업 또는 방문 ID가 다릅니다.")
    if (sample["schema_version"] != "roompick-sketch-1.0.0" or sample["label_id"] is not None
            or sample["label_status"] != "unverified" or sample["quality_status"] != "pending"):
        raise ValueError("자유 스케치는 미검수 라벨·pending 상태로만 수집합니다.")
    if sample["coordinate_system"] != "normalized_xy_top_left_down" or sample["time_origin"] != "first_pointerdown_ms":
        raise ValueError("좌표 또는 시간 기준이 올바르지 않습니다.")
    timestamp(sample["created_at"])
    for key in ("canvas_width", "canvas_height"):
        validation.number(sample[key], 1, 100000)
    validation.number(sample["device_pixel_ratio"], .1, 20)
    if sample["project_width"] != project.width or sample["project_height"] != project.height:
        raise ValueError("원본 사진 크기가 작업과 다릅니다.")
    validation.number(sample["duration_ms"], 0, 7 * 24 * 60 * 60 * 1000)
    if sample["submission_reason"] not in {"search", "project_changed", "page_exit", "recovered", "capture_limit"}:
        raise ValueError("제출 사유가 올바르지 않습니다.")
    if sample["result_id"] is not None:
        identifier(sample["result_id"])
    strokes, operations = sample["strokes"], sample["operations"]
    if not isinstance(strokes, list) or len(strokes) > 1000 or not isinstance(operations, list) or len(operations) > 1000:
        raise ValueError("원본은 최대 1,000획·1,000개 작업까지 제출할 수 있습니다.")
    warnings = set()
    if sample.get("capture_status", "complete") not in {"complete", "limited"}:
        raise ValueError("수집 완료 상태가 올바르지 않습니다.")
    if sample.get("capture_status") == "limited":
        warnings.add("capture_limit")
    total_points, last_stroke_t = 0, 0
    stroke_ids = []
    for order, stroke in enumerate(strokes):
        keys = {"stroke_id", "stroke_order", "tool", "ink_hex", "stroke_width_css_px", "canvas_width", "canvas_height",
                "device_pixel_ratio", "pointer_type", "end_reason", "points"}
        if not isinstance(stroke, dict) or set(stroke) != keys:
            raise ValueError("원본 획 형식이 올바르지 않습니다.")
        stroke_ids.append(identifier(stroke["stroke_id"]))
        if type(stroke["stroke_order"]) is not int or stroke["stroke_order"] != order:
            raise ValueError("원본 획 순서가 올바르지 않습니다.")
        if stroke["tool"] not in {"pen", "eraser"} or stroke["pointer_type"] not in {"mouse", "touch", "pen", "unknown"}:
            raise ValueError("원본 입력 도구가 올바르지 않습니다.")
        if stroke["end_reason"] not in {"up", "cancel", "lost_capture", "interrupted", "limited"}:
            raise ValueError("완료 또는 취소된 획만 제출할 수 있습니다.")
        if not isinstance(stroke["ink_hex"], str) or not re.fullmatch(r"#[0-9a-fA-F]{6}", stroke["ink_hex"]):
            raise ValueError("원본 색상이 올바르지 않습니다.")
        validation.number(stroke["stroke_width_css_px"], .001, 10000)
        for key in ("canvas_width", "canvas_height"):
            validation.number(stroke[key], 1, 100000)
            if stroke[key] != sample[key]:
                warnings.add("canvas_resized")
        validation.number(stroke["device_pixel_ratio"], .1, 20)
        points = stroke["points"]
        if not isinstance(points, list) or not 2 <= len(points) <= 100000:
            raise ValueError("원본 획에 시작·종료 점이 필요합니다.")
        total_points += len(points)
        if total_points > 100000:
            raise ValueError("원본 좌표는 총 100,000점까지 제출할 수 있습니다.")
        last_t = last_stroke_t
        for index, point in enumerate(points):
            required = {"x", "y", "t", "event_type", "source", "in_bounds"}
            if not isinstance(point, dict) or not required <= set(point) or set(point) - required - {"pressure", "tilt_x", "tilt_y", "canvas_width", "canvas_height"}:
                raise ValueError("원본 점 형식이 올바르지 않습니다.")
            for key in ("x", "y"):
                validation.number(point[key], -1000000, 1000000)
            validation.number(point["t"], last_t, sample["duration_ms"])
            last_t = point["t"]
            expected = "down" if index == 0 else ("up" if stroke["end_reason"] == "up" else "cancel") if index == len(points) - 1 else "move"
            if point["event_type"] != expected or point["source"] not in {"native", "coalesced", "synthetic"}:
                raise ValueError("원본 포인터 이벤트 순서가 올바르지 않습니다.")
            inside = 0 <= point["x"] <= 1 and 0 <= point["y"] <= 1
            if type(point["in_bounds"]) is not bool or point["in_bounds"] != inside:
                raise ValueError("좌표와 in_bounds가 다릅니다.")
            if not inside:
                warnings.add("outside_canvas")
            if point["source"] == "synthetic":
                warnings.add("synthetic_point")
            for key, low, high in (("pressure", 0, 1), ("tilt_x", -90, 90), ("tilt_y", -90, 90), ("canvas_width", 1, 100000), ("canvas_height", 1, 100000)):
                if key in point:
                    validation.number(point[key], low, high)
                    if key.startswith("canvas_"):
                        warnings.add("canvas_resized")
        last_stroke_t = last_t
        if stroke["end_reason"] != "up":
            warnings.add("cancelled_stroke")
    if len(set(stroke_ids)) != len(stroke_ids):
        raise ValueError("원본 획 ID가 중복됩니다.")
    document = copy.deepcopy(validation.document(sample["initial_document"]))
    # Stable IDs are required for replay; legacy drawing geometry remains unchanged.
    identifiers([row.get("id") for row in document["strokes"]])
    op_ids, last_t = [], 0
    for operation in operations:
        required = {"op_id", "type", "t", "client_time", "target_stroke_ids", "target_item_ids"}
        if not isinstance(operation, dict) or not required <= set(operation) or set(operation) - required - {"document_patch", "event_id", "source_stroke_id"}:
            raise ValueError("원본 편집 이력 형식이 올바르지 않습니다.")
        op_ids.append(identifier(operation["op_id"]))
        if "event_id" in operation:
            identifier(operation["event_id"])
        if "source_stroke_id" in operation and identifier(operation["source_stroke_id"]) not in stroke_ids:
            raise ValueError("편집 이력의 원본 획이 없습니다.")
        if operation["type"] not in validation.EVENT_TYPES:
            raise ValueError("지원하지 않는 편집 이력입니다.")
        timestamp(operation["client_time"])
        validation.number(operation["t"], last_t, sample["duration_ms"])
        last_t = operation["t"]
        targets = apply_patch(document, operation.get("document_patch", {}))
        if (identifiers(operation["target_stroke_ids"]) != targets["strokes"]
                or identifiers(operation["target_item_ids"]) != targets["items"]):
            raise ValueError("편집 대상과 변경분이 다릅니다.")
    if len(set(op_ids)) != len(op_ids):
        raise ValueError("편집 이력 ID가 중복됩니다.")
    if document != validation.document(sample["final_document"]):
        raise ValueError("편집 이력 재생 결과와 최종 문서가 다릅니다.")
    final_ids = [row["id"] for row in document["strokes"] if row["tool"] == "pen"]
    if identifiers(sample["final_active_stroke_ids"]) != final_ids:
        raise ValueError("최종 유효 획 목록이 다릅니다.")
    if type(sample["undo_count"]) is not int or sample["undo_count"] != sum(op["type"] == "history.undo" for op in operations):
        raise ValueError("실행 취소 횟수가 다릅니다.")
    if not document["strokes"] and not document["items"]:
        warnings.add("empty_final_document")
    if not strokes and document["strokes"]:
        warnings.add("restored_geometry_without_point_time")
    return sorted(warnings)


def save_immutable(root, sample):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    target = root / (sample["sample_id"] + ".json")
    data = json.dumps(sample, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")
    temporary = root / (".tmp-" + str(uuid4()))
    try:
        with temporary.open("xb") as output:
            output.write(data)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, target)
            return False
        except FileExistsError:
            if target.read_bytes() != data:
                raise FileExistsError("같은 sample_id에 다른 내용이 있습니다.") from None
            return True
    finally:
        temporary.unlink(missing_ok=True)
