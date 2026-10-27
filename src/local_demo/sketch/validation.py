import math
import re
from uuid import UUID

from .catalog import CATEGORIES, PRODUCT_IDS
from .stickers import STICKER_IDS

EVENT_TYPES = {"stroke.completed", "stroke.cancelled", "history.undo", "history.redo", "sketch.cleared", "item.added", "item.moved", "item.resized", "item.removed", "category.selected", "button.clicked", "tool.changed", "photo.selected", "photo.dropped", "export.downloaded", "annotation.added", "annotation.moved", "annotation.resized", "annotation.removed", "annotation.edited", "search.clicked", "view.changed", "result.moved", "result.resized", "result.cancelled", "redraw.clicked", "fill.applied", "selection.copied", "selection.pasted", "crop.selected"}
EVENT_TYPES |= {"selection.grouped", "selection.ungrouped", "photo.restored", "selection.boxed"}
EVENT_TYPES |= {"requirements.changed", "style.changed"}


def metadata(value):
    for key in ("id", "group_id"):
        if key in value:
            try:
                UUID(value[key])
            except (ValueError, TypeError, AttributeError):
                raise ValueError("요소·그룹 ID가 올바르지 않습니다.") from None
    cuts = value.get("cuts", [])
    if not isinstance(cuts, list) or len(cuts) > 100:
        raise ValueError("부분 지우기는 요소당 100회까지 저장할 수 있습니다.")
    for cut in cuts:
        if not isinstance(cut, dict) or set(cut) != {"tool", "color", "width", "pointer_type", "points"} or cut.get("tool") != "eraser":
            raise ValueError("부분 지우기 데이터가 올바르지 않습니다.")
        document({"strokes": [cut], "items": []})
    return sum(len(cut["points"]) for cut in cuts)


def number(value, minimum=0, maximum=1):
    if type(value) not in (int, float) or not math.isfinite(value) or not minimum <= value <= maximum:
        raise ValueError("좌표·두께·크기 값이 허용 범위를 벗어났습니다.")


def title(value):
    if not isinstance(value, str) or not 1 <= len(value.strip()) <= 100:
        raise ValueError("제목은 1~100자로 입력해 주세요.")
    return value.strip()


def document(value):
    if not isinstance(value, dict) or set(value) != {"strokes", "items"}:
        raise ValueError("스케치 데이터 형식이 올바르지 않습니다.")
    strokes, items = value["strokes"], value["items"]
    if not isinstance(strokes, list) or len(strokes) > 1000 or not isinstance(items, list) or len(items) > 100:
        raise ValueError("스케치는 1,000획, 가구는 100개까지 저장할 수 있습니다.")
    total_points = 0
    ids = set()
    for stroke in strokes:
        required = {"tool", "color", "width", "pointer_type", "points"}
        if not isinstance(stroke, dict) or not required <= set(stroke) or set(stroke) - required - {"category", "id", "group_id", "cuts"}:
            raise ValueError("획 데이터 형식이 올바르지 않습니다.")
        total_points += metadata(stroke)
        if "id" in stroke:
            identifier = str(UUID(stroke["id"]))
            if identifier in ids:
                raise ValueError("삽입 요소 ID가 중복됩니다.")
            ids.add(identifier)
        if not all(isinstance(stroke[key], str) for key in ("tool", "pointer_type")) or stroke["tool"] not in ("pen", "eraser") or stroke["pointer_type"] not in ("mouse", "touch", "pen"):
            raise ValueError("지원하지 않는 도구·카테고리·입력 방식입니다.")
        if "category" in stroke and (not isinstance(stroke["category"], str) or stroke["category"] not in CATEGORIES):
            raise ValueError("이전 획의 카테고리 값이 올바르지 않습니다.")
        if not isinstance(stroke["color"], str) or not re.fullmatch(r"#[0-9a-fA-F]{6}", stroke["color"]):
            raise ValueError("색상은 #RRGGBB 형식이어야 합니다.")
        number(stroke["width"], 0.001, 0.08)
        points = stroke["points"]
        if not isinstance(points, list) or not 1 <= len(points) <= 10000:
            raise ValueError("획마다 1~10,000개의 점이 필요합니다.")
        total_points += len(points)
        if total_points > 100000:
            raise ValueError("전체 점은 100,000개까지 저장할 수 있습니다.")
        for point in points:
            if not isinstance(point, dict) or set(point) != {"x", "y", "pressure"}:
                raise ValueError("점 데이터 형식이 올바르지 않습니다.")
            for key in ("x", "y", "pressure"):
                number(point[key])
    total_runs = 0
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("가구 데이터 형식이 올바르지 않습니다.")
        try:
            identifier = str(UUID(item.get("id")))
        except (ValueError, TypeError, AttributeError):
            raise ValueError("가구 ID가 올바르지 않습니다.") from None
        if identifier in ids:
            raise ValueError("삽입 요소 ID가 중복됩니다.")
        ids.add(identifier)
        total_points += metadata(item)
        if total_points > 100000:
            raise ValueError("전체 점과 지우기 점은 100,000개까지 저장할 수 있습니다.")
        base = {"id", "x", "y", "width", "height"}
        kind = item.get("kind")
        if kind is None:
            if set(item) != base | {"product_id"} or not isinstance(item["product_id"], str) or item["product_id"] not in PRODUCT_IDS:
                raise ValueError("이전 가구 데이터가 올바르지 않습니다.")
        elif kind == 'sticker':
            if (set(item) - {'group_id', 'cuts'} != base | {'kind', 'sticker_id'}
                    or not isinstance(item.get('sticker_id'), str) or item['sticker_id'] not in STICKER_IDS):
                raise ValueError('등록된 사진 스티커만 사용할 수 있습니다.')
        else:
            if not isinstance(kind, str) or kind not in {"text", "rectangle", "ellipse", "line", "arrow", "fill"}:
                raise ValueError("지원하지 않는 삽입 요소입니다.")
            extra = {"text", "font_size"} if kind == "text" else {"parent_id", "mask_width", "mask_height", "runs"} if kind == "fill" else {"stroke_width", "flip_x", "flip_y"}
            if set(item) - {"group_id", "cuts"} != base | {"kind", "color"} | extra or not isinstance(item["color"], str) or not re.fullmatch(r"#[0-9a-fA-F]{6}", item["color"]):
                raise ValueError("삽입 요소 형식이 올바르지 않습니다.")
            if kind == "text":
                if not isinstance(item["text"], str) or not 1 <= len(item["text"].strip()) <= 200:
                    raise ValueError("텍스트는 1~200자로 입력해 주세요.")
                number(item["font_size"], .005, .15)
            elif kind == "fill":
                for key in ("mask_width", "mask_height"):
                    if type(item[key]) is not int or not 1 <= item[key] <= 1200:
                        raise ValueError("채우기 마스크 크기가 올바르지 않습니다.")
                if item["parent_id"] is not None:
                    try:
                        UUID(item["parent_id"])
                    except (ValueError, TypeError, AttributeError):
                        raise ValueError("채우기 연결 ID가 올바르지 않습니다.") from None
                runs = item["runs"]
                if not isinstance(runs, list) or not 1 <= len(runs) <= 100000:
                    raise ValueError("채우기 영역이 너무 크거나 형식이 잘못됐습니다.")
                total_runs += len(runs)
                if total_runs > 200000:
                    raise ValueError("전체 채우기 영역 저장 한도를 초과했습니다.")
                for run in runs:
                    if not isinstance(run, list) or len(run) != 3 or not all(type(part) is int for part in run):
                        raise ValueError("채우기 픽셀 구간이 올바르지 않습니다.")
                    y, start, end = run
                    if not 0 <= y < item["mask_height"] or not 0 <= start <= end < item["mask_width"]:
                        raise ValueError("채우기 픽셀이 영역 밖에 있습니다.")
            else:
                number(item["stroke_width"], .001, .08)
                if type(item["flip_x"]) is not bool or type(item["flip_y"]) is not bool:
                    raise ValueError("도형 방향이 올바르지 않습니다.")
        for key in ("x", "y"):
            number(item[key])
        for key in ("width", "height"):
            number(item[key], .000001 if kind == "fill" else .001 if kind in ("line", "arrow") else .01)
        if item["x"] + item["width"] > 1.000001 or item["y"] + item["height"] > 1.000001:
            raise ValueError("가구는 사진 영역 안에 배치해 주세요.")
    return value


def events(value):
    if not isinstance(value, list) or len(value) > 500:
        raise ValueError("한 번에 최대 500개 이벤트를 저장할 수 있습니다.")
    validated = []
    for event in value:
        if not isinstance(event, dict):
            raise ValueError("이벤트 형식이 올바르지 않습니다.")
        try:
            identifier = UUID(event.get("event_id"))
        except (ValueError, TypeError, AttributeError):
            raise ValueError("이벤트 ID가 올바르지 않습니다.") from None
        event_type, payload = event.get("event_type"), event.get("payload")
        if not isinstance(event_type, str) or event_type not in EVENT_TYPES or not isinstance(payload, dict) or len(payload) > 10:
            raise ValueError("지원하지 않는 행동 이벤트입니다.")
        for key, value in payload.items():
            if key == "stroke" and event_type == "stroke.completed":
                document({"strokes": [value], "items": []})
                continue
            if key == "item" and event_type in {"item.added", "item.moved", "item.resized", "item.removed", "annotation.added", "annotation.moved", "annotation.resized", "annotation.removed", "annotation.edited", "fill.applied", "selection.pasted"}:
                document({"strokes": [], "items": [value]})
                continue
            text_limit = 1000 if event_type == "requirements.changed" else 100
            if len(key) > 40 or type(value) not in (str, int, float, bool) or (isinstance(value, str) and len(value) > text_limit):
                raise ValueError("이벤트 메타데이터가 올바르지 않습니다.")
            number_limit = 10 * 1024 * 1024 if key == "bytes" and event_type in {"photo.selected", "photo.dropped"} else 1000000
            if type(value) in (int, float) and (not math.isfinite(value) or abs(value) > number_limit):
                raise ValueError("이벤트 숫자 값이 올바르지 않습니다.")
        validated.append((identifier, event_type, payload))
    return validated
