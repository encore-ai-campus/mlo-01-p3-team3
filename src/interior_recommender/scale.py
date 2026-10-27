"""Visual size bounds, kept separate from measured product dimensions."""
import copy


def api_scale_bounds(plan, room_mapping):
    """Project original-image span ratios into the padded generator image."""
    result = {}
    for field, axis in (("width_ratio", 0), ("height_ratio", 1)):
        bounds = plan.get(field)
        factor = (room_mapping["original_size"][axis] * room_mapping["scale"][axis]
                  / room_mapping["size"][axis])
        result[field] = {key: value * factor for key, value in bounds.items()} if bounds else None
    return result


def assess_scale(result, scale_plan, bounds, category):
    """A model pass cannot override missing evidence or numeric size violations."""
    check = copy.deepcopy(result["scale_check"])
    targets = [obj for obj in result["objects"]
               if obj["change_type"] in {"ADDED", "REPLACED"} and obj["category_code"] == category]
    if (not scale_plan or scale_plan.get("confidence") not in {"HIGH", "MEDIUM"}
            or not bounds or not bounds.get("width_ratio") or not bounds.get("height_ratio")
            or check["room_geometry"] != "PRESERVED" or len(targets) != 1):
        check.update(status="UNCERTAIN", reason="크기 근거·원근·배경 유지 또는 단일 대상 대응을 확인할 수 없습니다. " + check["reason"])
        return check
    box = targets[0]["box"]
    observed = {"width_ratio": box["x_max"] - box["x_min"],
                "height_ratio": box["y_max"] - box["y_min"]}
    too_small = any(value < bounds[key]["minimum"] - 1e-6 for key, value in observed.items())
    too_large = any(value > bounds[key]["maximum"] + 1e-6 for key, value in observed.items())
    check["observed_api_ratios"] = observed
    check["expected_api_bounds"] = copy.deepcopy(bounds)
    if too_small or too_large:
        check["status"] = "OUT_OF_RANGE" if too_small and too_large else "TOO_SMALL" if too_small else "TOO_LARGE"
        check["findings"].append("생성 물체 영역의 가로·높이가 고정된 투영 크기 범위를 벗어났습니다.")
    return check
