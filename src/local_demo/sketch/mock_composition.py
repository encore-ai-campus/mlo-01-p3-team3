"""Local workflow simulation: text rules and placeholders, not image recognition or generative AI."""
import io
from pathlib import Path

from PIL import Image, ImageDraw
from .catalog import PRODUCTS

KEYWORDS = {"plant": ("화분", "식물", "plant"), "sofa": ("소파", "쇼파", "sofa", "couch"),
            "bed": ("침대", "bed"), "desk": ("책상", "테이블", "desk", "table"), "lamp": ("조명", "램프", "lamp", "light")}


def visible_ink(document, width, height):
    mask = Image.new("L", (width, height), 0)
    for stroke in document["strokes"]:
        layer = Image.new("L", (width, height), 0) if stroke.get("cuts") else mask
        target = ImageDraw.Draw(layer)
        for part in [stroke, *stroke.get("cuts", [])]:
            radius = max(1, round(part["width"] * width / 2))
            fill = 0 if part["tool"] == "eraser" else 255
            points = [(round(point["x"] * width), round(point["y"] * height)) for point in part["points"]]
            if len(points) > 1:
                target.line(points, fill=fill, width=radius * 2, joint="curve")
            for x, y in points:
                target.ellipse((x - radius, y - radius, x + radius, y + radius), fill=fill)
        if layer is not mask:
            mask.paste(255, mask=layer)
    return mask.getbbox()


VARIANT_COLORS = {"cream": "#d7ccba", "wood": "#b98b66", "sage": "#8c9e82"}


def furniture(category, color, variant=None, render_style=None):
    if variant in VARIANT_COLORS:
        color = ({"cream":"#6d9273","wood":"#829858","sage":"#3e7161"}.get(variant) if category == "plant" else VARIANT_COLORS[variant])
    if category == "sofa" and render_style == "photo":
        asset = Path(__file__).parent / "static" / "sketch" / "assets" / f"sofa-{variant or 'cream'}.png"
        if asset.is_file():
            with Image.open(asset) as source:
                return source.convert("RGBA")
    image = Image.new("RGBA", (300, 300))
    draw = ImageDraw.Draw(image)
    outline = "#344d40"
    def box(rect, fill=color, radius=12):
        draw.rounded_rectangle(rect, radius, fill=fill, outline=outline, width=4)
    if category == "plant":
        draw.ellipse((65, 8, 235, 184), fill=color, outline=outline, width=4)
        draw.line((150, 40, 150, 215), fill=outline, width=5)
        draw.polygon(((92, 190), (208, 190), (190, 285), (110, 285)), fill="#b7845f", outline=outline, width=4)
    elif category == "lamp":
        draw.line((150, 65, 150, 270), fill=outline, width=8)
        box((85, 266, 215, 286)); draw.polygon(((85, 15), (215, 15), (265, 112), (35, 112)), fill=color, outline=outline, width=4)
    elif category == "bed":
        box((20, 8, 280, 290)); box((42, 24, 136, 95), "#f8f0de"); box((164, 24, 258, 95), "#f8f0de"); box((34, 110, 266, 273))
    elif category == "desk":
        box((28, 90, 51, 286)); box((249, 90, 272, 286)); box((5, 30, 295, 118))
    else:
        box((28, 35, 272, 182)); box((42, 128, 258, 245)); box((8, 100, 45, 254)); box((255, 100, 292, 254))
        draw.line((150, 135, 150, 235), fill=outline, width=4); box((40, 257, 62, 282)); box((238, 257, 260, 282))
    return image


def compose(project, document, text_requirements="", style_preferences=()):
    if not project.photo:
        raise ValueError("공간 사진을 먼저 업로드해 주세요.")
    regions = []
    for item in document["items"]:
        text = item.get("text", "").lower()
        category = next((category for category, words in KEYWORDS.items() if any(word in text for word in words)), "sofa")
        regions.append((item, category, "text-rule-demo" if text else "placeholder-demo"))
    bbox = visible_ink(document, project.width, project.height)
    if bbox:
        x, y, right, bottom = bbox
        regions.append(({"x": x / project.width, "y": y / project.height, "width": (right - x) / project.width, "height": (bottom - y) / project.height}, "sofa", "placeholder-demo"))
    if not regions and text_requirements.strip():
        regions.append(({"x": .22, "y": .48, "width": .54, "height": .30}, "sofa", "text-request-demo"))
    if not regions:
        raise ValueError("사진 위에 그림·텍스트·도형을 먼저 표시해 주세요.")
    # The complete annotation is one desired product; drawing primitives are not products.
    text = (text_requirements + " " + " ".join(item.get("text", "") for item in document["items"])).lower()
    category = next((category for category, words in KEYWORDS.items() if any(word in text for word in words)), "sofa")
    left = min(region["x"] for region, _, _ in regions)
    top = min(region["y"] for region, _, _ in regions)
    right = max(region["x"] + region["width"] for region, _, _ in regions)
    bottom = max(region["y"] + region["height"] for region, _, _ in regions)
    region = {"x": left, "y": top, "width": right - left, "height": bottom - top,
              "kind": "text" if all(item.get("kind") == "text" for item in document["items"]) and not bbox else "sketch"}
    regions = [(region, category, "text-rule-demo" if text else "whole-sketch-placeholder-demo")]
    placements = []
    for index, (region, category, basis) in enumerate(regions):
        product = next(product for product in PRODUCTS if product["category"] == category)
        w = min(.8, max(.08, region["width"]))
        h = min(.8, max(.08, region["height"]))
        if region.get("kind") == "text":
            w, h = max(w, product["width"]), max(h, product["height"])
        x = min(1 - w, max(0, region["x"] + region["width"] / 2 - w / 2))
        y = min(1 - h, max(0, region["y"] + region["height"] / 2 - h / 2))
        placements.append({"id": f"mock-{index + 1}", "product_id": product["id"], "name": product["name"], "price": product["price"],
            "category": category, "variant": "sage" if "세이지 그린" in style_preferences else "wood" if "오크 우드" in style_preferences else "cream", "render_style": "photo", "basis": basis, "x": x, "y": y, "width": w, "height": h})
    return render_placements(project, placements), placements


def render_placements(project, placements):
    with project.photo.open("rb") as source, Image.open(source) as original:
        image = original.convert("RGBA")
    for item in placements:
        product = next(product for product in PRODUCTS if product["id"] == item["product_id"])
        graphic = furniture(product["category"], product["color"], item.get("variant"), item.get("render_style")).resize(
            (max(1, round(item["width"] * image.width)), max(1, round(item["height"] * image.height))), Image.Resampling.LANCZOS)
        image.alpha_composite(graphic, (round(item["x"] * image.width), round(item["y"] * image.height)))
    stream = io.BytesIO()
    image.convert("RGB").save(stream, format="PNG")
    return stream.getvalue()
