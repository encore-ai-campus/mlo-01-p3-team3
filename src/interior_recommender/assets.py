import hashlib
import io
import json
import math
import os
import time
from pathlib import Path
from uuid import uuid4

from PIL import Image, ImageOps

from .schemas import Box


def checksum(data: bytes):
    return hashlib.sha256(data).hexdigest()


def write_bytes(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + "." + uuid4().hex + ".tmp")
    try:
        with temp.open("wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        # Windows readers can briefly hold a destination without delete sharing.
        for attempt in range(8):
            try:
                os.replace(temp, path)
                break
            except PermissionError:
                if attempt == 7:
                    raise
                time.sleep(min(.01 * (attempt + 1), .05))
    finally:
        temp.unlink(missing_ok=True)


def write_json(path: Path, value):
    write_bytes(path, json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False).encode("utf-8"))


def read_json(path: Path):
    # A concurrent atomic replacement can briefly deny a read on Windows.
    for attempt in range(8):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except PermissionError:
            if attempt == 7:
                raise
            time.sleep(min(.01 * (attempt + 1), .05))


def png_bytes(data: bytes, *, resize=False, white_background=False, allow_webp=False):
    if len(data) > 15 * 1024 * 1024:
        raise ValueError("Image exceeds the MVP 15 MiB limit")
    with Image.open(io.BytesIO(data)) as image:
        if image.format not in ({"JPEG", "PNG", "WEBP"} if allow_webp else {"JPEG", "PNG"}):
            raise ValueError("Only PNG and JPEG images are supported")
        image.load()
        image = ImageOps.exif_transpose(image)
        if white_background and (image.mode in {"RGBA", "LA"} or "transparency" in image.info):
            foreground = image.convert("RGBA")
            background = Image.new("RGBA", image.size, "white")
            image = Image.alpha_composite(background, foreground).convert("RGB")
        else:
            image = image.convert("RGB")
        if resize:
            image.thumbnail((1024, 1024), Image.Resampling.LANCZOS)
        output = io.BytesIO()
        image.save(output, format="PNG")
        return output.getvalue(), image.size


def crop_bytes(data: bytes, box: Box):
    with Image.open(io.BytesIO(data)) as image:
        width, height = image.size
        bounds = (math.floor(box.x_min * width), math.floor(box.y_min * height),
                  math.ceil(box.x_max * width), math.ceil(box.y_max * height))
        crop = image.crop(bounds)
        output = io.BytesIO()
        crop.save(output, format="PNG")
        return output.getvalue(), crop.size


def generation_reference(data: bytes):
    """Fit without stretching/cropping; preserve the transform for normalized boxes."""
    with Image.open(io.BytesIO(data)) as image:
        width, height = image.size
        scale = min(1, 480 / max(width, height))
        scaled = (max(1, round(width * scale)), max(1, round(height * scale)))
        image = image.convert("RGB").resize(scaled, Image.Resampling.LANCZOS)
        side = max(scaled)
        offset = ((side - scaled[0]) // 2, (side - scaled[1]) // 2)
        canvas = Image.new("RGB", (side, side), "white")
        canvas.paste(image, offset)
        output = io.BytesIO()
        canvas.save(output, format="PNG")
        return output.getvalue(), {"original_size": [width, height], "size": [side, side],
                                  "scale": [scaled[0] / width, scaled[1] / height],
                                  "offset": list(offset), "padding": "white-square-center"}
