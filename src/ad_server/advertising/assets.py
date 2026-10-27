import hashlib
import io
from pathlib import Path
from PIL import Image, UnidentifiedImageError
from django.conf import settings
from .policy import TEMPLATES
from .errors import AdError

def banner(template_id):
    spec = next((r for r in TEMPLATES if r[0] == template_id), None)
    if not spec:
        raise AdError('UNKNOWN_TEMPLATE', '등록되지 않은 템플릿입니다.', 404)
    root = Path(settings.AD_BANNER_ROOT).resolve()
    p = (root / spec[2]).resolve()
    if not p.is_relative_to(root) or p.is_symlink():
        raise AdError('INVALID_ASSET', '배너 파일 경로를 확인하세요.', 404)
    try:
        data = p.read_bytes()
        with Image.open(io.BytesIO(data)) as im:
            fmt, size = im.format, im.size
            im.verify()
        if fmt != 'PNG' or size[1] != size[0] * 2:
            raise ValueError('Expected PNG with 1:2 ratio')
        return {'id': spec[0], 'name': spec[1], 'filename': spec[2], 'format': fmt,
                'width': size[0], 'height': size[1], 'bytes': len(data),
                'sha256': hashlib.sha256(data).hexdigest(), 'data': data}
    except (OSError, ValueError, UnidentifiedImageError, Image.DecompressionBombError) as exc:
        raise AdError('ASSET_UNAVAILABLE', '배너 파일의 형식·비율·연결을 확인하세요.', 503) from exc

def manifest():
    rows=[]
    for template_id, name, filename in TEMPLATES:
        try:
            r=banner(template_id); r.pop('data')
            r.update(status='READY', image_url=f'/assets/{template_id}/', embed_url=f'/embed/{template_id}/')
        except AdError as e:
            r={'id':template_id, 'name':name, 'filename':filename, 'status':e.code,
               'image_url':None, 'embed_url':None}
        rows.append(r)
    return rows
