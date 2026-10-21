"""Inspect existing batch receipts or convert approved results for Delta import.

This command never calls an embedding API. The source batch is never modified.
"""
import argparse
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import tempfile

from ..assets import checksum, read_json
from ..pipeline import contract_hash
from ..schemas import normalized_vector


def pixel_checksum(data):
    """Hash the RGB pixels independently of PNG compression and metadata."""
    from PIL import Image, ImageOps
    with Image.open(io.BytesIO(data)) as source:
        source.load()
        image = ImageOps.exif_transpose(source).convert('RGB')
        header = json.dumps({'mode': 'RGB', 'size': list(image.size)}, sort_keys=True, separators=(',', ':')).encode()
        return hashlib.sha256(b'preprocessed-pixels-v1\0' + header + b'\0' + image.tobytes()).hexdigest()


def completed(index_path):
    index_path = Path(index_path).resolve()
    root, index = index_path.parent, read_json(index_path)
    manifest = read_json(root / 'manifest.json')
    if manifest.get('mode') != 'LOCAL_IMAGE_BATCH' or contract_hash(manifest) != manifest['input_hash']:
        raise ValueError('BATCH_CONTRACT_CHANGED')
    expected = (index['model'], index['preprocessing_version'], index['dimension'])
    settings = manifest['settings']
    if expected != (settings['embedding_model'], settings['preprocessing_version'], settings['dimension']):
        raise ValueError('BATCH_SETTINGS_MISMATCH')
    def inside(name):
        path = (root / name).resolve()
        if not path.is_relative_to(root):
            raise ValueError('BATCH_PATH_ESCAPE')
        return path
    requested = {(r['stage'], r['asset'], r['source_checksum']) for r in manifest['request']['files']}
    seen = {}
    for item in index['files']:
        if item.get('status') != 'SUCCEEDED':
            continue
        if (item['stage'], item['asset'], item['source_checksum']) not in requested:
            raise ValueError('UNKNOWN_BATCH_IMAGE')
        stage = manifest['stages'][item['stage']]
        raw = inside(item['embedding_file']).read_bytes()
        asset = inside(item['asset']).read_bytes()
        value = json.loads(raw)
        if (stage['status'] != 'SUCCEEDED' or stage['output'] != item['embedding_file']
                or checksum(raw) != stage['output_checksum']
                or checksum(asset) != manifest['assets'][item['asset']]['checksum']
                or manifest['assets'][item['asset']]['source_checksum'] != item['source_checksum']
                or checksum(asset) != value['preprocessed_checksum']
                or value['image_checksum'] != item['source_checksum']
                or value.get('normalization') != 'L2'
                or expected != (value['model'], value['preprocessing_version'], value['dimension'])):
            raise ValueError('BATCH_OUTPUT_IDENTITY_MISMATCH')
        vector = normalized_vector(value['values'], value['dimension'])
        row = dict(image_checksum=value['image_checksum'], preprocessed_checksum=value['preprocessed_checksum'],
                   preprocessed_pixel_checksum=pixel_checksum(asset),
                   model_version=value['model'], preprocessing_version=value['preprocessing_version'],
                   dimension=value['dimension'], vector=vector)
        key = (row['image_checksum'], *expected)
        if key in seen:
            if seen[key] != checksum(json.dumps(row, sort_keys=True).encode()):
                raise ValueError('DUPLICATE_BATCH_VECTOR_CONFLICT')
            continue
        seen[key] = checksum(json.dumps(row, sort_keys=True).encode())
        yield row


def convert(index, output):
    output = Path(output).resolve()
    if output.exists():
        raise ValueError('IMMUTABLE_IMPORT_FILE_EXISTS')
    count = sum(1 for _ in completed(index))
    if not count:
        raise ValueError('NO_COMPLETED_BATCH_VECTORS')
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.batch-import-', dir=output.parent) as temporary:
        path = Path(temporary) / 'vectors.jsonl'
        stamp = datetime.now(timezone.utc).isoformat()
        with path.open('w', encoding='utf-8', newline='\n') as stream:
            for row in completed(index):
                stream.write(json.dumps(dict(row, created_at=stamp), sort_keys=True, allow_nan=False) + '\n')
        path.rename(output)
    return dict(status='CONVERTED', vectors=count, api_calls=0, output=str(output))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['inspect', 'convert'])
    parser.add_argument('--batch-index', type=Path, required=True)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--approved-reuse', action='store_true', help='사용자가 검증 결과의 재사용을 허가한 뒤에만 지정')
    args = parser.parse_args(argv)
    if args.command == 'inspect':
        print(json.dumps(dict(status='VERIFIED_BATCH', completed_vectors=sum(1 for _ in completed(args.batch_index)), api_calls=0)))
    elif not args.approved_reuse or args.output is None:
        parser.error('convert requires --approved-reuse and --output; inspect first and obtain approval')
    else:
        print(json.dumps(convert(args.batch_index, args.output)))


if __name__ == '__main__':
    main()
