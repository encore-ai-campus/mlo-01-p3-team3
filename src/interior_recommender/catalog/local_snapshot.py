"""Build an offline demo catalog from verified local and fixed Delta exports.

No network or embedding API calls. Inputs are immutable; output must be new.
"""
import argparse
from collections import Counter
from contextlib import ExitStack
from datetime import datetime, timezone
import hashlib
import io
import json
from pathlib import Path
import tempfile
import zipfile

import numpy as np
from PIL import Image

from ..assets import checksum
from ..domain.product_links import comparison_url
from ..domain.recommendation import ProductIndex
from .metadata import product_metadata


def file_hash(path):
    with Path(path).open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def build(local_products, server_archive, inventory_path, output, supplement=None):
    local_products, server_archive, inventory_path, output = map(
        lambda p: Path(p).resolve(), (local_products, server_archive, inventory_path, output))
    if output.exists():
        raise ValueError('Snapshot output already exists')
    local = json.loads(local_products.read_text(encoding='utf-8'))
    index = ProductIndex.load(local_products)
    inventory = json.loads(inventory_path.read_text(encoding='utf-8'))
    expected = {r['embedding_id']: r for r in inventory['records']}
    candidates, images, metadata = {}, {}, {}

    def add(row, original, image, vector, source):
        values = np.asarray(vector, dtype=np.float32)
        if values.shape != (local['dimension'],) or not np.isfinite(values).all() or not np.isclose(np.linalg.norm(values), 1, atol=2e-4):
            raise ValueError('Invalid completed vector')
        if original in images and checksum(images[original]) != checksum(image):
            with Image.open(io.BytesIO(images[original])) as left, Image.open(io.BytesIO(image)) as right:
                if left.convert('RGB').size != right.convert('RGB').size or left.convert('RGB').tobytes() != right.convert('RGB').tobytes():
                    raise ValueError('Conflicting pixels for one original image')
        images[original] = image
        product_id = row['product_id']
        identity = (product_id, original)
        candidates[identity] = (row, values, source)
        common = {k:v for k,v in row.items() if k not in ('image_id','image','image_checksum')}
        old = metadata.get(product_id)
        if old is None or str(common.get('observed_at') or '') > str(old.get('observed_at') or ''):
            metadata[product_id] = common

    for n, row in enumerate(local['products']):
        original = row['image_id'].rsplit(':',1)[-1]
        if len(original) != 64:
            raise ValueError('Local image lacks original checksum identity')
        add(row, original, Path(index.products[n].image).read_bytes(), index.vectors[n], 'LOCAL_COMPLETED_BATCH')

    with ExitStack() as stack:
        archives=[stack.enter_context(zipfile.ZipFile(server_archive))]
        if supplement:
            archives.append(stack.enter_context(zipfile.ZipFile(supplement)))
        archive=archives[0]
        server = json.loads(archive.read('vectors/manifest.json'))
        if (server['model_version'],server['dimension'],server['preprocessing_version']) != (local['embedding_model'],local['dimension'],local['preprocessing_version']):
            raise ValueError('Local/server embedding settings differ')
        if server['delta_version'] != inventory['delta_version'] or server['delta_path'] != inventory['delta_path']:
            raise ValueError('Server fixed version differs from inspected inventory')
        vectors = json.loads(archive.read('vectors/rows.json'))
        if len(vectors) != len(expected) or len({v['embedding_id'] for v in vectors}) != len(vectors):
            raise ValueError('Server inventory incomplete or duplicated')
        by_sha = {}
        for vector in vectors:
            if any(vector.get(k) != v for k,v in expected[vector['embedding_id']].items()):
                raise ValueError('Server export differs from verified inventory')
            body={k:vector[k] for k in ('image_checksum','preprocessed_checksum','model_version','preprocessing_version','dimension','embedding_id','vector','status')}
            if checksum(json.dumps(body,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode())!=vector['record_hash']:
                raise ValueError('Server vector content hash differs from inspected record')
            by_sha[vector['image_checksum']] = vector
        proof={}
        reports=[]
        for archive in archives:
            proof.update({p['image_checksum']:p for p in json.loads(archive.read('proof.json'))})
            for relation in json.loads(archive.read('relations.json')):
                product, image = relation['product'], relation['image']
                sha = image['sha256']
                vector = by_sha[sha]
                png = archive.read('images/'+sha+'.png')
                if checksum(png) != proof[sha]['exported_checksum'] or vector['preprocessed_checksum'] != proof[sha]['preprocessed_checksum']:
                    raise ValueError('Server image proof differs from vector')
                source=relation.get('metadata_source')
                if not source:
                    source = 'mongodb://pj3_products_raw/raw_products/'+relation['raw_document_id']+';document_hash='+relation['document_hash']+';raw_payload_hash='+product['raw_payload_hash']
                row = product_metadata(product, source)
                row.update(image_id='danawa:'+str(product['product_id'])+':'+sha,
                           comparison_url=comparison_url(product.get('product_url'),row['product_id']))
                add(row,sha,png,vector['vector'],'SERVER_DELTA_V'+str(server['delta_version']))
            reports.append(json.loads(archive.read('report.json')))
        report=dict(sources=reports,matched_images=len(proof),unmatched_vectors=sorted(set(by_sha)-set(proof)))

    used = {sha for _,sha in candidates}
    output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='.local-demo-',dir=output.parent) as temporary:
        bundle = Path(temporary)/'bundle'
        (bundle/'images').mkdir(parents=True)
        for sha in sorted(used):
            (bundle/'images'/f'{sha}.png').write_bytes(images[sha])
        rows, matrix, lineage = [], [], []
        for (pid,sha),(row,vector,source) in sorted(candidates.items()):
            rows.append(dict(metadata[pid],image_id=row['image_id'],image=f'images/{sha}.png',image_checksum=checksum(images[sha])))
            matrix.append(vector)
            lineage.append(dict(product_id=pid,image_id=row['image_id'],original_checksum=sha,vector_source=source))
        np.save(bundle/'vectors.npy',np.asarray(matrix,dtype=np.float32),allow_pickle=False)
        catalog=dict(test_only=True,bronze_verified=False,embedding_model=local['embedding_model'],
                     dimension=local['dimension'],preprocessing_version=local['preprocessing_version'],
                     products=rows,vectors_file='vectors.npy',vectors_checksum=file_hash(bundle/'vectors.npy'))
        (bundle/'products.json').write_text(json.dumps(catalog,ensure_ascii=False,indent=2),encoding='utf-8')
        # Run the same image/model/vector checks used by the actual worker.
        checked=ProductIndex.load(bundle/'products.json')
        del checked
        manifest=dict(contract='offline-local-demo-v1',created_at=datetime.now(timezone.utc).isoformat(),
                      products=len({p['product_id'] for p in rows}),image_relations=len(rows),unique_images=len(used),
                      categories=dict(Counter(p.get('category_code') or 'UNKNOWN' for p in rows)),api_calls=0,
                      local_products_checksum=file_hash(local_products),server_archive_checksum=file_hash(server_archive),
                      supplement_checksum=file_hash(supplement) if supplement else None,
                      inspected_inventory_checksum=file_hash(inventory_path),server_export=server,server_report=report,
                      lineage=lineage,server_image_proof=list(proof.values()),
                      files={p.relative_to(bundle).as_posix():file_hash(p) for p in sorted(bundle.rglob('*')) if p.is_file()})
        (bundle/'manifest.json').write_text(json.dumps(manifest,ensure_ascii=False,indent=2),encoding='utf-8')
        bundle.rename(output)
    return {k:manifest[k] for k in ('products','image_relations','unique_images','categories','api_calls')}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify',type=Path)
    parser.add_argument('--local-products',type=Path)
    parser.add_argument('--server-archive',type=Path)
    parser.add_argument('--server-inventory',type=Path)
    parser.add_argument('--supplement',type=Path)
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    if args.verify:
        root=args.verify.resolve()
        manifest=json.loads((root/'manifest.json').read_text(encoding='utf-8'))
        for name,digest in manifest['files'].items():
            path=(root/name).resolve()
            if not path.is_relative_to(root) or file_hash(path)!=digest:
                raise ValueError('Snapshot file checksum mismatch: '+name)
        checked=ProductIndex.load(root/'products.json')
        print(json.dumps(dict(status='VERIFIED',products=manifest['products'],image_relations=len(checked.products),unique_images=manifest['unique_images'],dimension=checked.dimension)))
    else:
        if not all((args.local_products,args.server_archive,args.server_inventory,args.output)):
            parser.error('Build requires --local-products, --server-archive, --server-inventory and --output')
        print(json.dumps(build(args.local_products,args.server_archive,args.server_inventory,args.output,args.supplement),ensure_ascii=False))


if __name__=='__main__':
    main()
