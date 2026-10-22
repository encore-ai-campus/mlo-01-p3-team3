import io
import json
import zipfile

import numpy as np
from PIL import Image
import pytest

from interior_recommender.assets import checksum
from interior_recommender.catalog.local_snapshot import build
from interior_recommender.domain.recommendation import ProductIndex


def inputs(root, *, tamper=False):
    local=root/'local';local.mkdir()
    image=io.BytesIO();Image.new('RGB',(8,8),'white').save(image,format='PNG')
    png=image.getvalue();sha=checksum(png);(local/'image.png').write_bytes(png)
    vector=[1.0]+[0.0]*767
    settings=dict(embedding_model='gemini-embedding-2',dimension=768,preprocessing_version='exif-rgb-fit1024-png-l2-v1')
    product=dict(product_id='danawa:1',image_id='danawa:1:'+sha,name='local',image='image.png',image_checksum=sha,metadata_source='fixture')
    (local/'products.json').write_text(json.dumps(dict(settings,products=[product],vectors=[vector])))
    record=dict(image_checksum=sha,preprocessed_checksum=sha,model_version=settings['embedding_model'],preprocessing_version=settings['preprocessing_version'],dimension=768,embedding_id='a'*32,vector=vector,status='READY')
    record['record_hash']=checksum(json.dumps(record,ensure_ascii=False,sort_keys=True,separators=(',',':')).encode())
    inventory=dict(delta_version=43,delta_path='s3a://fixture/vectors',records=[{k:record[k] for k in ('embedding_id','image_checksum','preprocessed_checksum','record_hash')}])
    inventory_path=root/'inventory.json';inventory_path.write_text(json.dumps(inventory))
    if tamper:record['vector']=[0.0,1.0]+[0.0]*766
    archive=root/'server.zip'
    with zipfile.ZipFile(archive,'w') as z:
        z.writestr('vectors/manifest.json',json.dumps(dict(delta_version=43,delta_path=inventory['delta_path'],model_version=settings['embedding_model'],preprocessing_version=settings['preprocessing_version'],dimension=768)))
        z.writestr('vectors/rows.json',json.dumps([record]))
        z.writestr('proof.json',json.dumps([dict(image_checksum=sha,exported_checksum=sha,preprocessed_checksum=sha)]))
        z.writestr('relations.json',json.dumps([dict(product=dict(source='danawa',product_id=str(pid),name='server '+str(pid),raw_payload_hash='b'*64),image=dict(sha256=sha),metadata_source='fixture://verified-handoff') for pid in (2,3)]))
        z.writestr('report.json',json.dumps(dict(api_calls=0)))
        z.writestr('images/'+sha+'.png',png)
    return local/'products.json',archive,inventory_path


def test_shared_image_keeps_three_product_relations_with_one_image_file(tmp_path):
    paths=inputs(tmp_path)
    output=tmp_path/'snapshot'
    result=build(*paths,output)
    index=ProductIndex.load(output/'products.json')
    assert result['products']==3 and result['unique_images']==1
    assert index.vectors.shape==(3,768)
    assert len(list((output/'images').iterdir()))==1
    assert all(np.array_equal(v,index.vectors[0]) for v in index.vectors)
    assert all(path.exists() for path in paths)


def test_vector_with_valid_shape_and_norm_but_changed_contents_is_rejected(tmp_path):
    paths=inputs(tmp_path,tamper=True)
    output=tmp_path/'snapshot'
    with pytest.raises(ValueError,match='content hash'):
        build(*paths,output)
    assert not output.exists()


def test_existing_snapshot_is_never_overwritten(tmp_path):
    paths=inputs(tmp_path)
    output=tmp_path/'snapshot';output.mkdir();(output/'marker').write_text('keep')
    with pytest.raises(ValueError,match='already exists'):
        build(*paths,output)
    assert (output/'marker').read_text()=='keep'
