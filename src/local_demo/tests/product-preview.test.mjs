import test from 'node:test';
import assert from 'node:assert/strict';
import {createProductPreview} from '../../interior_recommender/web/ui/product-preview.mjs';

globalThis.window = {addEventListener() {}};
function fixture(api) {
  const elements = {};
  const panel = {hidden:true, querySelector: key => elements[key] ||= {
    hidden:true, textContent:'', handlers:{}, addEventListener(event, handler) {this.handlers[event] = handler;},
    removeAttribute(name) {delete this[name];}
  }};
  return {panel, elements, preview:createProductPreview({api, base:'/interior-recommand/', parent:()=> 'parent', panel})};
}
const item = {image_id:'product-image', name:'추천 의자'};
const completed = {job:{status:'SUCCEEDED'}, selected_product:item, demo:true, disclaimer:'AI 시안', result_image:'generated.png'};

test('selection posts only a candidate ID and renders a labeled owned preview', async () => {
  const calls=[];
  const f=fixture(async (path, options) => {calls.push({path, options}); return options ? {run_id:'child'} : completed;});
  await f.preview.start(item);
  assert.deepEqual(JSON.parse(calls[0].options.body), {image_id:'product-image'});
  assert.equal(calls[0].path, 'api/runs/parent/previews');
  assert.equal(f.elements['[data-preview-image]'].src, '/interior-recommand/assets/child/generated.png');
  assert.match(f.elements['[data-preview-status]'].textContent, /데모 이미지.*AI 시안/);
  f.preview.clear();
  assert.equal(f.panel.hidden,true);
  assert.equal(f.elements['[data-preview-image]'].src,undefined);
});

test('account/input invalidation discards a late preview response', async () => {
  let release;
  const wait = new Promise(resolve => release=resolve);
  const f=fixture(async (path, options) => options ? {run_id:'child'} : wait);
  const pending=f.preview.start(item);
  await Promise.resolve(); await Promise.resolve();
  f.preview.clear(); release(completed); await pending;
  assert.equal(f.panel.hidden,true);
  assert.equal(f.elements['[data-preview-image]'].src,undefined);
});

test('retry targets the failed preview without resubmitting a recommendation', async () => {
  const calls=[];
  let failed=true;
  const f=fixture(async (path, options) => {
    calls.push(path);
    if(path.endsWith('/actions/prepare')) {failed=false; return {};}
    return options ? {run_id:'child'} : failed ? {...completed, job:{status:'FAILED',error:'생성 실패'},result_image:null} : completed;
  });
  await f.preview.start(item);
  assert.equal(f.elements['[data-preview-retry]'].hidden,false);
  await f.elements['[data-preview-retry]'].handlers.click();
  assert.ok(calls.includes('api/runs/child/actions/prepare'));
  assert.equal(calls.filter(path=>path.endsWith('/previews')).length,1);
  assert.equal(f.elements['[data-preview-image]'].hidden,false);
});
