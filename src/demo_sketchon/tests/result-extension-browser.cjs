// Run without Django or a live database: the real template, CSS and browser modules use intercepted APIs.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');
const root = path.join(__dirname, '..');
const copy = value => JSON.parse(JSON.stringify(value));
const svg = '<svg xmlns="http://www.w3.org/2000/svg" width="900" height="600"><rect width="900" height="600" fill="#e5e9de"/><rect x="40" y="70" width="300" height="180" fill="#8a9e7b"/></svg>';
const baseline = [
    {id:'p1',name:'소파',x:.1,y:.2,width:.25,height:.25},
    {id:'p2',name:'책상',x:.55,y:.25,width:.2,height:.3},
];
let result = {result_id:'result1',project_id:'project1',version:1,mode:'mock',photo_url:'/photo.svg',download_url:'/download/',data_url:'/data/',placements:copy(baseline),initial_placements:copy(baseline),category_options:{sofa:'소파',desk:'책상'},scene_objects:baseline.map((item,i)=>({id:'o'+(i+1),placement_id:item.id,label:item.name,category_id:i?'desk':'sofa',change_type:'ADDED',bbox:{x:item.x,y:item.y,width:item.width,height:item.height},preview_url:'/photo.svg'}))};
const saves=[], recommendations=[], reactions=[], revisions=[];
let failSave=false, recommendationFailure=null, holdRecommendation=null, revisionFailure=false;
function html() {
    return fs.readFileSync(path.join(root,'sketch/templates/sketch/result.html'),'utf8')
        .replace("{% include 'sketch/brand.html' %}",fs.readFileSync(path.join(root,'sketch/templates/sketch/brand.html'),'utf8'))
        .replace(/{% static '([^']+)' %}/g,(_,asset)=>'/static/'+asset)
        .replace('{{ initial|json_script:"result-data" }}','<script id="result-data" type="application/json">'+JSON.stringify(result)+'</script>')
        .replace('{{ visitor_id|json_script:"visitor-data" }}','<script id="visitor-data" type="application/json">"visitor1"</script>')
        .replace('{{ platform_config|json_script:"platform-config" }}','<script id="platform-config" type="application/json">{}</script>')
        .replace('{% csrf_token %}','<input name="csrfmiddlewaretoken" value="test">')
        .replace('{{ visitor_label }}','visitor1').replace('{{ prediction }}','소파')
        .replace('{{ width }}','900').replace('{{ height }}','600').replace('{{ initial.photo_url }}','/photo.svg')
        .replace('{{ initial.notice }}','모의 결과').replace(/{%[^%]*%}/g,'');
}
(async()=>{
    const browser=await chromium.launch({headless:true,channel:process.env.ROOMPICK_TEST_BROWSER || 'msedge'});
    try {
        const context=await browser.newContext({viewport:{width:1440,height:1000}}), page=await context.newPage(), errors=[];
        page.on('pageerror',error=>errors.push(error.message));
        await context.route('http://roompick.test/**',async route=>{
            const request=route.request(),url=new URL(request.url()),body=request.method()==='POST'?request.postDataJSON():null;
            const json=(data,status=200)=>route.fulfill({status,contentType:'application/json',body:JSON.stringify(data)});
            if(url.pathname==='/result/')return route.fulfill({contentType:'text/html',body:html()});
            if(url.pathname.startsWith('/static/')) {
                const file=path.join(root,'sketch/static',url.pathname.slice('/static/'.length));
                return route.fulfill({contentType:file.endsWith('.mjs')?'text/javascript':'text/css',body:fs.readFileSync(file,'utf8')});
            }
            if(url.pathname.endsWith('/placements/')) {
                saves.push(copy(body));
                if(failSave){failSave=false;return json({error:'버전 충돌'},409);}
                assert.equal(body.version,result.version);
                result.placements=body.placements.map(item=>({...baseline.find(row=>row.id===item.id),...item}));result.version++;
                result.scene_objects=result.scene_objects.map((obj,index)=>{const item=result.placements.find(row=>row.id===obj.placement_id);return {...obj,id:'o'+(index+1)+'v'+result.version,bbox:{x:item.x,y:item.y,width:item.width,height:item.height}};});
                return json(result);
            }
            if(url.pathname.endsWith('/recommendations/')) {
                recommendations.push(copy(body));
                const id=url.pathname.split('/')[3];
                if(holdRecommendation)await holdRecommendation;
                if(recommendationFailure){const failure=recommendationFailure;recommendationFailure=null;return json(failure.data,failure.status);}
                return json({status:'SUCCEEDED',scene_object_id:id,recommendation_run_id:'run'+recommendations.length,mode:'mock',items:[1,2,3].map(n=>({id:'item'+n,name:'샘플 '+n,brand:'RoomPick 예시',image_url:'/photo.svg',price:null,price_note:'판매 상품 아님',link_available:false}))});
            }
            if(url.pathname.endsWith('/revisions/')) {
                revisions.push(copy(body));if(revisionFailure){revisionFailure=false;return json({error:'품목 저장 지연'},503);}const old=url.pathname.split('/')[3],obj=result.scene_objects.find(row=>row.id===old);
                obj.id+='r';obj.category_id=body.category_id;obj.bbox=body.bbox;obj.user_corrected=true;return json(result);
            }
            if(url.pathname.includes('/recommendation-items/')&&url.pathname.endsWith('/events/')){reactions.push(copy(body));return json({ok:true});}
            if(url.pathname.endsWith('/events/'))return json({ok:true});
            if(url.pathname.startsWith('/api/jobs/'))return json({status:'FAILED',error:'old request'},404);
            return route.fulfill({contentType:'image/svg+xml',body:svg});
        });
        await page.goto('http://roompick.test/result/');
        await page.waitForFunction(()=>document.querySelectorAll('.placement').length===2);
        assert.match(await page.locator('h1').innerText(),/visitor1 님이 원하시는/);
        assert.equal(await page.locator('#edit-placement, #resize-selection, #search-region, .region-numbers, #region-x, #region-y').count(),0);
        assert.equal(await page.locator('#region-width, #region-height').count(),2);
        assert.equal(await page.locator('h1').innerText(),'visitor1 님이 원하시는 “소파”을 생성하였습니다.');
        assert.deepEqual(await page.locator('.result-board .board-result-actions > button').evaluateAll(nodes=>nodes.map(node=>node.id)),['download-png','redraw','edit-sketch','new-search']);
        assert.deepEqual(await page.locator('.board-result-actions [data-restart]').evaluateAll(nodes=>nodes.map(node=>node.dataset.restart)),['blank','sketch','new']);
        assert.equal(await page.locator('#download-png').innerText(),'이미지 내려받기');
        assert.equal(await page.locator('#download-png, #redraw, #edit-sketch, #new-search').count(),4);
        assert.equal(await page.locator('#download-json').count(),0);
        await page.locator('#view-original').click();
        await page.locator('#object-correction > summary').click();
        await page.waitForFunction(()=>document.querySelector('.placement.selected')?.dataset.placementId==='p1'&&!document.querySelector('#placements').hidden);
        assert.equal(await page.locator('#placements').evaluate(node=>node.inert),false);
        assert.equal(await page.locator('.placement.selected img').isVisible(),true);
        assert.equal(await page.locator('#object-category').inputValue(),'sofa');
        assert.equal(await page.locator('#save-object').isDisabled(),true);
        await page.locator('#object-correction-form').evaluate(form=>form.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true})));
        await page.waitForTimeout(100);assert.equal(revisions.length,0,'unchanged categories never call revision API');
        assert.equal(await page.locator('#region-width').inputValue(),'25');
        assert.equal(await page.locator('#region-height').inputValue(),'25');
        await page.locator('#object-category').selectOption('desk');
        await page.locator('#region-width').fill('32');await page.locator('#region-width').press('Tab');
        await page.waitForFunction(()=>!document.querySelector('#download-png').disabled);
        assert.equal(result.placements[0].width,.32,'numeric width saves the actual image placement');
        assert.equal(await page.locator('#object-category').inputValue(),'desk','numeric resize preserves pending category');
        await page.locator('#object-category').selectOption('sofa');
        await page.locator('#region-height').fill('36');await page.locator('#region-height').press('Tab');
        await page.waitForFunction(()=>!document.querySelector('#download-png').disabled);
        assert.equal(result.placements[0].height,.36,'numeric height saves the actual image placement');
        assert.equal(await page.locator('#save-object').isDisabled(),true,'size changes save independently of category changes');
        await page.locator('#region-width').fill('99');await page.locator('#region-width').press('Tab');
        assert.equal(saves.length,2,'out-of-photo dimensions cannot be saved');
        await page.locator('#region-width').fill('32');
        await page.locator('#undo-placement').click();await page.waitForFunction(()=>!document.querySelector('#redo-placement').disabled);
        assert.equal(result.placements[0].height,.25);assert.equal(await page.locator('#region-height').inputValue(),'25');
        await page.locator('#redo-placement').click();await page.waitForFunction(()=>!document.querySelector('#download-png').disabled);
        assert.equal(result.placements[0].height,.36);
        failSave=true;await page.locator('#region-height').fill('40');await page.locator('#region-height').press('Tab');
        await page.waitForFunction(()=>document.querySelector('#status').textContent.includes('버전 충돌'));
        assert.equal(result.placements[0].height,.36);assert.equal(await page.locator('#region-height').inputValue(),'36','failed numeric resize restores stored dimensions');
        await page.locator('#reset-placement').click();await page.waitForFunction(()=>document.querySelector('#reset-placement').disabled);
        saves.length=0;
        await page.locator('#object-correction > summary').click();
        assert.equal(await page.locator('[data-product-slot]').count(),3);
        await page.locator('[data-placement-id="p1"]').click();
        assert.equal(saves.length,0,'a click selects without a version change');
        assert.equal(await page.locator('#recommend').isEnabled(),true);
        const drag=async(id='p1')=>{
            await page.locator('[data-placement-id="'+id+'"]').scrollIntoViewIfNeeded();
            const box=await page.locator('[data-placement-id="'+id+'"]').boundingBox();
            await page.mouse.move(box.x+box.width/2,box.y+box.height/2);await page.mouse.down();
            await page.mouse.move(box.x+box.width/2+55,box.y+box.height/2+25,{steps:5});await page.mouse.up();
        };
        await drag();await page.waitForFunction(()=>!document.querySelector('#undo-placement').disabled);
        assert.equal(saves.length,1);const moved=copy(result.placements);
        await page.reload();await page.waitForFunction(()=>!document.querySelector('#undo-placement').disabled);
        assert.equal(await page.locator('#reset-placement').isEnabled(),true);
        const stored=()=>page.evaluate(()=>Object.fromEntries(Object.keys(sessionStorage).filter(key=>key.startsWith('roompick.placement-history.')).map(key=>[key,sessionStorage.getItem(key)])));
        const beforeFailure=await stored();failSave=true;await page.locator('#undo-placement').click();
        await page.waitForFunction(()=>document.querySelector('#status').textContent.includes('버전 충돌'));
        assert.deepEqual(await stored(),beforeFailure,'failed CAS preserves both stacks');assert.deepEqual(result.placements,moved);
        await page.locator('#undo-placement').click();await page.waitForFunction(()=>!document.querySelector('#redo-placement').disabled);
        assert.deepEqual(result.placements,baseline);
        await page.locator('#redo-placement').click();await page.waitForFunction(()=>!document.querySelector('#undo-placement').disabled);assert.deepEqual(result.placements,moved);
        await page.reload();await page.waitForFunction(()=>!document.querySelector('#reset-placement').disabled);
        await page.locator('#reset-placement').click();await page.waitForFunction(()=>document.querySelector('#reset-placement').disabled);assert.deepEqual(result.placements,baseline,'reset uses server creation placement after reload');
        await page.locator('#view-original').click();assert.equal(await page.locator('#placements').isHidden(),true);
        await page.locator('#view-result').click();assert.equal(await page.locator('#placements').isVisible(),true);
        await page.locator('[data-object-id="'+result.scene_objects[0].id+'"]').click();
        recommendationFailure={status:503,data:{error:'일시 지연'}};await page.locator('#recommend').click();
        await page.locator('#retry-recommendation').waitFor({state:'visible'});assert.equal(await page.locator('[data-product-slot]').count(),3);
        await page.locator('#retry-recommendation').click();await page.locator('[data-item-id="item3"]').waitFor();
        assert.equal(recommendations[0].request_id,recommendations[1].request_id,'network retries reuse one request id');
        await page.locator('[data-item-id="item1"]').scrollIntoViewIfNeeded();
        await page.waitForFunction(()=>!Object.keys(localStorage).some(key=>key.startsWith('roompick.product-events.')&&key.includes('.pending.')));
        await page.locator('[data-item-id="item1"] .product-actions button').nth(0).click();
        await page.locator('[data-item-id="item1"] .product-actions button').nth(1).click();
        await page.waitForTimeout(150);
        assert.equal(reactions.filter(row=>row.event_type==='PRODUCT_IMPRESSION').length>0,true);
        assert.equal(reactions.some(row=>row.event_type==='PRODUCT_SELECTED'),true);assert.equal(reactions.some(row=>row.event_type==='PRODUCT_REJECTED'),true);
        await page.locator('#recommend').click();await page.waitForFunction(()=>!document.querySelector('#recommend').disabled);
        assert.notEqual(recommendations[2].request_id,recommendations[1].request_id,'an explicit new recommendation starts a new run');
        recommendationFailure={status:200,data:{status:'FAILED',error:'추천 실패'}};await page.locator('#recommend').click();await page.locator('#retry-recommendation').waitFor({state:'visible'});
        const failedId=recommendations.at(-1).request_id;await page.locator('#retry-recommendation').click();await page.waitForFunction(()=>!document.querySelector('#recommend').disabled);
        assert.notEqual(recommendations.at(-1).request_id,failedId,'terminal failed jobs get a fresh request id');
        let release;holdRecommendation=new Promise(resolve=>release=resolve);
        await page.locator('#recommend').click();await page.locator('[data-object-id="'+result.scene_objects[1].id+'"]').click();release();holdRecommendation=null;
        await page.waitForTimeout(150);assert.equal(await page.locator('[data-item-id]').count(),0,'late response cannot populate another selected object');
        assert.equal(await page.locator('[data-product-slot]').count(),3);
        await page.locator('#view-original').click();
        await page.locator('#object-correction > summary').click();
        await page.waitForFunction(()=>!document.querySelector('#placements').hidden);
        assert.equal(await page.locator('.placement.selected').getAttribute('data-placement-id'),'p2','opening correction keeps the selected furniture');
        assert.equal(await page.locator('#save-object').isDisabled(),true);
        await page.locator('#object-category').selectOption('sofa');assert.equal(await page.locator('#save-object').isEnabled(),true);
        await page.locator('#object-category').selectOption('desk');assert.equal(await page.locator('#save-object').isDisabled(),true);
        const revisionCount=revisions.length;
        await page.locator('#object-correction-form').evaluate(form=>form.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true})));
        await page.waitForTimeout(100);assert.equal(revisions.length,revisionCount);
        await page.locator('#object-category').selectOption('sofa');
        revisionFailure=true;await page.locator('#save-object').click();
        await page.waitForFunction(()=>document.querySelector('#object-correction-status').textContent.includes('품목 저장 지연'));
        const failedRevision=revisions.at(-1).request_id,versionBeforeMove=result.version;
        await drag('p2');await page.waitForFunction(()=>!document.querySelector('#download-png').disabled);
        assert.equal(result.version,versionBeforeMove+1,'the real furniture remains movable while correction is open');
        assert.equal(await page.locator('#object-category').inputValue(),'sofa','unsaved category survives object/version refresh');
        assert.equal(await page.locator('#save-object').isEnabled(),true);
        assert.deepEqual(result.scene_objects[1].bbox,(({x,y,width,height})=>({x,y,width,height}))(result.placements[1]));
        const beforeCorner=copy(result.placements[1]);
        await page.locator('[data-placement-id="p2"] .resize-handle').scrollIntoViewIfNeeded();
        const corner=await page.locator('[data-placement-id="p2"] .resize-handle').boundingBox();
        await page.mouse.move(corner.x+corner.width/2,corner.y+corner.height/2);await page.mouse.down();
        await page.mouse.move(corner.x+corner.width/2+15,corner.y+corner.height/2+15,{steps:4});await page.mouse.up();
        await page.waitForFunction(()=>!document.querySelector('#download-png').disabled);
        assert.equal(result.placements[1].width>beforeCorner.width,true,'the real corner handle resizes during correction');
        assert.equal(result.placements[1].x,beforeCorner.x);
        const beforeKeyboard=copy(result.placements[1]);
        await page.locator('[data-placement-id="p2"]').focus();await page.keyboard.press('Shift+ArrowRight');
        await page.waitForFunction(()=>!document.querySelector('#download-png').disabled);
        assert.equal(result.placements[1].width>beforeKeyboard.width,true,'Shift arrows resize without a resize mode button');
        assert.equal(result.placements[1].x,beforeKeyboard.x);
        assert.equal(await page.locator('#object-category').inputValue(),'sofa');
        const placementsBeforeCorrection=copy(result.placements),version=result.version;
        await page.locator('#save-object').click();
        await page.waitForFunction(()=>document.querySelector('#object-correction-status').textContent.includes('적용됐어요'));
        assert.notEqual(revisions.at(-1).request_id,failedRevision,'new object versions reset a failed correction request id');
        assert.deepEqual(revisions.at(-1).bbox,(({x,y,width,height})=>({x,y,width,height}))(result.placements[1]),'correction uses the actual current furniture bounds');
        assert.deepEqual(result.placements,placementsBeforeCorrection);assert.equal(result.version,version);
        assert.equal(await page.locator('#save-object').isDisabled(),true,'successful category revision becomes the current default');
        await page.locator('[data-placement-id="p1"]').click();
        assert.equal(await page.locator('#object-category').inputValue(),'sofa');
        await page.locator('#object-category').selectOption('desk');
        await page.locator('[data-placement-id="p2"]').click();
        assert.equal(await page.locator('#object-category').inputValue(),'sofa','switching furniture refreshes category defaults');
        assert.equal(await page.locator('#save-object').isDisabled(),true);
        await page.setViewportSize({width:390,height:844});
        assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth),true,'mobile result has no horizontal overflow');
        assert.deepEqual(errors,[]);
        console.log('Result browser behavior passed: direct drag/click, CAS/history/reload/reset, original toggle, stable slots, impressions/reactions, request retry/staleness, real-placement correction/resize, category dirtiness, mobile layout.');
    } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
