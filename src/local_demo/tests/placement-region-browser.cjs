// Isolated local server only. Intercept recommendation admission: no paid API calls.
const assert = require('node:assert/strict');
const {chromium} = require('playwright');
const base = process.env.ROOMPICK_TEST_URL || 'http://127.0.0.1:18037';

(async () => {
    const browser = await chromium.launch({headless:true, channel:'msedge'});
    try {
        const context = await browser.newContext({viewport:{width:1440,height:1100}});
        const page = await context.newPage();
        const errors=[];
        page.on('pageerror', error => {errors.push(error.message); console.error('Page error:',error.message);});
        // An existing tab can retain the geometry module from before alphaBounds.
        const staleGeometry=require('node:fs').readFileSync(require('node:path').join(__dirname,'../sketch/static/sketch/geometry.mjs'),'utf8')
            .replace('export function alphaBounds','function alphaBounds');
        await page.route(base+'/static/sketch/geometry.mjs',route=>route.fulfill({contentType:'text/javascript',body:staleGeometry}));
        await require('./browser-member.cjs')(context,base);
        await page.goto(base);
        await page.locator('#start-sketch').click();
        await page.waitForFunction(() => Object.keys(sessionStorage).some(key=>key.startsWith('local_demo.history.')) && !document.querySelector('#search').disabled);
        await page.locator('#sample-empty').click();
        await page.waitForFunction(() => !document.querySelector('#room-photo').hidden && !document.querySelector('#search').disabled);
        await page.locator('[data-sticker="office-chair"]').click();
        await page.waitForFunction(() => !document.querySelector('#search').disabled && document.querySelector('#counts').textContent.includes('1개'));
        const sticker=await page.evaluate(() => {
            const key=Object.keys(sessionStorage).find(key=>key.startsWith('local_demo.history.'));
            return JSON.parse(sessionStorage.getItem(key)).drawing.items.find(item=>item.kind==='sticker');
        });
        await page.locator('[data-tool="text"]').click();
        await page.locator('#surface').scrollIntoViewIfNeeded();
        const box=await page.locator('#surface').boundingBox();
        await page.locator('#surface').click({position:{x:box.width*.08,y:box.height*.08}});
        await page.locator('#inline-text').fill('location annotation');
        await page.locator('#inline-text').press('Enter');
        let captured;
        await page.route(base+'/interior-recommand/api/runs',async route=>{
            const request=route.request();
            const form=await new Response(request.postDataBuffer(),{headers:{'Content-Type':request.headers()['content-type']}}).formData();
            captured={region:JSON.parse(form.get('requested_region')),text:form.get('text')};
            await route.fulfill({status:400,json:{error:'Browser verification stopped before any model call.'}});
        });
        await page.locator('#search-top').click();
        await page.waitForFunction(() => !document.querySelector('#search').disabled);
        assert.ok(captured?.region,'visible artwork supplies an explicit placement region');
        const r=captured.region;
        assert.ok(r.x_min>=sticker.x-.01 && r.x_max<=sticker.x+sticker.width+.01);
        assert.ok(r.y_min>=sticker.y-.01 && r.y_max<=sticker.y+sticker.height+.01);
        assert.ok(r.x_min>.15 && r.y_min>.15,'text annotation outside the furniture is excluded');
        assert.ok(r.x_max>r.x_min && r.y_max>r.y_min);
        assert.equal(captured.text,'','canvas labels are not copied into the description');
        const previousPhoto=await page.locator('#room-photo').getAttribute('src');
        const uploaded=await (await context.request.get(base+previousPhoto)).body();
        page.once('dialog',dialog=>dialog.accept());
        await page.locator('#photo-file').setInputFiles({name:'uploaded-room.png',mimeType:'image/png',buffer:uploaded});
        await page.waitForFunction(previous=>document.querySelector('#room-photo').getAttribute('src')!==previous &&
            !document.querySelector('#search').disabled && document.querySelector('#room-photo').naturalWidth>0,previousPhoto);
        assert.deepEqual(errors,[]);
        console.log('PASS: stale geometry cache tolerated; sample/photo upload work; placement bounds sent; no model API calls');
        await context.close();
    } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1;});
