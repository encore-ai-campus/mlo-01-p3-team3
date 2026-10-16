const assert = require('node:assert/strict');
const {chromium} = require('playwright');
const base = process.env.ROOMPICK_TEST_URL || 'http://127.0.0.1:8015';
(async () => {
    const browser = await chromium.launch({headless:true, channel:'msedge'});
    try {
        for (const mobile of [false, true]) {
            const context = await browser.newContext({viewport:mobile ? {width:390,height:844} : {width:1440,height:1000}, hasTouch:mobile, isMobile:mobile});
            const page = await context.newPage(), errors=[];
            page.on('pageerror', error => errors.push(error.message));
            await require('./browser-member.cjs')(context,base);
        await page.goto(base); await page.locator('#start-sketch').click();
            const visitor = await page.locator('#visitor-data').textContent();
            assert.equal(await page.locator('#undo').isDisabled(), true);
            await page.locator('#sample-empty').click({timeout:3000});
            await page.waitForFunction(() => !document.querySelector('#room-photo').hidden && !document.querySelector('#search').disabled);
            assert.equal(await page.locator('#undo').isDisabled(), false);
            const uploaded = page.url();
            await page.locator('#undo').click(); await page.waitForFunction(() => document.querySelector('#room-photo').hidden);
            await page.locator('#redo').click(); await page.waitForURL(uploaded);
            if (mobile) await page.locator('#tab-description').click();
            const text = page.locator('#text-requirements');
            await text.fill('창가에 작은 화분을 놓아 주세요'); await text.blur();
            await page.locator('[data-preference="크림 패브릭"]').click();
            await page.locator('[data-preference="오크 우드"]').click();
            assert.equal(await page.locator('[data-preference][aria-pressed=true]').count(), 1);
            if (mobile) await page.locator('#tab-sketch').click();
            await page.locator('#undo').click();
            assert.equal(await page.locator('[data-preference="크림 패브릭"]').getAttribute('aria-pressed'), 'true');
            await page.locator('#undo').click(); await page.locator('#undo').click();
            assert.equal(await text.inputValue(), '');
            await page.locator('#redo').click(); await page.locator('#redo').click(); await page.locator('#redo').click();
            assert.equal(await text.inputValue(), '창가에 작은 화분을 놓아 주세요');
            // Unsaved input travels through crop and its undo/redo.
            await page.locator('#crop').click();
            await page.locator('#surface').scrollIntoViewIfNeeded();
            const box = await page.locator('#surface').boundingBox();
            await page.mouse.move(box.x+box.width*.1, box.y+box.height*.1); await page.mouse.down();
            await page.mouse.move(box.x+box.width*.9,box.y+box.height*.9,{steps:8}); await page.mouse.up();
            await page.locator('#apply-crop').click(); await page.waitForURL(url => url.toString()!==uploaded);
            assert.equal(await text.inputValue(), '창가에 작은 화분을 놓아 주세요');
            const cropped=page.url(); await page.reload();
            await page.waitForFunction(() => !document.querySelector('#search').disabled);
            assert.equal(await text.inputValue(), '창가에 작은 화분을 놓아 주세요');
            await page.locator('#undo').click(); await page.waitForURL(uploaded);
            assert.equal(await text.inputValue(), '창가에 작은 화분을 놓아 주세요');
            await page.locator('#redo').click(); await page.waitForURL(cropped);
            for (const id of ['fill','ellipse','arrow','group-selection','resize-selection']) assert.equal(await page.locator('#'+id).isVisible(),true);
            await page.locator(mobile ? '#search-mobile' : '#search').click(); await page.waitForURL('**/results/**');
            const result=JSON.parse(await page.locator('#result-data').textContent());
            assert.equal(result.placements.length,1); assert.equal(result.generation_context.text_requirements, '창가에 작은 화분을 놓아 주세요');
            assert.deepEqual(result.generation_context.style_preferences,['오크 우드']);
            assert.match(await page.locator('h1').textContent(), /님이 원하시는/);
            assert.equal(await page.locator('[data-product-slot]').count(),3);
            const item=page.locator('.placement').first();
            await item.click();
            const recommendation=page.waitForResponse(r=>r.url().endsWith('/recommendations/') && r.request().method()==='POST');
            await page.locator('#recommend').click(); assert.equal((await recommendation).status(),201);
            await page.waitForFunction(()=>document.querySelectorAll('[data-product-slot] .product-actions').length===3);
            for (const image of await page.locator('[data-product-slot] img').all()) {
                await image.scrollIntoViewIfNeeded(); await image.evaluate(image=>image.decode());
            }
            let reacted=page.waitForResponse(r=>r.url().includes('/recommendation-items/')&&r.url().endsWith('/events/')&&r.request().postDataJSON()?.event_type==='PRODUCT_SELECTED');
            await page.getByRole('button',{name:'마음에 들어요',exact:true}).first().click(); assert.equal((await reacted).status(),200);
            reacted=page.waitForResponse(r=>r.url().includes('/recommendation-items/')&&r.url().endsWith('/events/')&&r.request().postDataJSON()?.event_type==='PRODUCT_REJECTED');
            await page.getByRole('button',{name:'다른 취향이에요',exact:true}).first().click(); assert.equal((await reacted).status(),200);
            await page.screenshot({path:`test-results/extension/${mobile?'mobile':'desktop'}-result.png`,fullPage:true});
            // Search correction is independent from the composition bitmap and its placement version.
            await page.locator('#object-correction summary').first().click();
            await page.locator('#object-category').selectOption('desk');
            const corrected=page.waitForResponse(r=>r.url().endsWith('/revisions/'));
            await page.locator('#save-object').click();
            const correctedResult=await (await corrected).json();
            assert.equal(correctedResult.image_url,result.image_url); assert.equal(correctedResult.version,result.version);
            assert.equal(correctedResult.scene_objects[0].category_id,'desk');
            await page.locator('#object-correction summary').first().click();
            // Direct dragging, placement undo/redo, reset and original comparison use the real server.
            await item.scrollIntoViewIfNeeded(); const itemBox=await item.boundingBox();
            let saved=page.waitForResponse(r=>r.url().endsWith('/placements/'));
            await page.mouse.move(itemBox.x+itemBox.width*.4,itemBox.y+itemBox.height*.4); await page.mouse.down();
            await page.mouse.move(itemBox.x+itemBox.width*.4+22,itemBox.y+itemBox.height*.4+10,{steps:6}); await page.mouse.up();
            assert.equal((await saved).status(),200);
            await page.waitForFunction(()=>!document.querySelector('#undo-placement').disabled);
            saved=page.waitForResponse(r=>r.url().endsWith('/placements/')); await page.locator('#undo-placement').click(); assert.equal((await saved).status(),200);
            await page.waitForFunction(()=>!document.querySelector('#redo-placement').disabled);
            saved=page.waitForResponse(r=>r.url().endsWith('/placements/')); await page.locator('#redo-placement').click(); assert.equal((await saved).status(),200);
            await page.waitForFunction(()=>!document.querySelector('#reset-placement').disabled);
            saved=page.waitForResponse(r=>r.url().endsWith('/placements/')); await page.locator('#reset-placement').click(); assert.equal((await saved).status(),200);
            await page.waitForFunction(()=>!document.querySelector('#view-original').disabled);
            await page.locator('#view-original').click(); assert.equal(await page.locator('#placements').isVisible(),false);
            await page.locator('#view-result').click(); assert.equal(await page.locator('#placements').isVisible(),true);
            for(const [id,name] of [['download-png','sketchon-result.png']]) {
                const downloaded=page.waitForEvent('download'); await page.locator('#'+id).click(); assert.equal((await downloaded).suggestedFilename(),name);
            }
            await page.locator('#edit-sketch').click(); await page.waitForURL('**/editor/**');
            await page.waitForFunction(() => !document.querySelector('#search').disabled);
            assert.equal(await text.inputValue(), '창가에 작은 화분을 놓아 주세요');
            assert.equal(await page.locator('[data-preference="오크 우드"]').getAttribute('aria-pressed'), 'true');
            assert.equal(await page.locator('#visitor-data').textContent(),visitor);
            assert.ok(await page.evaluate(() => document.documentElement.scrollWidth<=innerWidth+1));
            assert.deepEqual(errors,[]);
            await page.screenshot({path:`test-results/extension/${mobile?'mobile':'desktop'}-editor.png`,fullPage:true});
            console.log(`${mobile?'mobile':'desktop'} extension flow passed`);
            await context.close();
        }
    } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exit(1);});
