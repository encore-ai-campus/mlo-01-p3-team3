const assert=require('node:assert/strict');
const {chromium}=require('playwright');
const fs=require('node:fs');
const base=process.env.ROOMPICK_TEST_URL||'http://127.0.0.1:8015';
(async()=>{
    fs.mkdirSync('test-results/ui',{recursive:true});
    const browser=await chromium.launch({headless:true,channel:'msedge'});
    try {
        for(const [name,width,height] of [['desktop',1366,768],['mobile',390,844]]) {
            const context=await browser.newContext({viewport:{width,height},isMobile:width<600,hasTouch:width<600});
            const page=await context.newPage(), errors=[]; page.on('pageerror',e=>errors.push(e.message));
            await require('./browser-member.cjs')(context,base);
        await page.goto(base); await page.locator('.hero-photo img').evaluate(image=>image.decode());
            const overflow=()=>page.evaluate(()=>document.documentElement.scrollWidth>innerWidth);
            const brand=async()=>{
                assert.match(await page.title(),/스케치·온 SketchOn/);
                const logo=page.locator('.header .brand-logo');
                assert.equal(await page.locator('.brand-name,.brand-english,.brand-mark').count(),0);
                assert.equal(await page.locator('.header .brand').getAttribute('aria-label'),'스케치·온 SketchOn 홈');
                assert.equal(await logo.getAttribute('alt'),'Sketch · On');
                await logo.evaluate(image=>image.decode());
                assert.equal(await logo.evaluate(image=>image.naturalWidth),2172);
                assert.ok((await logo.getAttribute('src')).endsWith('sketchon-logo-approved.png'));
                assert.ok((await page.locator('link[rel="icon"]').getAttribute('href')).endsWith('sketchon-pencil.svg'));
                const logoBox=await page.locator('.header .brand-viewport').boundingBox();
                assert.ok(Math.abs(logoBox.width/logoBox.height-1780/375)<.02,'approved artwork keeps its proportions');
                for(const testWidth of [320,601,768]) {
                    await page.setViewportSize({width:testWidth,height});
                    const headerFits=await page.locator('.header').evaluate(header=>{
                        const boxes=[...header.children].map(node=>node.getBoundingClientRect()).filter(box=>box.width&&box.height);
                        return boxes.every((box,index)=>box.left>=0&&box.right<=innerWidth&&(!index||boxes[index-1].right<=box.left+1));
                    });
                    assert.equal(headerFits,true,'logo and header controls fit at '+testWidth+'px');
                }
                await page.setViewportSize({width,height});
                assert.equal(await page.locator('body').innerText().then(text=>text.includes('RoomPick')),false);
            };
            await brand();
            assert.equal(await overflow(),false);
            await page.screenshot({path:`test-results/ui/${name}-home.png`,fullPage:true});
            await page.locator('#start-sketch').click(); await page.waitForFunction(()=>!document.querySelector('#sample-empty').disabled);
            await brand();
            const choose=await page.locator('#sample-empty').boundingBox(), initialStage=await page.locator('#stage').boundingBox(); assert.ok(choose.y>=initialStage.y&&choose.y+choose.height<=initialStage.y+initialStage.height,'initial photo action stays inside the full-width photo area');
            const canvas=await page.locator('#editor-board').boundingBox(), panel=await page.locator('#idea-panel').boundingBox();
            if(width>900) {assert.ok(panel.x>=canvas.x+canvas.width,'description beside the canvas'); assert.ok(Math.abs(panel.y-canvas.y)<2);}
            else {assert.equal(await page.locator('#idea-panel').isVisible(),false);}
            await page.locator('#sample-empty').click(); await page.waitForFunction(()=>!document.querySelector('#room-photo').hidden&&!document.querySelector('#search').disabled);
            for(const id of ['pen','eraser','fill','text','box-select','rectangle','ellipse','line','arrow','group-selection','resize-selection']) assert.equal(await page.locator('#'+id).isVisible(),true);
            if(width<600) await page.locator('#tab-description').click();
            await page.locator('#text-requirements').fill('창가에 소파를 놓아주세요'); await page.locator('[data-preference="세이지 그린"]').click();
            assert.equal(await overflow(),false);
            await page.screenshot({path:`test-results/ui/${name}-editor.png`,fullPage:true});
            await page.locator(width<600?'#search-mobile':'#search').click(); await page.waitForURL('**/results/**');
            const placement=page.locator('.placement').first(); await placement.click(); await page.locator('#recommend').click();
            await page.waitForFunction(()=>document.querySelectorAll('[data-product-slot] .product-actions').length===3);
            await brand();
            for(const image of await page.locator('[data-product-slot] img').all()) {await image.scrollIntoViewIfNeeded(); await image.evaluate(image=>image.decode());}
            assert.equal(await overflow(),false); assert.equal(await page.locator('[data-product-slot]').count(),3);
            await page.screenshot({path:`test-results/ui/${name}-result.png`,fullPage:true});
            assert.deepEqual(errors,[]); console.log(name+' modern UI layout passed'); await context.close();
        }
    } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exit(1);});
