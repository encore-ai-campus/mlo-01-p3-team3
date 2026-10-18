// Run against an isolated INTERIOR_RECOMMENDER_DEMO=1 server. No paid calls.
const assert = require('node:assert/strict');
const {chromium} = require('playwright');
const base = process.env.ROOMPICK_TEST_URL || 'http://127.0.0.1:8016';
const prefix = '/interior-recommand/';

(async () => {
  const browser = await chromium.launch({headless:true, channel:'msedge'});
  try {
    const context = await browser.newContext({viewport:{width:1440,height:1000}});
    const page = await context.newPage(), errors=[];
    page.on('pageerror', error => errors.push(error.message));
    const mode = await (await context.request.get(base+prefix+'api/csrf')).json();
    assert.equal(mode.demo,true,'browser check requires offline mode before submission');
    await page.goto(base+prefix);
    assert.ok(new URL(page.url()).pathname.startsWith('/editor/'),'no run opens the existing SketchOn editor');
    await page.waitForFunction(() => !document.querySelector('#sample-empty').disabled);
    assert.equal(await page.locator('#search').evaluate(el => getComputedStyle(el).backgroundColor),'rgb(36, 79, 66)');
    await page.locator('#sample-empty').click();
    await page.waitForFunction(() => !document.querySelector('#room-photo').hidden && !document.querySelector('#search').disabled);
    await page.getByRole('button',{name:'오피스 의자 사진 붙이기',exact:true}).click();
    await page.waitForFunction(() => !document.querySelector('#search').disabled);
    assert.match(await page.locator('#counts').innerText(),/표시 1개/);
    await page.locator('#text-requirements').fill('창가에 놓을 원목 의자를 찾아줘');
    await page.locator('#surface').scrollIntoViewIfNeeded();
    const box = await page.locator('#surface').boundingBox();
    await page.mouse.move(box.x+box.width*.25,box.y+box.height*.4);
    await page.mouse.down();
    await page.mouse.move(box.x+box.width*.5,box.y+box.height*.6,{steps:5});
    await page.mouse.up();
    await page.locator('#search').click();
    await page.waitForURL('**/interior-recommand/?run=*');
    await page.waitForFunction(() => document.querySelectorAll('#items .candidate').length > 0);
    assert.match(await page.title(),/SketchOn/);
    assert.doesNotMatch(await page.locator('body').innerText(),/RoomPick/);
    assert.equal(await page.locator('main input[type=file], main textarea, #request-form').count(),0,'results must not ask for the same inputs again');
    assert.match(await page.locator('#requested-text').innerText(),/창가에 놓을 원목 의자를 찾아줘/);
    const run = new URL(page.url()).searchParams.get('run');
    const state = await (await context.request.get(base+prefix+'api/runs/'+run)).json();
    assert.ok(state.sketch_image);
    assert.ok(state.sketch_strokes.length > 0);
    const project = await (await context.request.get(base+'/api/projects/'+state.external_project.id+'/')).json();
    assert.equal(project.document.strokes.length,state.sketch_strokes.length,'editor input persisted for editing');
    assert.equal(project.request_context.text_requirements,'창가에 놓을 원목 의자를 찾아줘');
    await page.locator('#room').evaluate(el=>el.decode());
    await page.locator('#sketch-overlay').evaluate(el=>el.decode());
    const image = page.locator('#items .candidate img').first();
    await image.scrollIntoViewIfNeeded(); await image.evaluate(el=>el.decode());
    const href = await page.locator('#items .candidate a').first().getAttribute('href');
    assert.ok(href.startsWith(prefix+'api/runs/'));
    const link = await context.request.get(base+href,{maxRedirects:0});
    assert.equal(link.status(),302);
    assert.ok(link.headers().location.startsWith('https://prod.danawa.com/info/'));
    for (const width of [320,390,768,1440]) {
      await page.setViewportSize({width,height:1000});
      assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),`result overflow at ${width}`);
    }
    if (process.env.ROOMPICK_TEST_SCREENSHOT) await page.screenshot({path:process.env.ROOMPICK_TEST_SCREENSHOT,fullPage:true});
    const edit = await page.locator('#edit-input').getAttribute('href');
    for (const [width,button] of [[1440,'#search-top'],[390,'#search-mobile']]) {
      await page.setViewportSize({width,height:1000});
      await page.goto(base+edit);
      await page.waitForFunction(()=>!document.querySelector('#search').disabled);
      assert.equal(await page.locator('#text-requirements').inputValue(),'창가에 놓을 원목 의자를 찾아줘');
      assert.match(await page.locator('#counts').innerText(),/1획/);
      assert.match(await page.locator('#counts').innerText(),/표시 1개/);
      await page.locator(button).click();
      await page.waitForURL('**/interior-recommand/?run=*');
      await page.waitForFunction(()=>document.querySelectorAll('#items .candidate').length>0);
      assert.equal(await page.locator('#request-form').count(),0);
    }
    await page.goto(base+edit);
    await page.waitForFunction(()=>!document.querySelector('#search').disabled);
    assert.equal(await page.locator('#preview-mock').count(),0);
    assert.equal(await page.locator('[data-sticker]').count(),7);
    assert.deepEqual(errors,[]);
    console.log('PASS: existing editor → primary/top/mobile submit → read-only input preview and results; edit restores photo/sketch/text; 320–1440px, assets and links.');
  } finally { await browser.close(); }
})().catch(error=>{console.error(error);process.exitCode=1;});
