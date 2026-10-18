// Existing composition entry points, on an isolated server; no direct-engine calls.
const assert = require('node:assert/strict');
const {chromium} = require('playwright');
const base = process.env.ROOMPICK_TEST_URL || 'http://127.0.0.1:8015';
(async () => {
  const browser = await chromium.launch({headless:true, channel:'msedge'});
  try {
    for (const [button,mobile] of [['search',false],['search-top',false],['search-mobile',true]]) {
      const context = await browser.newContext({viewport:mobile ? {width:390,height:844} : {width:1440,height:1000},isMobile:mobile,hasTouch:mobile});
      const page = await context.newPage(), errors=[], direct=[];
      page.on('pageerror',error=>errors.push(error.message));
      page.on('request',request=>{if(request.url().includes('/interior-recommand/'))direct.push(request.url());});
      await require('./browser-member.cjs')(context,base);
      await page.goto(base); await page.locator('#start-sketch').click();
      await page.waitForFunction(()=>!document.querySelector('#sample-empty').disabled);
      await page.locator('#sample-empty').click();
      await page.waitForFunction(()=>!document.querySelector('#room-photo').hidden&&!document.querySelector('#search').disabled);
      if (mobile) await page.locator('#tab-description').click();
      await page.locator('#text-requirements').fill('창가에 작은 화분');
      await page.locator('#text-requirements').blur();
      await page.locator('#'+button).click(); await page.waitForURL('**/results/**');
      for (const id of ['download-png','undo-placement','reset-placement','redraw','edit-sketch','new-search','recommend'])
        assert.equal(await page.locator('#'+id).count(),1,id);
      await page.locator('.placement').first().click(); await page.locator('#recommend').click();
      await page.waitForFunction(()=>document.querySelectorAll('[data-product-slot] .product-actions').length===3);
      assert.deepEqual(direct,[]); assert.deepEqual(errors,[]);
      console.log(button+': existing result and three sample product cards passed');
      await context.close();
    }
  } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exit(1);});
