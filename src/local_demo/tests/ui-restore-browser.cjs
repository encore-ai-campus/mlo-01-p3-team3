// Dev editor submission routing on an isolated server; recommendation admission is mocked.
const assert = require('node:assert/strict');
const {randomUUID} = require('node:crypto');
const {chromium} = require('playwright');
const base = process.env.ROOMPICK_TEST_URL || 'http://127.0.0.1:8015';
(async () => {
  const browser = await chromium.launch({headless:true, channel:'msedge'});
  try {
    for (const [button,mobile] of [['search',false],['search-top',false],['search-mobile',true]]) {
      const context = await browser.newContext({viewport:mobile ? {width:390,height:844} : {width:1440,height:1000},isMobile:mobile,hasTouch:mobile});
      const page = await context.newPage(), errors=[], submissions=[];
      page.on('pageerror',error=>errors.push(error.message));
      await require('./browser-member.cjs')(context,base);
      await page.route('**/interior-recommand/api/runs',async route=>{
        assert.equal(route.request().method(),'POST');
        submissions.push(route.request());
        await route.fulfill({status:202,contentType:'application/json',body:JSON.stringify({run_id:randomUUID(),page_url:'/editor/?direct-submitted=1'})});
      });
      await page.goto(base); await page.locator('#start-sketch').click();
      await page.waitForFunction(()=>Object.keys(sessionStorage).some(key=>key.startsWith('local_demo.history.')) && !document.querySelector('#sample-empty').disabled);
      assert.equal(await page.locator('[data-sticker]').count(),7);
      await page.locator('#sample-empty').click();
      await page.waitForFunction(()=>!document.querySelector('#room-photo').hidden&&!document.querySelector('#search').disabled);
      if (mobile) await page.locator('#tab-description').click();
      await page.locator('#text-requirements').fill('창가에 작은 화분');
      await page.locator('#text-requirements').blur();
      await page.locator('#'+button).click(); await page.waitForURL('**/editor/?direct-submitted=1');
      assert.equal(submissions.length,1);
      assert.deepEqual(errors,[]);
      assert.equal((await context.request.get(base+'/ops/',{maxRedirects:0})).status(),403);
      console.log(button+': direct recommendation routing, sticker controls, and member ops denial passed (mock admission)');
      await context.close();
    }
  } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exit(1);});
