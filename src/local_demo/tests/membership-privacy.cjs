const assert=require('node:assert/strict'),{chromium}=require('playwright');
const base=process.env.ROOMPICK_TEST_URL||'http://127.0.0.1:8015';
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'});
 try{
  const context=await browser.newContext({viewport:{width:1366,height:900}});
  await require('./browser-member.cjs')(context,base);
  const page=await context.newPage();await page.goto(base+'/editor/');await page.locator('#sample-empty').click();await page.waitForFunction(()=>!document.querySelector('#search').disabled && !document.querySelector('#room-photo').hidden);
  await page.locator('#text-requirements').fill('화분');await page.locator('#search').click();await page.waitForURL('**/results/**');
  const original=JSON.parse(await page.locator('#result-data').textContent());
  await page.screenshot({path:'test-results/membership-result-desktop.png',fullPage:true});
  await page.locator('#save-project').click();await page.locator('#saved-project-title').fill('A의 비공개 프로젝트 제목');
  await page.locator('#cancel-project-save').click();
  let release,started;const gate=new Promise(resolve=>release=resolve),ready=new Promise(resolve=>started=resolve);
  await page.route('**/recommendations/',async route=>{const response=await route.fetch();started();await gate;await route.fulfill({response});});
  await page.locator('#scene-objects button').first().click();await page.locator('#recommend').click();await ready;
  await page.locator('#save-project').click();
  const status=await(await context.request.get(base+'/api/account/status/')).json();
  await context.request.post(base+'/accounts/logout/',{headers:{'X-CSRFToken':status.csrf_token}});
  await require('./browser-member.cjs')(context,base);
  await page.evaluate(()=>{document.dispatchEvent(new Event('visibilitychange'));});
  await page.waitForFunction(()=>document.body.classList.contains('account-inaccessible'));
  release();await page.waitForTimeout(400);
  assert.equal(await page.locator('#product-grid img:visible').count(),0);
  assert.equal(await page.locator('#result-stage:visible').count(),0);
  assert.equal(await page.locator('#save-project-dialog:visible').count(),0);
  assert.equal((await context.request.get(base+original.photo_url)).status(),404);
  assert.equal((await context.request.get(base+original.image_url)).status(),404);
  await page.goto(base+'/my/projects/');await page.screenshot({path:'test-results/membership-projects-desktop.png',fullPage:true});
  console.log('membership privacy PASS: A logout -> B login in same cookie jar; old result concealed, late recommendation not exposed; A photo/result APIs deny B');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1});
