const assert=require('node:assert/strict');
const {chromium}=require('playwright');
const base=process.env.ROOMPICK_TEST_URL||'http://127.0.0.1:8015';
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'});
 try {
  const context=await browser.newContext({viewport:{width:1366,height:900}}),page=await context.newPage(),errors=[];
  let signupNotices=0;
  page.on('dialog',async dialog=>{assert.equal(dialog.type(),'alert');assert.equal(dialog.message(),'가입이 완료되었습니다!');signupNotices++;await dialog.accept();});
  page.on('pageerror',error=>errors.push(error.message));
  await page.goto(base+'/editor/');await page.locator('#sample-empty').click();await page.waitForFunction(()=>!!document.querySelector('#room-photo').getAttribute('src'));
  await page.locator('#text-requirements').fill('창가에 화분 하나를 놓아주세요');
  await page.locator('.account-login').click();await page.waitForURL('**/accounts/login/**');
  const next=new URL(page.url()).searchParams.get('next');
  const username='browser'+Date.now();
  await page.goto(base+'/accounts/signup/?next='+encodeURIComponent(next));
  await page.locator('[name=username]').fill(username);await page.locator('[name=nickname]').fill('브라우저회원');
  await page.locator('[name=password1]').fill('Browser-user-2468!');await page.locator('[name=password2]').fill('Browser-user-2468!');
  await page.getByRole('button',{name:'가입하고 시작하기'}).click();await page.waitForURL('**/editor/**');
  assert.equal(signupNotices,1);
  assert.equal((await(await context.request.get(base+'/api/account/status/')).json()).authenticated,true);
  await page.reload();assert.equal(signupNotices,1,'signup success notice does not repeat on refresh');
  assert.equal(await page.locator('#text-requirements').inputValue(),'창가에 화분 하나를 놓아주세요');
  await page.waitForFunction(()=>!document.querySelector('#search').disabled);await page.locator('#search').click();await page.waitForURL('**/results/**');
  await page.locator('#save-project').click();await page.locator('#saved-project-title').fill('브라우저 거실');await page.locator('#save-project-form button[type=submit]').click();
  await page.locator('#project-save-status a').waitFor();
  await page.locator('#scene-objects button').first().click();await page.locator('#recommend').click();await page.locator('#product-grid [data-member-only]').first().waitFor();
  await page.locator('#product-grid [data-member-only]').first().click();await page.waitForFunction(()=>document.querySelector('#product-grid [data-member-only]').textContent.includes('해제'));
  const resultUrl=page.url();await page.locator('#project-save-status a').click();await page.locator('[data-edit-version]').first().click();await page.waitForURL('**/editor/**');
  assert.equal(await page.locator('#text-requirements').inputValue(),'창가에 화분 하나를 놓아주세요');
  await page.locator('#sample-empty').click();await page.waitForFunction(()=>!document.querySelector('#search').disabled);
  await page.locator('#search').click();await page.waitForURL('**/results/**');
  await page.locator('#save-project').click();await page.locator('#save-project-form button[type=submit]').click();
  await page.locator('#project-save-status a').waitFor();assert.match(await page.locator('#project-save-status').innerText(),/버전 2/);
  await page.goto(base+'/my/wishlist/');assert.equal(await page.locator('[data-remove-wishlist]').count(),1);
  await page.goto(base+'/my/projects/');await page.locator('.compare-form button').click();await page.waitForURL('**/my/compare/**');assert.equal(await page.locator('.saved-compare img').count(),2);
  for(const width of [320,390,768,1366,1700]) {
   for(const route of ['/','/my/projects/','/my/profile/',new URL(resultUrl).pathname]) {
    await page.setViewportSize({width,height:900});await page.goto(base+route);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false,route+' overflow '+width);
    const ad=page.locator('.ad-placeholder');if(route==='/'){assert.equal(await ad.count(),0,'Home ad-free');continue;}assert.equal(await ad.count(),1,route+' ad');
    if(width>=1280){const bounds=await ad.boundingBox();assert.equal(Math.round(bounds.height),600,route+' ad rail height');assert.equal(Math.round(bounds.width),300,route+' ad rail width');}else assert.equal(await ad.isVisible(),false,route+' narrow ad rail hidden');
   }
  }
  await page.goto(resultUrl);await page.locator('.account-member button').click();await page.waitForURL(base+'/');
  await page.goBack();await page.waitForTimeout(400);
  assert.equal(await page.locator('#save-project:visible').count(),0);
  const protectedResult=await context.request.get(resultUrl);assert.equal(protectedResult.status(),404);
  await page.goto(base+'/editor/');assert.equal(await page.locator('.account-login').isVisible(),true);
  assert.deepEqual(errors,[]);await page.screenshot({path:'test-results/membership-editor-mobile.png',fullPage:true});
  console.log('membership browser: signup, guest input claim, save, wishlist, restore + photo replacement -> version 2, compare, responsive 320–1700, logout/back PASS');
 } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1});
