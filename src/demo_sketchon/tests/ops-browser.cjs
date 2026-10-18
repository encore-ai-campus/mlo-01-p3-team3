const {chromium}=require('playwright');
const assert=require('node:assert/strict');
const fs=require('node:fs');const path=require('node:path');
const origin=process.env.TEST_BASE_URL||'http://127.0.0.1:8015';
async function main(){
 const browser=await chromium.launch({channel:'msedge',headless:true});
 const context=await browser.newContext({viewport:{width:1440,height:1000}});
 const page=await context.newPage();const errors=[];
 page.on('pageerror',e=>errors.push(e.message));
 page.on('response',r=>{if(r.status()>=500)errors.push(`${r.status()} ${r.url()}`);if(r.url().includes('/static/')&&r.status()>=400)errors.push(`${r.status()} ${r.url()}`);});
 try{
  await page.goto(origin+'/ops/');assert(page.url().includes('/accounts/login/'));
  await page.locator('[name=username]').fill('ops_demo');await page.locator('[name=password]').fill('Ops-test-only-2468!');
  await Promise.all([page.waitForURL('**/ops/'),page.getByRole('button',{name:'로그인',exact:true}).click()]);
  await page.locator('.ops-main h1').waitFor();
  for(const target of ['','people/','projects/','events/','collection/','ads/','ad-reports/','audit/']){
   await page.goto(origin+'/ops/'+target);await page.locator('.ops-main h1').waitFor();
   await page.waitForFunction(()=>!document.body.classList.contains('account-revalidating'));
   assert.equal(await page.locator('.ad-sidebar').count(),0);
   assert(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth+1),target+' desktop overflow');
  }
  const fixture=JSON.parse(fs.readFileSync(path.join(__dirname,'../ops-fixture.json')));
  await page.goto(origin+'/ops/projects/'+fixture.project_id+'/');
  await page.locator('.ops-sketch').waitFor();
  await page.waitForFunction(()=>document.querySelector('.ops-sketch').getContext('2d').getImageData(240,240,1,1).data[3]>0);
  await page.screenshot({path:path.join(__dirname,'../ops-detail.png'),fullPage:true});
  await page.goto(origin+'/ops/ads/new/');
  const id='demo-'+Date.now();
  const values={campaign_id:id,title:'공간에 어울리는 그린',description:'광고 서버 연결 전 초안입니다.',advertiser:'데모 브랜드',landing_url:'https://example.com/plant',bid_units:'500',starts_at:'2026-10-16T00:00',ends_at:'2030-12-31T23:59',reason:'데모 캠페인 등록'};
  for(const [key,value] of Object.entries(values))await page.locator(`[name=${key}]`).fill(value);
  await page.locator('[name=category]').selectOption('plant');await page.locator('[name=slot_id]').selectOption('result-right');
  await page.locator('[name=creative]').setInputFiles(path.join(__dirname,'../sketch/static/sketch/assets/room-empty.png'));
  await page.locator('[name=active]').check();
  await Promise.all([page.waitForURL('**/ops/ads/'),page.getByRole('button',{name:'초안 저장'}).click()]);
  let card=page.locator('.ops-campaign').filter({hasText:id});
  await card.locator('[name=decision]').selectOption('APPROVED');await card.locator('form').filter({has:page.locator('[name=decision]')}).locator('[name=reason]').fill('정책·품목·기간 확인');
  await Promise.all([page.waitForURL('**/ops/ads/'),card.getByRole('button',{name:'심사 저장'}).click()]);
  await page.reload();card=page.locator('.ops-campaign').filter({hasText:id});assert((await card.innerText()).includes('APPROVED'));assert((await card.innerText()).includes('미송출'));
  await card.getByRole('link',{name:'초안 수정'}).click();await page.locator('[name=title]').fill('초안 문구 변경');await page.locator('[name=reason]').fill('소재 문구 수정');
  await Promise.all([page.waitForURL('**/ops/ads/'),page.getByRole('button',{name:'초안 저장'}).click()]);
  await page.reload();card=page.locator('.ops-campaign').filter({hasText:id});assert((await card.innerText()).includes('PENDING'));
  await page.screenshot({path:path.join(__dirname,'../ops-ads.png'),fullPage:true});
  await page.goto(origin+'/ops/');await page.screenshot({path:path.join(__dirname,'../ops-dashboard.png'),fullPage:true});
  await page.setViewportSize({width:390,height:844});
  for(const target of ['','projects/','collection/','ads/','ad-reports/']){
    await page.goto(origin+'/ops/'+target);await page.locator('.ops-main h1').waitFor();
    const overflow=await page.evaluate(()=>({width:innerWidth,scroll:document.documentElement.scrollWidth,wide:[...document.querySelectorAll('body *')].filter(el=>el.getBoundingClientRect().right>innerWidth+1).slice(0,10).map(el=>[el.tagName,el.className,el.getBoundingClientRect().width])}));
    if(overflow.scroll>overflow.width+1){await page.screenshot({path:path.join(__dirname,'../ops-mobile-overflow.png'),fullPage:true});console.log(overflow);}
    assert(overflow.scroll<=overflow.width+1,target+' mobile overflow');
  }
  await page.screenshot({path:path.join(__dirname,'../ops-mobile.png'),fullPage:true});
  await page.goto(origin+'/');await page.getByRole('link',{name:'운영 관리'}).waitFor();
  await page.getByRole('link',{name:'운영 관리'}).click();
  await page.locator('form[action="/accounts/logout/"] button').click();await page.waitForURL(origin+'/');
  await page.goBack();await page.waitForTimeout(400);
  assert.equal(await page.locator('.ops-main').count(),0,'logout back exposed admin content');
  await page.goto(origin+'/accounts/login/?next=/ops/collection/');
  await page.locator('[name=username]').fill('collection_demo');await page.locator('[name=password]').fill('Ops-test-only-2468!');
  await Promise.all([page.waitForURL('**/ops/collection/'),page.getByRole('button',{name:'로그인',exact:true}).click()]);
  await page.waitForFunction(()=>window.__sketchAccountState?.ops_page_access===true);
  assert.equal(new URL(page.url()).pathname,'/ops/collection/');assert.equal(await page.locator('.ops-main h1').innerText(),'스케치 수집');
  assert.equal(await page.locator('.ops-sidebar nav a').count(),1);
  assert.deepEqual(errors,[]);console.log('PASS ops: login/access, 8 menus, private preview, ad draft/review/re-review, no delivery, desktop/mobile, logout/back');
 }finally{await browser.close();}
}
main().catch(e=>{console.error(e);process.exit(1);});
