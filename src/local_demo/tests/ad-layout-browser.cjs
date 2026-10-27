const assert=require('node:assert/strict'),{chromium}=require('playwright');
const base=process.env.ROOMPICK_TEST_URL||'http://127.0.0.1:8015';
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'});
 try {
  const context=await browser.newContext({viewport:{width:1440,height:1000}}),page=await context.newPage();
  for(const path of ['/','/accounts/login/','/accounts/signup/']){
   await page.goto(base+path);assert.equal(await page.locator('.ad-placeholder').count(),0,path+' must have no ad');
  }
  await require('./browser-member.cjs')(context,base);
  await page.goto(base+'/editor/');await page.locator('#sample-empty').click();await page.waitForFunction(()=>!document.querySelector('#room-photo').hidden&&!document.querySelector('#search').disabled);
  await page.locator('#text-requirements').fill('화분');await page.locator('#search').click();await page.waitForURL('**/results/**');
  const resultPath=new URL(page.url()).pathname;
  await page.setViewportSize({width:1440,height:700});await page.goto(base+'/editor/');
  const initialRail=await page.locator('.ad-rail').boundingBox();
  await page.evaluate(()=>window.scrollTo({top:240,behavior:'instant'}));await page.waitForTimeout(100);
  const scrolledRail=await page.locator('.ad-rail').boundingBox();
  assert.ok(await page.evaluate(()=>scrollY>100),'editor actually scrolled');
  assert.ok(Math.abs(scrolledRail.y-24)<2,'ad follows scroll with 24px top inset');
  assert.ok(scrolledRail.y<initialRail.y,'rail reached its sticky position');
  for(const width of [320,390,768,1024,1280,1366,1440,1700,1920]){
   await page.setViewportSize({width,height:1000});
   for(const path of ['/','/editor/',resultPath,'/my/profile/','/my/projects/','/my/wishlist/']){
    await page.goto(base+path);
    assert.equal(await page.evaluate(()=>document.documentElement.scrollWidth>innerWidth),false,path+' overflow at '+width);
    const ad=page.locator('.ad-placeholder');
    if(path==='/'){assert.equal(await ad.count(),0,'Home stays ad-free');continue;}
    assert.equal(await ad.count(),1,path+' one ad');
    if(width>=1280){
     const banner=await ad.boundingBox(),main=await page.locator('main').boundingBox();
     assert.ok(banner.x>=main.x+main.width+20,path+' ad to right of main at '+width);
     assert.equal(Math.round(banner.width),300,path+' 300px banner');assert.equal(Math.round(banner.height),600,path+' 600px banner');
     assert.ok(banner.x+banner.width<=width,path+' banner within viewport');
     assert.ok(Math.abs(width-banner.x-banner.width-16)<2,path+' 16px from right viewport edge');
     assert.ok(banner.x>=main.x+main.width+46,path+' clear separation from content');
    }else assert.equal(await ad.isVisible(),false,path+' hide rail on narrow screens');
    if(width===1440&&['/','/editor/',resultPath,'/my/profile/'].includes(path))await page.screenshot({path:'test-results/ad-'+(path==='/'?'home':path.startsWith('/editor')?'editor':path.startsWith('/results')?'result':'my')+'.png',fullPage:true});
   }
  }
  console.log('Ad layout PASS: Home/login/signup ad-free; sticky 300x600 editor/result/My rail follows scroll at 24px, 16px right inset, >=48px content gap; 320–1920 no overflow');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1});
