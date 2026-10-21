const assert=require('node:assert/strict'),{chromium}=require('playwright');
const base=process.env.ROOMPICK_TEST_URL||'http://127.0.0.1:8015';
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'});
 try{
  const context=await browser.newContext(),page=await context.newPage(),username='raw'+Date.now(),password='Raw-recovery-2468!';
  let status=await(await context.request.get(base+'/api/account/status/')).json();
  await context.request.post(base+'/accounts/signup/',{headers:{'X-CSRFToken':status.csrf_token},form:{username,nickname:'원본A',password1:password,password2:password}});
  await context.route('**/samples/',route=>route.fulfill({status:503,contentType:'application/json',body:'{"error":"offline"}'}));
  await page.goto(base+'/editor/');await page.locator('#sample-empty').click();await page.waitForFunction(()=>!document.querySelector('#room-photo').hidden&&!document.querySelector('#search').disabled);
  const projectId=new URL(page.url()).pathname.split('/').filter(Boolean).at(-1);
  await page.locator('#text-requirements').fill('A의 화분 스케치');await page.locator('#text-requirements').blur();
  await page.locator('#search').click();await page.waitForURL('**/results/**');
  const rows=async()=>page.evaluate(()=>new Promise((resolve,reject)=>{const open=indexedDB.open('local-demo-sketch-v1',2);open.onsuccess=()=>{const db=open.result,tx=db.transaction(['outbox','failed'],'readonly'),out=tx.objectStore('outbox').getAll(),failed=tx.objectStore('failed').getAll();tx.oncomplete=()=>{resolve({outbox:out.result,failed:failed.result});db.close()};};open.onerror=()=>reject(open.error)}));
  let old;for(let i=0;i<30;i++){old=(await rows()).outbox.filter(row=>row.project_id===projectId);if(old.length)break;await page.waitForTimeout(100)}
  assert.ok(old.length,'A Raw retained offline');
  await context.unroute('**/samples/');status=await(await context.request.get(base+'/api/account/status/')).json();
  await context.request.post(base+'/accounts/logout/',{headers:{'X-CSRFToken':status.csrf_token}});
  await require('./browser-member.cjs')(context,base);
  await page.goto(base+'/editor/');await page.waitForTimeout(1200);await page.evaluate(()=>window.dispatchEvent(new Event('online')));await page.waitForTimeout(400);
  const underB=await rows(),ids=new Set(old.map(row=>row.sample_id));
  assert.equal(underB.failed.filter(row=>ids.has(row.sample_id)).length,0,'account ownership rejection must not permanently quarantine A Raw');
  assert.equal(underB.outbox.filter(row=>ids.has(row.sample_id)).length,old.length);
  status=await(await context.request.get(base+'/api/account/status/')).json();await context.request.post(base+'/accounts/logout/',{headers:{'X-CSRFToken':status.csrf_token}});
  status=await(await context.request.get(base+'/api/account/status/')).json();await context.request.post(base+'/accounts/login/',{headers:{'X-CSRFToken':status.csrf_token},form:{username,password}});
  await page.goto(base+'/editor/');
  let remaining=old.length;for(let i=0;i<50;i++){remaining=(await rows()).outbox.filter(row=>ids.has(row.sample_id)).length;if(!remaining)break;await page.waitForTimeout(100)}
  assert.equal(remaining,0,'A Raw delivered after A returns');
  console.log('membership Raw recovery PASS: A offline outbox survives B login without quarantine and delivers when A returns');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1});
