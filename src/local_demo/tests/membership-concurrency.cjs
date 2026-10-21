const assert=require('node:assert/strict'),{randomUUID}=require('node:crypto'),fs=require('node:fs');
const {chromium}=require('playwright');
const base=process.env.ROOMPICK_TEST_URL||'http://127.0.0.1:8015';
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'});
 try{
  const context=await browser.newContext();
  let status=await(await context.request.get(base+'/api/account/status/')).json();
  const headers={'X-CSRFToken':status.csrf_token};
  const bytes=fs.readFileSync('sketch/static/sketch/assets/room-hero.png');
  const project=async()=>await(await context.request.post(base+'/api/projects/',{headers,multipart:{title:'동시성',photo:{name:'test.png',mimeType:'image/png',buffer:bytes}}})).json();
  const projects=await Promise.all(Array.from({length:8},project));
  const bodies=projects.map(()=>({request_id:randomUUID(),revision:0,document:{strokes:[],items:[]},text_requirements:'화분'}));
  const responses=await Promise.all(projects.map((p,i)=>context.request.post(base+`/api/projects/${p.id}/search/`,{headers,data:bodies[i]})));
  assert.equal(responses.filter(r=>r.status()===201).length,3);
  assert.equal(responses.filter(r=>r.status()===429).length,5);
  const index=responses.findIndex(r=>r.status()===201);
  const replay=await context.request.post(base+`/api/projects/${projects[index].id}/search/`,{headers,data:bodies[index]});assert.equal(replay.status(),200);
  assert.equal((await(await context.request.get(base+'/api/account/status/')).json()).quota.used,3);
  await require('./browser-member.cjs')(context,base);
  status=await(await context.request.get(base+'/api/account/status/')).json();headers['X-CSRFToken']=status.csrf_token;
  const p=await project(),body={request_id:randomUUID(),revision:0,document:{strokes:[],items:[]},text_requirements:'소파'};
  const generated=await Promise.all(Array.from({length:8},()=>context.request.post(base+`/api/projects/${p.id}/search/`,{headers,data:body})));
  assert.equal(generated.filter(r=>r.status()===201).length,1);assert.equal(generated.filter(r=>r.status()===200).length,7);
  const result=await generated[0].json(),saveBody={request_id:randomUUID(),result_version:0,title:'경합 저장'};
  const saved=await Promise.all(Array.from({length:8},()=>context.request.post(base+`/api/results/${result.result_id}/save/`,{headers,data:saveBody})));
  assert.equal(saved.filter(r=>r.status()===201).length,1);assert.equal(saved.filter(r=>r.status()===200).length,7);
  const versions=await Promise.all(saved.map(r=>r.json()));assert.equal(new Set(versions.map(v=>v.version_id)).size,1);
  console.log('membership concurrency PASS: 8 guest requests -> 3 successes/5 quota denials; replay no charge; 8 identical generation requests -> 1 creation; 8 identical saves -> 1 version');
 }finally{await browser.close();}
})().catch(error=>{console.error(error);process.exitCode=1});
