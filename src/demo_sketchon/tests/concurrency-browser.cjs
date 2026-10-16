// Small real SQLite contention regression, not a production capacity benchmark.
const assert=require('node:assert/strict');
const {randomUUID}=require('node:crypto');
const {chromium}=require('playwright');
const base=process.env.ROOMPICK_TEST_URL || 'http://127.0.0.1:8015';
(async()=>{
    const browser=await chromium.launch({headless:true,channel:'msedge'});
    try {
        const context=await browser.newContext(), page=await context.newPage();
        await page.goto(base+'/editor/');
        const csrf=await page.locator('[name=csrfmiddlewaretoken]').inputValue();
        const project=JSON.parse(await page.locator('#initial-data').textContent());
        const headers={'X-CSRFToken':csrf};
        const events=await Promise.all(Array.from({length:16},(_,i)=>context.request.post(base+`/api/projects/${project.id}/events/`,{
            headers,data:{events:[{event_id:randomUUID(),event_type:'button.clicked',payload:{button:'parallel-'+i}}]},
        })));
        assert.deepEqual(events.map(r=>r.status()),Array(16).fill(200));
        await page.locator('#sample-empty').click(); await page.waitForFunction(()=>!document.querySelector('#room-photo').hidden&&!document.querySelector('#search').disabled);
        const id=page.url().split('/').filter(Boolean).at(-1);
        const generated=await context.request.post(base+`/api/projects/${id}/scene-generations/`,{
            headers,data:{request_id:randomUUID(),revision:0,document:{strokes:[],items:[]},text_requirements:'소파'},
        });
        assert.equal(generated.status(),201); const scene=await generated.json();
        const requestId=randomUUID(), path=base+`/api/scene-objects/${scene.scene_objects[0].id}/recommendations/`;
        const runs=await Promise.all(Array.from({length:8},()=>context.request.post(path,{headers,data:{scene_generation_id:scene.result_id,request_id:requestId}})));
        assert.ok(runs.every(r=>[200,201,202].includes(r.status())));
        const replies=await Promise.all(runs.map(r=>r.json())); assert.ok(replies.every(r=>r.recommendation_run_id===requestId));
        const succeeded=await context.request.get(base+`/api/jobs/${requestId}/`); assert.equal((await succeeded.json()).status,'SUCCEEDED');
        const rows=scene.placements.map(({id,x,y,width,height})=>({id,x,y,width,height}));
        rows[0].x=0;
        const saves=await Promise.all(Array.from({length:8},()=>context.request.post(base+`/api/results/${scene.result_id}/placements/`,{headers,data:{version:0,placements:rows}})));
        assert.equal(saves.filter(r=>r.status()===200).length,1); assert.equal(saves.filter(r=>r.status()===409).length,7);
        console.log('Concurrency regression passed: 16 event writes, 8 same-ID recommendation requests, 8 competing placement saves; no 500 responses.');
    } finally {await browser.close();}
})().catch(error=>{console.error(error);process.exit(1);});
