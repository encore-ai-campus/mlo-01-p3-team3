const assert=require('node:assert/strict');
const {chromium}=require('playwright');
const fs=require('node:fs');
const base=process.env.ROOMPICK_TEST_URL||'http://127.0.0.1:8015';
(async()=>{
 const browser=await chromium.launch({headless:true,channel:'msedge'});
 try {
  fs.mkdirSync('test-results/revision7',{recursive:true});
  for(const [name,width,height] of [['desktop',1366,900],['small-desktop',1100,900],['wide-desktop',1700,1000],['mobile',390,844]]) {
   const mobile=width<600;
   const context=await browser.newContext({viewport:{width,height},hasTouch:mobile,isMobile:mobile});
   const page=await context.newPage(), errors=[]; page.on('pageerror',e=>errors.push(e.message));
   await page.goto(base+'/editor/'); await page.waitForFunction(()=>!document.querySelector('#sample-empty').disabled);
   assert.equal(await page.locator('#select').count(),0);
   assert.equal(await page.getByRole('button',{name:'＋ 사진 선택',exact:true}).count(),1);
   assert.equal(await page.locator('#upload,#sample-room').count(),0);
   await page.locator('#sample-empty').click(); await page.waitForFunction(()=>!document.querySelector('#room-photo').hidden&&!document.querySelector('#search').disabled);
   assert.equal(await page.locator('#sample-empty').isVisible(),true,'same photo buttons remain available after upload');
   const stageWidth=await page.locator('#stage').evaluate(el=>el.getBoundingClientRect().width);
   const available=await page.locator('#drop-zone').evaluate(el=>el.clientWidth-parseFloat(getComputedStyle(el).paddingLeft)-parseFloat(getComputedStyle(el).paddingRight));
   assert.ok(Math.abs(stageWidth-available)<2,'photo fills the board width');
   const editorDisplay=await page.locator('#stage').evaluate(el=>[el.getBoundingClientRect().width,el.getBoundingClientRect().height]);
   for(const [id,color,size,font] of [['eraser',false,true,false],['fill',true,false,false],['text',true,false,true],['box-select',false,false,false],['pen',true,true,false]]) {
    await page.locator('#'+id).click(); assert.equal(await page.locator('#color').isVisible(),color,id+' color'); assert.equal(await page.locator('#thickness').isVisible(),size,id+' width'); assert.equal(await page.locator('#font-size').isVisible(),font,id+' font');
   }
   async function click(x,y) {await page.locator('#surface').scrollIntoViewIfNeeded(); const b=await page.locator('#surface').boundingBox(); await page.mouse.click(b.x+b.width*x,b.y+b.height*y);}
   await page.locator('#text').click(); await click(.2,.3); await page.locator('#inline-text').fill('화분'); await page.locator('#inline-text').press('Enter');
   await page.locator('#box-select').click(); await click(.22,.32);
   assert.equal(await page.locator('#color').isVisible(),true,'selected text has contextual color control');
   assert.equal(await page.locator('#font-size').isVisible(),false,'font sizing is shown while entering/editing text, not passive selection');
   await page.locator('#color').fill('#ee3344');
   const hasRed=()=>page.locator('#ink').evaluate(canvas=>{const p=canvas.getContext('2d').getImageData(0,0,canvas.width,canvas.height).data;for(let i=0;i<p.length;i+=4)if(p[i]===238&&p[i+1]===51&&p[i+2]===68&&p[i+3]===255)return true;return false;});
   assert.equal(await hasRed(),true);await page.locator('#undo').click();await page.waitForFunction(()=>!document.querySelector('#redo').disabled);assert.equal(await hasRed(),false);
   await page.locator('#redo').click();await page.waitForFunction(()=>!document.querySelector('#undo').disabled);assert.equal(await hasRed(),true);
   await page.locator('#box-select').click();await click(.22,.32);await page.locator('#fill').click();assert.equal(await page.locator('#font-size').isVisible(),false,'fill must not retain a selected text font control');
   if(mobile) await page.locator('#tab-description').click();
   await page.locator('#text-requirements').fill('작은 화분'); await page.locator(mobile?'#search-mobile':'#search').click(); await page.waitForURL('**/results/**');
   assert.match(await page.locator('h1').textContent(),/님이 원하시는.*화분.*을 생성하였습니다/);
   assert.equal(await page.locator('#resize-selection,#search-region,.region-numbers,#download-json').count(),0);
   assert.equal(await page.locator('#region-width,#region-height').count(),2);
   assert.equal(await page.locator('#download-png').textContent(),'이미지 내려받기');
   assert.equal(await page.locator('.board-result-actions #download-png,.board-result-actions [data-restart]').count(),4);
   const result=JSON.parse(await page.locator('#result-data').textContent());
   const data=await context.request.get(base+result.data_url).then(r=>r.json());
   assert.equal(data.input_document.items[0].color,'#ee3344');
   const dimensions=await page.locator('#result-background').evaluate(async image=>{await image.decode(); return [image.naturalWidth,image.naturalHeight];});
   const resultSize=await page.evaluate(async url=>{const image=new Image();image.src=url;await image.decode();return [image.naturalWidth,image.naturalHeight];},result.image_url);
   assert.deepEqual(dimensions,resultSize,'display photo and composition pixels match');
   const resultDisplay=await page.locator('#result-stage').evaluate(el=>[el.getBoundingClientRect().width,el.getBoundingClientRect().height]);
   assert.ok(resultDisplay.every((value,i)=>Math.abs(value-editorDisplay[i])<2),'editor and result have the same displayed photo dimensions');
   const apply=page.locator('#save-object'); await page.locator('#object-correction > summary').click();
   await page.waitForFunction(()=>document.querySelectorAll('.placement.selected').length===1);
   assert.equal(await page.locator('.placement.selected').count(),1,'correction selects the generated furniture');
   const previousWidth=Number(await page.locator('#region-width').inputValue());
   const resized=page.waitForResponse(r=>r.url().endsWith('/placements/'));
   await page.locator('#region-width').fill(String(previousWidth>30?previousWidth-5:previousWidth+5)); await page.locator('#region-width').press('Tab');
   assert.equal((await resized).status(),200);
   await page.waitForFunction(()=>!document.querySelector('#download-png').disabled);
   assert.equal(await apply.isDisabled(),true); await page.locator('#object-category').selectOption('desk'); assert.equal(await apply.isEnabled(),true);
   await page.locator('#object-category').selectOption('plant'); assert.equal(await apply.isDisabled(),true);
   await page.locator('#object-category').selectOption('desk');
   const b=await page.locator('.placement.selected').boundingBox(); await page.locator('.placement.selected').scrollIntoViewIfNeeded(); const p=await page.locator('.placement.selected').boundingBox();
   const saved=page.waitForResponse(r=>r.url().endsWith('/placements/'));
   await page.mouse.move(p.x+p.width*.4,p.y+p.height*.4);await page.mouse.down();await page.mouse.move(p.x+p.width*.4+15,p.y+p.height*.4+10,{steps:5});await page.mouse.up();assert.equal((await saved).status(),200);
   await page.waitForFunction(()=>!document.querySelector('#save-object').disabled);
   assert.equal(await page.locator('#object-category').inputValue(),'desk','placement movement preserves pending category');
   const correction=page.waitForResponse(r=>r.url().endsWith('/revisions/'));await apply.click();assert.equal((await correction).status(),201);
   await page.waitForFunction(()=>document.querySelector('#save-object').disabled);
   await page.screenshot({path:`test-results/revision7/${name}-result.png`,fullPage:true});
   const dl=page.waitForEvent('download'); await page.locator('#download-png').click(); assert.equal((await dl).suggestedFilename(),'sketchon-result.png');
   assert.deepEqual(errors,[]);console.log(name+' revision7 flow passed');await context.close();
  }
 } finally {await browser.close();}
})().catch(e=>{console.error(e);process.exit(1);});
