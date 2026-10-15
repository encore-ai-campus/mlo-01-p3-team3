import {createRoomPickClient} from './roompick-client.mjs';
const base = new URL('../', import.meta.url).pathname.replace(/\/$/, '');
const client = createRoomPickClient({baseUrl:base});
const $ = id => document.getElementById(id);
let runId = new URL(location.href).searchParams.get('run');
let timer, current;
const labels = {NEEDS_SCALE_REVIEW:'공간 대비 크기 확인 필요',NEEDS_ANNOTATION_REVIEW:'주석·치수선·문자 확인 필요',NEEDS_STRUCTURE_REVIEW:'참조 이미지와의 구조 차이 · 확인 필요',PENDING:'입력 준비',NEEDS_CATEGORY_SELECTION:'품목 확인 필요',UNSUPPORTED:'지원하지 않는 요청',TEMPLATE_UNAVAILABLE:'이 품목의 템플릿이 없습니다',NEEDS_TEMPLATE_SELECTION:'참조 템플릿을 선택하세요',READY_TO_GENERATE:'생성 요청이 준비되었습니다',GENERATED:'이미지 생성 완료',COMPLETED:'생성 · 비교 분석 완료',NO_OBJECTS:'분석된 물체가 없습니다',BUDGET_WAIT:'호출 예산 대기',FAILED:'작업 실패'};
const changes = {ADDED:'추가',REPLACED:'교체',EXISTING:'기존 유지',UNCERTAIN:'확인 필요'};
const el = (tag, text, cls) => {const node=document.createElement(tag);if(text!==undefined)node.textContent=text;if(cls)node.className=cls;return node;};
function message(text,error=false){$('notice').textContent=text;$('notice').classList.toggle('error',error);}
const asset = name => `${base}/assets/${encodeURIComponent(runId)}/${name.split('/').map(encodeURIComponent).join('/')}`;
async function jsonPost(path,value){return client.api(path,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(value)});}
async function switchRun(id){runId=id;history.replaceState(null,'',`${base}/?run=${encodeURIComponent(id)}`);await refresh();}
function showPlan(plan){$('plan-info').replaceChildren();if(!plan)return;for(const [key,label] of [['category_code','요청 품목'],['color','명시한 색상'],['material','명시한 재질'],['placement_intent','배치 의도'],['reason','해석 근거']]){if(plan[key])$('plan-info').append(el('dt',label),el('dd',plan[key]));}}
function render(state){
    current=state;const running=state.job?.status==='RUNNING';
    $('empty').hidden=true;$('workspace').hidden=false;
    $('run-label').textContent=`입력 버전 ${state.revision_no} · ${state.run_id}`;
    if(state.external_project?.id)document.querySelector('.back').href=`http://192.168.0.44:8003/editor/${encodeURIComponent(state.external_project.id)}/`;
    $('room').src=asset(state.room_image);$('before').src=asset(state.room_image);
    $('sketch-wrap').hidden=!state.sketch_image;if(state.sketch_image)$('sketch').src=asset(state.sketch_image);
    $('request-text').textContent=state.text||'스케치 입력';showPlan(state.plan);
    const phase=state.result_image?'result':state.preview?'preview':state.plan?'plan':'input';
    document.querySelectorAll('[data-step]').forEach(n=>n.classList.toggle('active',n.dataset.step===phase));
    const status=running?`${{prepare:'Gemini 해석 · 템플릿 검색',generate:'Cloudflare 생성 · 원본 비교',compare:'원본 · 결과 비교 분석'}[state.job.action]} 중…`:state.job?.status==='FAILED'?state.job.error:labels[state.status]||state.status;
    message(status,state.job?.status==='FAILED');
    $('retry-prepare').hidden=state.job?.status!=='FAILED'||state.job?.action!=='prepare';$('retry-prepare').disabled=running;
    $('stage-info').textContent=Object.entries(state.stages).map(([k,v])=>`${k}: ${v.status}`).join(' · ');
    $('candidate-count').textContent=`${state.candidates.length}개 후보`;
    $('candidates').replaceChildren();
    for(const candidate of state.candidates){const card=el('article',undefined,'candidate'),image=el('img');image.src=asset(candidate.image);image.alt='참조 템플릿';card.append(image,el('strong',candidate.template_id.startsWith('danawa-')?'다나와 · '+candidate.template_id.slice(7,19):candidate.template_id),el('p',`벡터 유사도 ${candidate.score.toFixed(3)} · v${candidate.version}`));
        if(candidate.differences.length)card.append(el('p',candidate.differences.map(v=>`${v.attribute}: 요청 ${v.requested}, 자료 ${v.template||'미지정'}`).join(' / '),'differences'));
        const button=el('button','이 템플릿 선택');button.disabled=running;button.onclick=()=>choose(candidate.template_id);card.append(button);$('candidates').append(card);}
    $('candidate-note').textContent=state.reference_catalog?.source==='danawa'?'저장된 다나와 이미지 벡터에서 검색한 상위 6개입니다. 품목·색상·재질은 미검증이므로 이미지를 확인하고 선택하세요.':state.status==='TEMPLATE_UNAVAILABLE'?'해당 품목의 생성용 템플릿을 등록하고 Canvas에서 새 요청을 보내세요.':'의미 유사도 순입니다. 점수는 정확도 백분율이 아니며 명시 조건과 차이를 함께 확인하세요.';
    $('category-selection').hidden=state.status!=='NEEDS_CATEGORY_SELECTION';
    $('category').replaceChildren(...state.categories.map(value=>{const option=el('option',value);option.value=value;return option;}));
    $('preview-section').hidden=!state.preview;
    if(state.preview){$('sketch-policy').textContent=state.preview.sketch_policy?.sent_to_generator?'상세 가구 스케치를 생성 참조로 전달합니다.':state.sketch_image?'배치용/불확실 스케치는 해석에만 사용하고 생성 API에는 보내지 않습니다. 원본은 보존됩니다.':'스케치 없이 생성합니다.';$('reference-structure').textContent=JSON.stringify({reference_structure:state.preview.reference_structure||{},scale_plan:state.preview.scale_plan||{},api_scale_bounds:state.preview.api_scale_bounds||{}},null,2);$('generation-model').textContent=`${state.preview.model} · ${state.preview.width} × ${state.preview.height}`;$('prompt').textContent=state.preview.prompt;$('references').replaceChildren();for(const ref of state.preview.images){const fig=el('figure'),img=el('img');img.src=asset(ref.api_asset);img.alt=ref.role;fig.append(img,el('figcaption',`image ${ref.order} · ${ref.role}`));$('references').append(fig);}}
    $('generate').disabled=running||!!state.result_image;
    $('result-section').hidden=!state.result_image;$('compare').disabled=running||!state.result_image;
    if(state.result_image){showFidelity(state);$('after').src=asset(state.result_image);$('demo-result').textContent=state.demo?'· 데모 합성':' ';drawObjects(state.comparison?.objects||[]);}
    clearTimeout(timer);if(running)timer=setTimeout(refresh,1200);
}
function showFidelity(state){
    const scale=state.comparison?.scale_check;
    const scaleStatus=scale?.status||'UNCERTAIN';
    $('scale-status').textContent=`공간 대비 크기 검사: ${{APPROPRIATE:'적정 범위로 판정',TOO_SMALL:'과소 생성',TOO_LARGE:'과대 생성',OUT_OF_RANGE:'가로·높이 비례 범위 이탈',UNCERTAIN:'판단 보류'}[scaleStatus]} · ${scale?.reason||'크기 검사가 완료되지 않았습니다.'}${scale?.findings?.length?' / '+scale.findings.join(' / '):''}`;
    $('scale-status').classList.toggle('error',scaleStatus!=='APPROPRIATE');
    const annotations=state.comparison?.annotation_check;
    const annotationStatus=annotations?.status||'UNCERTAIN';
    $('annotation-status').textContent=`주석·치수선·문자 검사: ${{CLEAR:'추가 주석 없음으로 판정',DETECTED:'추가 주석 발견',UNCERTAIN:'확인 필요'}[annotationStatus]} · ${annotations?.reason||'검사가 완료되지 않았습니다.'}${annotations?.findings?.length?' / '+annotations.findings.join(' / '):''}`;
    $('annotation-status').classList.toggle('error',annotationStatus!=='CLEAR');
    const fidelity=state.comparison?.reference_fidelity;
    const status=fidelity?.status||'UNCERTAIN';
    $('reference-status').textContent=`참조 구조 검사: ${{MATCH:'일치로 판정',MISMATCH:'참조 구조와 차이 있음',UNCERTAIN:'확인 필요'}[status]} · ${fidelity?.reason||'비교 분석이 완료되지 않았습니다.'}${fidelity?.differences?.length?' / '+fidelity.differences.join(' / '):''}`;
    $('reference-status').classList.toggle('error',status!=='MATCH');
    $('structure-note').textContent='AI 비교와 투영 영역 검사이며 실제 치수·설치 가능성을 보증하지 않습니다. 과대·과소·판단 보류 결과는 보존하며 자동 재생성하지 않습니다. 새 요청 준비 후 생성 버튼으로 다시 호출합니다.';
    $('new-generation').hidden=!state.selected_template_id||(status==='MATCH'&&annotationStatus==='CLEAR'&&scaleStatus==='APPROPRIATE');
    $('new-generation').disabled=state.job?.status==='RUNNING';
}
$('new-generation').onclick=()=>choose(current.selected_template_id);
function drawObjects(objects){$('objects').replaceChildren();$('boxes').replaceChildren();$('boxes').setAttribute('viewBox','0 0 1000 1000');$('boxes').setAttribute('preserveAspectRatio','none');for(const obj of objects){const color=obj.change_type==='EXISTING'?'#8a938f':obj.change_type==='UNCERTAIN'?'#bc8b39':'#b76b43';const rect=document.createElementNS('http://www.w3.org/2000/svg','rect');for(const [k,v] of Object.entries({x:obj.box.x_min*1000,y:obj.box.y_min*1000,width:(obj.box.x_max-obj.box.x_min)*1000,height:(obj.box.y_max-obj.box.y_min)*1000,fill:'none',stroke:color,'stroke-width':4}))rect.setAttribute(k,v);$('boxes').append(rect);const row=el('div',undefined,'object'),text=el('div');text.append(el('b',obj.category_code),el('p',obj.reason));row.append(el('span',changes[obj.change_type],`badge ${obj.change_type==='EXISTING'?'existing':obj.change_type==='UNCERTAIN'?'uncertain':''}`),text);if(!obj.eligible_for_selection)text.append(el('p',obj.change_type==='EXISTING'?'기본 추천 대상에서 제외':'사용자 확인 전 상품 검색 대기'));$('objects').append(row);}if(!objects.length)$('objects').append(el('p','결과 이미지는 보존됩니다. 물체가 없거나 비교 분석이 완료되지 않았습니다.','hint'));}
async function refresh(){if(!runId)return;try{render(await client.api(`/api/runs/${runId}`));}catch(error){message(error.message,true);}}
async function choose(template_id){try{message('선택을 새 입력 버전으로 저장하는 중…');const value=await jsonPost(`/api/runs/${runId}/selection`,{template_id});await switchRun(value.run_id);}catch(error){message(error.message,true);}}
$('confirm-category').onclick=async()=>{try{const value=await jsonPost(`/api/runs/${runId}/selection`,{category_code:$('category').value});await switchRun(value.run_id);}catch(error){message(error.message,true);}};
$('generate').onclick=async()=>{try{$('generate').disabled=true;await jsonPost(`/api/runs/${runId}/actions/generate`,{});await refresh();}catch(error){message(error.message,true);$('generate').disabled=false;}};
$('compare').onclick=async()=>{try{await jsonPost(`/api/runs/${runId}/actions/compare`,{retry:true});await refresh();}catch(error){message(error.message,true);}};
$('refresh').onclick=refresh;
$('retry-prepare').onclick=async()=>{try{await jsonPost(`/api/runs/${runId}/actions/prepare`,{retry:true});await refresh();}catch(error){message(error.message,true);}};
$('test-form').onsubmit=async event=>{event.preventDefault();const button=event.target.querySelector('button');button.disabled=true;try{const form=new FormData(event.target);if(!form.get('sketch')?.size)form.delete('sketch');form.set('request_id',crypto.randomUUID());form.set('document',JSON.stringify({strokes:[],items:[]}));const value=await client.api('/api/runs',{method:'POST',body:form});await switchRun(value.run_id);}catch(error){message(error.message,true);}finally{button.disabled=false;}};
$('template-form').onsubmit=async event=>{event.preventDefault();try{const form=new FormData(event.target);form.set('reviewed','true');form.set('attributes',JSON.stringify(Object.fromEntries(['color','material'].filter(k=>form.get(k)).map(k=>[k,form.get(k)]))));await client.api('/api/templates/register',{method:'POST',body:form});event.target.reset();await loadCatalog();message('템플릿을 저장했습니다. 새 입력 요청부터 반영됩니다.');}catch(error){message(error.message,true);}};
async function loadCatalog(){try{const value=await client.api('/api/templates');$('catalog-info').textContent=value.reference_catalog?.source==='danawa'?`다나와 저장 벡터 ${value.templates.length}개 사용 · 이미지 재임베딩 없이 재사용 · 품목 메타데이터 미확인`:`등록된 생성용 템플릿 ${value.templates.length}개`;$('template-form').hidden=value.reference_catalog?.source==='danawa';}catch(error){message(error.message,true);}}
await client.api('/api/csrf');await loadCatalog();if(runId)await refresh();
