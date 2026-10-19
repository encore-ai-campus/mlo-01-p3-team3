(() => {
 'use strict';
 const $=id=>document.getElementById(id);
 function newId(){if(typeof crypto.randomUUID==='function')return crypto.randomUUID();const b=crypto.getRandomValues(new Uint8Array(16));b[6]=(b[6]&15)|64;b[8]=(b[8]&63)|128;const h=Array.from(b,v=>v.toString(16).padStart(2,'0')).join('');return h.slice(0,8)+'-'+h.slice(8,12)+'-'+h.slice(12,16)+'-'+h.slice(16,20)+'-'+h.slice(20);}
 let owner=localStorage.getItem('sketchon-ad-test-visitor');if(!owner){owner=newId();localStorage.setItem('sketchon-ad-test-visitor',owner);}
 let requestBody=null,current=null,generation=0,timer=null,visibleSent=false,fraction=0,expiry=null,rendered=false,reportRevision=0;
 const number=value=>new Intl.NumberFormat('ko-KR').format(value);
 const percent=value=>value===null||value===undefined?'계산 전':value.toFixed(1)+'%';
 const node=(tag,text,cls)=>{const element=document.createElement(tag);if(text!==undefined)element.textContent=text;if(cls)element.className=cls;return element;};
 const raw=value=>{$('output').textContent=JSON.stringify(value,null,2);};
 async function api(path,data){const response=await fetch(path,{method:data?'POST':'GET',credentials:'same-origin',headers:{'Content-Type':'application/json','X-CSRFToken':$('sandbox-app').dataset.csrf},body:data?JSON.stringify(data):undefined});const value=await response.json();if(!response.ok)throw new Error(value.error?.message||'광고 서버 요청을 처리하지 못했습니다.');return value;}
 function hide(){clearTimeout(timer);clearTimeout(expiry);$('creative').classList.add('hidden');$('empty').hidden=false;rendered=false;}
 function markers(summary){for(const key of ['rendered','visible','clicked']){const target=$('current-'+key),confirmed=summary[key]===true;target.textContent=confirmed?'집계 확인':'아직 집계 없음';target.classList.toggle('is-confirmed',confirmed);}}
 function emptyRow(target,columns,text){const row=node('tr'),cell=node('td',text,'table-empty');cell.colSpan=columns;row.append(cell);target.replaceChildren(row);}
 function showDecision(d,retry){const s=d.summary;raw(d);$('decision-state').textContent=s.state_label;$('status').textContent=retry?'같은 요청의 기존 결과를 확인했습니다. 새 입찰을 만들지 않습니다.':s.state_label+' · 조건 검사를 마쳤습니다.';
  if(d.status==='SELECTED'&&!d.displayable)$('status').textContent='이전 선정 결과입니다. 현재 중지·차단·만료 등으로 표시할 수 없습니다.';
  $('selected-title').textContent=s.selected_title;$('selected-owner').textContent=[s.advertiser_name,s.campaign_name].filter(Boolean).join(' · ')||'별도 승인 안내 또는 선정 가능한 광고 없음';
  $('winning-bid').textContent=s.bid_points===undefined||s.bid_points===null?'—':number(s.bid_points)+' 포인트';
  $('candidate-count').textContent=number(s.candidate_count)+'개';$('representative-count').textContent=number(s.representative_count)+'개';
  $('selection-reason').textContent=s.explanation;$('bid-note').textContent=s.history_note+' 실제 결제·차감은 없습니다.';$('expiry-note').textContent='표시 만료 '+s.expires_label;markers(s);
  const body=$('bid-rows');body.replaceChildren();
  for(const bid of s.bids){const row=node('tr',undefined,bid.selected?'winning-row':'');const label=node('td');label.append(node('strong',bid.campaign_name),node('small',bid.advertiser_name));const outcome=node('td');outcome.append(node('span',bid.outcome,'outcome-chip'));row.append(label,node('td',number(bid.bid_points)+' 포인트','points'),outcome);body.append(row);}
  if(!s.bids.length)emptyRow(body,3,'입찰 비교에 참여한 광고가 없습니다.');
  $('excluded-reasons').replaceChildren(...(s.excluded.length?s.excluded.map(item=>node('li',item.label+' · '+number(item.count)+'건')):[node('li','기록된 제외 사유가 없습니다.')]));
 }
 async function loadReport(showRaw=false){const serial=++reportRevision;try{const data=await api('/sandbox/report/');if(serial!==reportRevision)return;const s=data.summary;
  $('total-rendered').textContent=number(s.rendered)+'회';$('total-visible').textContent=number(s.visible)+'회';$('total-clicked').textContent=number(s.clicked)+'회';$('total-ctr').textContent=percent(s.ctr_percent);$('report-time').textContent=s.generated_label+' 기준';
  const body=$('report-rows');body.replaceChildren();for(const item of s.rows){const row=node('tr'),label=node('td');label.append(node('strong',item.campaign_name),node('small',item.date));row.append(label,node('td',item.slot_label),node('td',number(item.rendered)+'회'),node('td',number(item.visible)+'회'),node('td',number(item.clicked)+'회'),node('td',percent(item.ctr_percent)));body.append(row);}if(!s.rows.length)emptyRow(body,6,'아직 이미지 표시가 집계된 광고가 없습니다.');
  $('report-note').textContent='노출일은 한국 시간 기준입니다. 중복·진단 사건은 성과에 더하지 않습니다. 선정 불가 기록 '+number(s.no_fill_count)+'건.';if(showRaw)raw(data);
 }catch(error){if(serial===reportRevision){$('report-time').textContent='조회 실패';$('report-note').textContent=error.message+' 표시된 기존 수치는 최신이 아닐 수 있습니다.';}}}
 async function eventFor(decision,kind,extra={}){const version=generation;const result=await api('/sandbox/decisions/'+decision.decision_id+'/events/',{event_id:newId(),visitor_id:owner,kind,occurred_at:new Date().toISOString(),...extra});if(version===generation&&current?.decision_id===decision.decision_id){markers(result.summary);$('event-notice').textContent=result.summary.kind_label+' · '+result.summary.state_label+' — '+result.summary.reason_label;}loadReport();return result;}
 function observe(){clearTimeout(timer);if(!current||!rendered||visibleSent||fraction<.5||document.hidden)return;const d=current,v=generation;timer=setTimeout(async()=>{if(v!==generation||document.hidden||fraction<.5)return;visibleSent=true;try{await eventFor(d,'VISIBLE',{fraction,continuous_ms:1000,active:true});}catch(error){$('event-notice').textContent=error.message;}},1000);}
 new IntersectionObserver(entries=>{fraction=entries[0].intersectionRatio;observe();},{threshold:[0,.5,1]}).observe($('ad-image'));document.addEventListener('visibilitychange',observe);
 async function select(fresh){const retry=!fresh&&!!requestBody;if(fresh||!requestBody){requestBody={request_id:newId(),visitor_id:owner,context_id:newId(),placement:$('placement').value,signals:$('signal').value?[{key:$('signal').value,source:$('source').value}]:[],organic_product_ids:[]};}
  const v=++generation;hide();current=null;visibleSent=false;$('status').textContent='집행 조건과 입찰 포인트를 확인하고 있습니다…';$('decision-state').textContent='선정 중';$('new-context').disabled=true;$('retry').disabled=true;$('event-notice').textContent='새 광고의 표시 결과를 기다리고 있습니다.';
  try{const d=await api('/sandbox/decisions/',requestBody);if(v!==generation)return;current=d;showDecision(d,retry);if(!d.displayable){$('empty').textContent='지금 표시할 수 있는 광고가 없습니다.';loadReport();return;}
   const image=$('ad-image');image.onload=async()=>{if(v!==generation)return;if(Date.now()>=Date.parse(d.expires_at)){hide();$('status').textContent='광고 표시 기간이 만료됐습니다.';return;}
    $('creative').classList.remove('hidden');$('empty').hidden=true;expiry=setTimeout(()=>{if(v!==generation)return;hide();$('status').textContent='광고 표시 기간이 만료됐습니다. 자동으로 다시 입찰하지 않습니다.';$('decision-state').textContent='표시 기간 만료';$('empty').textContent='광고 표시 기간이 끝났습니다.';},Math.max(0,Date.parse(d.expires_at)-Date.now()));
    await new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)));if(v!==generation)return;
    try{const event=await eventFor(d,'RENDERED',{rendered:true});if(v!==generation)return;if(!['VALID','DUPLICATE'].includes(event.status)&&d.status==='SELECTED'){hide();$('status').textContent='현재 표시할 수 없는 광고입니다.';return;}rendered=true;observe();}catch(error){hide();$('status').textContent=error.message;}};
   image.onerror=async()=>{if(v!==generation)return;hide();$('status').textContent='배너 이미지를 불러오지 못했습니다.';try{await eventFor(d,'LOAD_ERROR');}catch(error){$('event-notice').textContent=error.message;}};
   image.alt=d.creative.alt_text;image.src=d.creative.image_url;
  }catch(error){if(v===generation){hide();$('status').textContent=error.message;$('decision-state').textContent='요청 실패';}}
  finally{if(v===generation){$('new-context').disabled=false;$('retry').disabled=false;}}
 }
 $('new-context').onclick=()=>select(true);$('retry').onclick=()=>select(false);$('report').onclick=()=>loadReport(true);
 $('ad-click').onclick=async()=>{if(!current)return;const decision=current;const tab=window.open('about:blank','_blank');if(tab)tab.opener=null;try{const result=await eventFor(decision,'CLICK');raw(result);if(tab&&result.navigation?.allowed)tab.location.replace(result.navigation.url);else{if(tab)tab.close();if(!tab&&result.navigation?.allowed)$('event-notice').textContent+=' 브라우저의 새 창 차단을 해제해 주세요.';}}catch(error){if(tab)tab.close();$('event-notice').textContent=error.message;}};
 loadReport();
})();
