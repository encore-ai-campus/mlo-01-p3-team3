import {initialAccount} from './account-state.mjs';

const rail = document.querySelector('[data-ad-delivery]');
if (rail) {
  const screen = matchMedia('(min-width:1280px)');
  const image = rail.querySelector('img'), button = rail.querySelector('button');
  const initialActor = initialAccount().actor_key;
  const scope = {slot_id:rail.dataset.slot};
  if (rail.dataset.project) scope.project_id = rail.dataset.project;
  if (rail.dataset.run) scope.run_id = rail.dataset.run;
  let current=null, generation=0, attempted=false, loading=false, rendered=false;
  let fraction=0, visibleSent=false, visibleTimer=null, expiry=null, disabled=false;
  const events=new Map();
  const uuid=()=>{if(crypto.randomUUID)return crypto.randomUUID();const b=crypto.getRandomValues(new Uint8Array(16));b[6]=(b[6]&15)|64;b[8]=(b[8]&63)|128;const h=[...b].map(x=>x.toString(16).padStart(2,'0')).join('');return `${h.slice(0,8)}-${h.slice(8,12)}-${h.slice(12,16)}-${h.slice(16,20)}-${h.slice(20)}`;};
  const active=()=>!disabled&&screen.matches&&!document.hidden&&!document.body.classList.contains('account-revalidating')&&!document.body.classList.contains('account-inaccessible')&&initialAccount().actor_key===initialActor;
  function hide(){clearTimeout(visibleTimer);clearTimeout(expiry);rail.hidden=true;rendered=false;fraction=0;}
  async function api(path,body){const token=document.querySelector('[name=csrfmiddlewaretoken]')?.value;if(!token)throw Error('CSRF unavailable');const abort=new AbortController(),timer=setTimeout(()=>abort.abort(),6000);try{const r=await fetch(path,{method:'POST',credentials:'same-origin',cache:'no-store',signal:abort.signal,headers:{'Content-Type':'application/json','X-CSRFToken':token},body:JSON.stringify(body)});if(!r.ok)throw Error('Ad request unavailable');return await r.json();}finally{clearTimeout(timer);}}
  async function event(kind,extra={}){const d=current;if(!d||!active())throw Error('Inactive ad');const key=d.decision_id+':'+kind;let payload=events.get(key);if(!payload){payload={event_id:uuid(),kind,occurred_at:new Date().toISOString(),...extra};events.set(key,payload);}return api(`/api/ads/${encodeURIComponent(d.decision_id)}/events/`,payload);}
  function observe(){clearTimeout(visibleTimer);if(!current||!rendered||visibleSent||fraction<.5||!active())return;const version=generation;visibleTimer=setTimeout(async()=>{if(version!==generation||!active()||fraction<.5)return;visibleSent=true;try{await event('VISIBLE',{fraction,continuous_ms:1000,active:true});}catch{}},1000);}
  async function display(){if(loading||attempted||!active())return;loading=true;const version=++generation;hide();try{const d=await api('/api/ads/display/',scope);if(version!==generation||!active())return;attempted=d.status!=='WAITING';if(d.displayable!==true||!d.creative||Date.parse(d.expires_at)<=Date.now())return;current=d;visibleSent=false;
      image.onload=async()=>{if(version!==generation||!active()||Date.parse(d.expires_at)<=Date.now())return;rail.hidden=false;expiry=setTimeout(()=>{generation++;hide();},Math.max(0,Date.parse(d.expires_at)-Date.now()));await new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)));if(version!==generation||!active())return;try{const result=await event('RENDERED',{rendered:true});if(version!==generation)return;if(!['VALID','DUPLICATE'].includes(result.status)&&d.status==='SELECTED'){hide();return;}rendered=true;observe();}catch{hide();}};
      image.onerror=()=>{if(version!==generation)return;void event('LOAD_ERROR').catch(()=>{});hide();};image.alt=d.creative.alt_text||d.creative.title||'광고';image.src=d.creative.image_url;
    }catch{attempted=true;hide();}finally{if(version===generation)loading=false;}}
  new IntersectionObserver(entries=>{fraction=entries[0].intersectionRatio;observe();},{threshold:[0,.5,1]}).observe(image);
  button.addEventListener('click',async()=>{if(!current||!active()||rail.hidden)return;const tab=window.open('about:blank','_blank');if(tab)tab.opener=null;button.disabled=true;try{const result=await event('CLICK');if(tab&&result.navigation?.allowed===true)tab.location.replace(result.navigation.url);else tab?.close();if(result.navigation?.reason!=='ACTIVE')hide();}catch{tab?.close();}finally{button.disabled=false;}});
  function refresh(){if(initialAccount().actor_key!==initialActor||document.body.classList.contains('account-inaccessible')){disabled=true;generation++;hide();return;}if(!active()){generation++;loading=false;hide();return;}if(current){attempted=false;}void display();}
  window.addEventListener('sketchon:project-changed',event=>{
    const id=event.detail?.project_id;
    if(scope.slot_id!=='editor-right'||typeof id!=='string'||!id||id===scope.project_id)return;
    generation++; hide(); current=null; attempted=false; loading=false; visibleSent=false;
    image.onload=null; image.onerror=null; image.removeAttribute('src');
    scope.project_id=id; rail.dataset.project=id;
    void display();
  });
  screen.addEventListener('change',refresh);
  document.addEventListener('visibilitychange',()=>{if(document.hidden){generation++;loading=false;hide();}else refresh();});
  let accountFlags='';
  new MutationObserver(()=>{const next=['account-revalidating','account-inaccessible'].map(key=>document.body.classList.contains(key)).join(':');if(next!==accountFlags){accountFlags=next;refresh();}}).observe(document.body,{attributes:true,attributeFilter:['class']});
  window.addEventListener('sketchon:recommendations-ready',()=>{if(!attempted)void display();});
  window.addEventListener('pagehide',()=>{generation++;loading=false;hide();});
  window.addEventListener('pageshow',event=>{if(event.persisted)refresh();});
  if(scope.slot_id==='editor-right')void display();
}
