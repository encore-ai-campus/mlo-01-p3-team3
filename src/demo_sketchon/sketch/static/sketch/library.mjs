import {initAccountState, initialAccount} from './account-state.mjs';
import {identifier} from './events.mjs';

async function api(path, body) {
    const epoch = window.__sketchAccountEpoch || 0;
    const options = {credentials:'same-origin',cache:'no-store',headers:{Accept:'application/json'}};
    if (body !== undefined) {
        options.method='POST'; options.headers['Content-Type']='application/json';
        options.headers['X-CSRFToken']=document.querySelector('[name=csrfmiddlewaretoken]')?.value || '';
        options.body=JSON.stringify(body);
    }
    const response = await fetch(path,options), data = await response.json();
    if (epoch !== (window.__sketchAccountEpoch || 0)) throw new Error('계정이 변경되었어요. 다시 시도해 주세요.');
    if (!response.ok) throw new Error(data.error || '요청을 완료하지 못했어요.');
    return data;
}

export function resultLibrary({getResult, canSave}) {
    const button=document.getElementById('save-project'), dialog=document.getElementById('save-project-dialog');
    let member=initialAccount().authenticated, saving=false, request=null, stamp=null;
    function controls() { if(button) { button.hidden=!member; button.disabled=saving || !canSave(); } }
    button?.addEventListener('click',()=>{ if(member && canSave()) dialog.showModal(); });
    document.getElementById('cancel-project-save')?.addEventListener('click',()=>dialog.close());
    document.getElementById('save-project-form')?.addEventListener('submit',async event=>{
        event.preventDefault(); if(saving || !member || !canSave()) return;
        saving=true; controls();
        const result=getResult(), title=document.getElementById('saved-project-title').value.trim();
        const key=result.result_id+':'+result.version+':'+title;
        if(stamp!==key) { stamp=key; request=identifier(); }
        const status=document.getElementById('project-save-status');
        try {
            const saved=await api(`/api/results/${result.result_id}/save/`,{request_id:request,result_version:result.version,title});
            status.replaceChildren(); const link=document.createElement('a'); link.href=saved.page_url; link.textContent=`버전 ${saved.version_number} 저장 완료 · 프로젝트 보기 ↗`; status.append(link); dialog.close();
        } catch(error) { document.getElementById('save-project-error').textContent=error.message; }
        finally { saving=false; controls(); }
    });
    return {controls,setAccount(next,{denied=false,changed=false}={}){
        member=next.authenticated && !denied;
        if(denied || changed || !member){
            dialog?.close();
            const title=document.getElementById('saved-project-title');if(title)title.value='나의 공간';
            document.getElementById('save-project-error').textContent='';
        }
        controls();
    }};
}

export function wishlistButton(item, runId) {
    const button=document.createElement('button'); button.textContent='♡ 찜하기';button.dataset.memberOnly='';button.hidden=!initialAccount().authenticated;
    let entry=null, loading=false;
    async function reflect() {
        try {
            const data=await api('/api/wishlist/');
            const key=item.product_id ? 'product:'+item.product_id : 'sample:'+item.catalog_key;
            entry=data.items.find(row=>row.stable_item_key===key)?.id || null;
            button.textContent=entry?'♥ 찜 해제':'♡ 찜하기';button.setAttribute('aria-pressed',String(!!entry));
        } catch { /* Auth refresh and API enforce ownership. */ }
    }
    if(!button.hidden) void reflect();
    button.addEventListener('click',async()=>{
        if(loading) return; loading=true;button.disabled=true;
        try {
            if(entry) { await api(`/api/wishlist/${entry}/remove/`,{});entry=null; }
            else { const data=await api('/api/wishlist/',{recommendation_run_id:runId,item_id:item.id});entry=data.id; }
            button.textContent=entry?'♥ 찜 해제':'♡ 찜하기';button.setAttribute('aria-pressed',String(!!entry));
        } catch(error) { document.getElementById('recommendation-status').textContent=error.message; }
        finally {loading=false;button.disabled=false;}
    });
    return button;
}

if(document.body.classList.contains('member-page')) {
    initAccountState();
    document.addEventListener('click',async event=>{
        const edit=event.target.closest('[data-edit-version]'), remove=event.target.closest('[data-remove-wishlist]');
        const button=edit || remove;if(!button)return;
        button.disabled=true;
        try {
            if(edit) { const next=await api(`/api/saved-versions/${edit.dataset.editVersion}/edit/`,{});window.location.assign(next.editor_url); }
            else { await api(`/api/wishlist/${remove.dataset.removeWishlist}/remove/`,{});remove.closest('[data-wishlist-entry]').remove(); }
        } catch(error) { const status=document.getElementById('library-status');if(status)status.textContent=error.message; }
        finally {button.disabled=false;}
    });
}
