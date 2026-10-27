import {initAccountState} from '/static/sketch/account-state.mjs';
import {drawDocument} from './preview.mjs';
initAccountState({onChange(next){if(!next.can_access_ops || next.ops_page_access === false){document.querySelector('main')?.replaceChildren();location.replace(next.ops_url || '/accounts/login/?next=/ops/');}}});
const canvas=document.querySelector('[data-sketch-preview]');
if(canvas){try{drawDocument(canvas.getContext('2d'),JSON.parse(document.getElementById(canvas.dataset.sketchPreview).textContent),canvas);}catch(error){canvas.replaceWith(Object.assign(document.createElement('p'),{textContent:'스케치 원본은 데이터 펼치기에서 확인할 수 있습니다.'}));}}
