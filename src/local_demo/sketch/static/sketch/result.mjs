import {initAccountState, initialAccount} from './account-state.mjs';
import {resultLibrary, wishlistButton} from './library.mjs';
import {createDurableQueue} from './durable-queue.mjs';
import {createPlatformAPI} from './platform-api.mjs';
import {createTracker, identifier} from './events.mjs';
import {normalizePoint, moveItem, resizeItem} from './geometry.mjs';

const $ = id => document.getElementById(id);
const visitor = JSON.parse($('visitor-data').textContent);
var library;
let result = JSON.parse($('result-data').textContent);
let placements = structuredClone(result.placements), selected = null, gesture = null, busy = false;
const {api, record, flushEvents} = createTracker(result.project_id, visitor);
const elements = new Map();
const platform = createPlatformAPI(api, JSON.parse($('platform-config').textContent || '{}'));
let recommendation = null, recommending = false, recommendationRequest = null, activeObjectId = null, requestSequence = 0;
let showingOriginal = false, renderedVersion = null;
let placementPast = [], placementFuture = [];
const initialPlacements = structuredClone(result.initial_placements || placements);
const historyKey = version => 'local_demo.placement-history.' + initialAccount().actor_key + '.' + result.result_id + '.' + version;
try {
    const saved = JSON.parse(sessionStorage.getItem(historyKey(result.version)) || 'null');
    if (saved && JSON.stringify(saved.current) === JSON.stringify(placements) && Array.isArray(saved.past) && Array.isArray(saved.future)) {
        const valid = stack => stack.every(rows => Array.isArray(rows) && rows.length === placements.length && rows.every(row => placements.some(item => item.id === row.id) && ['x','y','width','height'].every(key => Number.isFinite(row[key]))));
        if (valid(saved.past) && valid(saved.future)) { placementPast = saved.past; placementFuture = saved.future; }
    }
} catch {}
function persistHistory(previousVersion) {
    try {
        sessionStorage.setItem(historyKey(result.version), JSON.stringify({current: placements, past: placementPast, future: placementFuture}));
        if (previousVersion !== result.version) sessionStorage.removeItem(historyKey(previousVersion));
    } catch {}
}
const objectButtons = new Map();
let impressionObserver = null, correctionObjectId = null, correctionPlacementId = null, correctionVersion = null, correctionRequest = null;
const productEventKey = 'local_demo.product-events.' + initialAccount().actor_key;
const productEvents = createDurableQueue(productEventKey);
let delivering = false;
async function deliverProductEvents() {
    if (delivering) return;
    delivering = true;
    try {
        for (const row of productEvents.rows()) {
            try {
                await api('/api/recommendation-items/' + encodeURIComponent(row.item_id) + '/events/', row.body, {timeoutMs:4000});
                productEvents.remove(row.id);
            } catch (error) {
                if ([400,404,409,422].includes(error.status)) productEvents.reject(row.id,error.message);
                else break;
            }
        }
    } catch { /* Storage failures leave pending events available for retry. */ }
    finally { delivering = false; }
}
function productEvent(item, event_type, runId) {
    if (!item || !runId) return;
    const id = identifier();
    productEvents.add({id, item_id: item.id, body: {recommendation_run_id: runId, event_type, event_id: id}});
    void deliverProductEvents();
}
setInterval(deliverProductEvents, 2000);
window.addEventListener('online', deliverProductEvents);
void deliverProductEvents();
function currentObject() { return (result.scene_objects || []).find(row => row.placement_id === selected); }
const productSlots = [...document.querySelectorAll('[data-product-slot]')];
function emptyRecommendations(title = '상품 이미지', hint = '추천할 물체를 선택해 주세요.') {
    impressionObserver?.disconnect();
    for (const [index, card] of productSlots.entries()) {
        card.classList.remove('rejected', 'skeleton'); delete card.dataset.itemId;
        const photo = document.createElement('div'); photo.className = 'product-image';
        const number = document.createElement('span'); number.textContent = String(index + 1).padStart(2, '0');
        const label = document.createElement('small'); label.textContent = title; photo.append(number, label);
        const name = document.createElement('h3'); name.textContent = '추천 상품 ' + (index + 1);
        const note = document.createElement('p'); note.textContent = hint; card.replaceChildren(photo, name, note);
    }
    $('catalog-mode').textContent = '상품 연결 준비'; $('retry-recommendation').hidden = true;
}
function renderObjects() {
    renderedVersion = result.version; objectButtons.clear(); $('scene-objects').replaceChildren();
    for (const obj of result.scene_objects || []) {
        const button = document.createElement('button'); button.type = 'button'; button.className = 'scene-object-button';
        button.dataset.objectId = obj.id; button.setAttribute('aria-pressed', 'false');
        const image = document.createElement('img'); image.src = obj.preview_url; image.alt = '';
        const copy = document.createElement('span'), title = document.createElement('strong'), note = document.createElement('small');
        title.textContent = obj.label;
        note.textContent = ({ADDED:'새로 배치한 물체', REPLACED:'교체한 물체', EXISTING:'기존 물체 · 추천 제외', UNCERTAIN:'추가·교체 여부 확인 필요'})[obj.change_type] || '확인 필요';
        if (result.mode === 'mock') note.textContent += ' · 데모';
        copy.append(title, note); const check = document.createElement('span'); check.className = 'object-check'; check.textContent = '✓'; check.setAttribute('aria-hidden', 'true');
        button.append(image, copy, check); button.disabled = obj.change_type === 'EXISTING';
        button.addEventListener('click', () => { if (busy || gesture) return; selected = obj.placement_id; draw(); });
        $('scene-objects').append(button); objectButtons.set(obj.id, button);
    }
}
function syncSelection() {
    if (renderedVersion !== result.version) renderObjects();
    const obj = currentObject();
    if ((obj?.id || null) !== activeObjectId) {
        activeObjectId = obj?.id || null; recommendationRequest = null; recommendation = null;
        requestSequence++; recommending = false; $('confirm-object').checked = false;
        emptyRecommendations();
        $('recommendation-status').textContent = obj ? `${obj.label}의 유사 상품을 찾아볼까요?` : '물체를 선택하면 상품을 확인할 수 있어요.';
        $('recommendation-status').classList.remove('error-text');
    }
    for (const [id, button] of objectButtons) button.setAttribute('aria-pressed', String(id === activeObjectId));
    $('object-confirmation').hidden = obj?.change_type !== 'UNCERTAIN';
    $('recommendation-context').hidden = !obj;
    if (obj) {
        $('recommended-object-image').src = obj.preview_url;
        $('recommended-object-name').textContent = obj.label + (obj.user_corrected ? ' · 수정한 검색 품목' : '');
    }
    $('adjust-search').disabled = busy || !!gesture;
    if (correctionPlacementId !== (obj?.placement_id || null)) {
        correctionPlacementId = obj?.placement_id || null;
        if (obj) $('object-category').value = obj.category_id;
        $('object-correction-status').textContent = '';
    }
    if (correctionObjectId !== (obj?.id || null) || correctionVersion !== result.version) {
        correctionObjectId = obj?.id || null; correctionVersion = result.version; correctionRequest = null;
    }
    const placement = placements.find(row => row.id === selected);
    for (const dimension of ['width', 'height']) {
        const field = $('region-' + dimension);
        field.value = placement ? Number((placement[dimension] * 100).toFixed(2)) : '';
        field.max = placement ? Number(((1 - placement[dimension === 'width' ? 'x' : 'y']) * 100).toFixed(2)) : 100;
    }
    for (const field of $('object-correction-form').elements) field.disabled = !obj || busy || !!gesture;
    $('save-object').disabled = !obj || busy || !!gesture || $('object-category').value === obj.category_id;
    $('recommend').disabled = busy || !!gesture || recommending || !obj || obj.change_type === 'EXISTING' || (obj.change_type === 'UNCERTAIN' && !$('confirm-object').checked);
}

function message(text, error = false) { $('status').textContent = text; $('status').classList.toggle('error', error); }
function controls() {
    library?.controls();
    for (const id of ['download-png', 'redraw', 'edit-sketch', 'new-search']) $(id).disabled = busy || !!gesture;
    $('view-result').disabled = $('view-original').disabled = busy || !!gesture;
    $('undo-placement').disabled = busy || !!gesture || !placementPast.length;
    $('redo-placement').disabled = busy || !!gesture || !placementFuture.length;
    $('reset-placement').disabled = busy || !!gesture || JSON.stringify(placements) === JSON.stringify(initialPlacements);
    $('placements').inert = showingOriginal;
    $('placements').style.pointerEvents = busy ? 'none' : '';
    syncSelection();
}
function draw() {
    for (const item of placements) {
        const element = elements.get(item.id);
        Object.assign(element.style, {left: item.x * 100 + '%', top: item.y * 100 + '%', width: item.width * 100 + '%', height: item.height * 100 + '%'});
        element.classList.toggle('selected', item.id === selected);
    }
    $('selection-name').textContent = placements.find(item => item.id === selected)?.name || '';
    controls();
}
async function save(historyAction = 'edit') {
    const previous = structuredClone(result.placements);
    if (JSON.stringify(placements) === JSON.stringify(previous)) return;
    const previousVersion = result.version;
    requestSequence++; recommending = false; recommendationRequest = null; recommendation = null; emptyRecommendations();
    try { sessionStorage.removeItem('local_demo.recommendation.' + result.result_id); } catch {}
    busy = true; controls(); message('조절한 배치를 합성 이미지에 반영하고 있어요.');
    try {
        result = await api('/api/results/' + result.result_id + '/placements/', {
            version: result.version, placements: placements.map(({id, x, y, width, height}) => ({id, x, y, width, height})),
        });
        placements = structuredClone(result.placements);
        if (historyAction === 'undo') { placementPast.pop(); placementFuture.push(previous); }
        else if (historyAction === 'redo') { placementFuture.pop(); placementPast.push(previous); }
        else { placementPast.push(previous); placementPast = placementPast.slice(-20); placementFuture = []; }
        persistHistory(previousVersion);
        message('반영됐어요. 현재 위치와 크기로 결과를 내려받을 수 있습니다.');
    } catch (error) {
        placements = structuredClone(result.placements);
        message(error.message + ' 저장된 배치로 되돌렸습니다.', true);
    } finally { busy = false; draw(); }
}
function position(event) { return normalizePoint(event.clientX, event.clientY, $('result-stage').getBoundingClientRect()); }
for (const item of placements) {
    const element = document.createElement('button');
    element.type = 'button'; element.className = 'placement'; element.dataset.placementId = item.id;
    element.setAttribute('aria-label', item.name + ': 눌러 선택, 끌거나 방향키로 이동, 모서리 또는 Shift 방향키로 크기 조절');
    const image = document.createElement('img'); image.alt = ''; image.draggable = false;
    image.src = '/api/results/' + result.result_id + '/sprites/' + item.id + '/';
    image.addEventListener('error', () => { message('가구 미리보기를 불러오지 못했습니다. 새로고침해 주세요.', true); });
    const handle = document.createElement('span'); handle.className = 'resize-handle'; handle.setAttribute('aria-hidden', 'true');
    element.append(image, handle); $('placements').append(element); elements.set(item.id, element);
    element.addEventListener('pointerdown', event => {
        if (busy || gesture || showingOriginal || !event.isPrimary || event.button !== 0) return;
        selected = item.id; draw();
        event.preventDefault();
        const current = placements.find(row => row.id === selected);
        gesture = {kind:'placement', id: event.pointerId, clientX:event.clientX, clientY:event.clientY, moved:false, start: position(event), source: structuredClone(current), element,
            resizing: event.target === handle, pointerType: event.pointerType};
        element.setPointerCapture(event.pointerId); draw();
    });
    element.addEventListener('pointermove', event => {
        if (!gesture || gesture.kind !== 'placement' || gesture.id !== event.pointerId || gesture.element !== element) return;
        if (!gesture.moved && Math.hypot(event.clientX - gesture.clientX, event.clientY - gesture.clientY) < 6) return;
        gesture.moved = true;
        event.preventDefault(); const pos = position(event);
        placements[placements.findIndex(row => row.id === selected)] = (gesture.resizing ? resizeItem : moveItem)(gesture.source, pos.x - gesture.start.x, pos.y - gesture.start.y);
        draw();
    });
    for (const type of ['pointerup', 'pointercancel', 'lostpointercapture']) element.addEventListener(type, event => finish(event, type !== 'pointerup'));
    element.addEventListener('click', () => { if (!busy && !gesture) { selected = item.id; draw(); } });
    element.addEventListener('keydown', event => {
        if (busy || gesture || showingOriginal || !event.key.startsWith('Arrow')) return;
        event.preventDefault(); selected = item.id;
        const index = placements.findIndex(row => row.id === selected), delta = event.shiftKey ? .02 : .005;
        placements[index] = (event.shiftKey ? resizeItem : moveItem)(placements[index], event.key === 'ArrowRight' ? delta : event.key === 'ArrowLeft' ? -delta : 0, event.key === 'ArrowDown' ? delta : event.key === 'ArrowUp' ? -delta : 0);
        record(event.shiftKey ? 'result.resized' : 'result.moved', {result_id: result.result_id, placement_id: item.id, pointer_type: 'keyboard'});
        draw(); void save();
    });
}
function finish(event, cancelled = false) {
    if (!gesture || (event && gesture.id !== event.pointerId)) return;
    const done = gesture; gesture = null;
    const index = placements.findIndex(row => row.id === done.source.id), item = placements[index];
    if (cancelled) {
        placements[index] = done.source; record('result.cancelled', {result_id: result.result_id, placement_id: done.source.id});
    } else if (JSON.stringify(item) !== JSON.stringify(done.source)) {
        record(done.resizing ? 'result.resized' : 'result.moved', {result_id: result.result_id, placement_id: item.id,
            x: item.x, y: item.y, width: item.width, height: item.height, pointer_type: done.pointerType});
        void save();
    }
    if (done.element.hasPointerCapture(done.id)) done.element.releasePointerCapture(done.id);
    draw(); void flushEvents();
}
window.addEventListener('blur', () => finish(null, true));
for (const [id, action] of [['undo-placement','undo'], ['redo-placement','redo'], ['reset-placement','reset']]) $(id).addEventListener('click', () => {
    if (busy || gesture) return;
    const next = action === 'undo' ? placementPast.at(-1) : action === 'redo' ? placementFuture.at(-1) : initialPlacements;
    if (!next) return;
    placements = structuredClone(next); draw(); void save(action);
});
function download(url, format) {
    if (busy || gesture) return;
    const link = document.createElement('a'); link.href = url; link.download = 'sketchon-result.' + format; link.click();
    record('export.downloaded', {format, result_id: result.result_id, version: result.version});
    void flushEvents();
}
$('download-png').addEventListener('click', () => download(result.download_url, 'png'));
for (const button of document.querySelectorAll('[data-restart]')) button.addEventListener('click', async () => {
    if (busy || gesture) return;
    busy = true; controls(); record('redraw.clicked', {result_id: result.result_id, mode: button.dataset.restart});
    try {
        const project = await api('/api/results/' + result.result_id + '/redraw/', {mode: button.dataset.restart});
        void flushEvents(); window.location.assign(project.editor_url);
    } catch (error) { busy = false; controls(); message(error.message, true); }
});
document.addEventListener('click', event => { const button = event.target.closest('button'); if (button?.id) record('button.clicked', {button: button.id}); });
$('confirm-object').addEventListener('change', controls);
for (const [id, original] of [['view-result', false], ['view-original', true]]) $(id).addEventListener('click', () => {
    if (busy || gesture) return;
    showingOriginal = original; $('placements').hidden = original;
    $('view-result').setAttribute('aria-pressed', String(!original)); $('view-original').setAttribute('aria-pressed', String(original));
    record('view.changed', {view: original ? 'original' : 'generated', result_id: result.result_id}); controls();
});

function renderRecommendations(data) {
    emptyRecommendations();
    $('catalog-mode').textContent = data.mode === 'mock' ? '샘플 카드 · 판매 상품 아님' : '유사 상품';
    $('recommendation-subtitle').textContent = data.notice || '현재 선택한 물체와 비슷한 상품입니다. 실제 규격과 옵션을 확인해 주세요.';
    if (!data.items.length) {
        emptyRecommendations('상품 검색 결과 없음','다른 물체나 검색 조건으로 다시 시도해 주세요.');
        return;
    }
    // An API response is not an impression. Observe only cards entering the viewport.
    impressionObserver = new IntersectionObserver(entries => {
        for (const entry of entries) if (entry.isIntersecting) {
            const item = data.items.find(row => row.id === entry.target.dataset.itemId);
            if (recommendation?.recommendation_run_id === data.recommendation_run_id) productEvent(item, 'PRODUCT_IMPRESSION', data.recommendation_run_id); impressionObserver.unobserve(entry.target);
        }
    }, {threshold: 0.1});
    for (const [index, item] of data.items.slice(0,3).entries()) {
        const card = productSlots[index]; card.replaceChildren(); card.dataset.itemId = item.id;
        const photo = document.createElement('div'); photo.className = 'product-image';
        const image = document.createElement('img'); image.src = item.image_url; image.alt = item.name; image.loading = 'lazy';
        image.addEventListener('error', () => { image.hidden = true; const fallback = document.createElement('span'); fallback.className = 'image-fallback'; fallback.textContent = '이미지 준비 중'; photo.append(fallback); }, {once: true});
        const label = document.createElement('span'); label.className = 'sample-label'; label.textContent = data.mode === 'mock' ? 'SAMPLE' : item.source;
        photo.append(image, label);
        const name = document.createElement('h3'); name.textContent = item.name;
        const brand = document.createElement('p'); brand.textContent = item.brand?.replace(/^RoomPick 예시/, 'SketchOn 예시');
        const price = document.createElement('p'); price.className = 'product-price'; price.textContent = item.price === null ? (data.mode === 'mock' ? '가격 · 판매 연결 미제공' : '판매처에서 가격 확인') : new Intl.NumberFormat('ko-KR').format(item.price) + '원';
        const note = document.createElement('p'); note.textContent = item.price_note;
        const actions = document.createElement('div'); actions.className = 'product-actions';
        const choose = document.createElement('button'); choose.textContent = '마음에 들어요'; choose.setAttribute('aria-pressed', 'false');
        const reject = document.createElement('button'); reject.textContent = '다른 취향이에요'; reject.setAttribute('aria-pressed', 'false');
        choose.addEventListener('click', () => {
            if (choose.getAttribute('aria-pressed') === 'true') return;
            choose.setAttribute('aria-pressed', 'true'); reject.setAttribute('aria-pressed', 'false'); card.classList.remove('rejected'); productEvent(item, 'PRODUCT_SELECTED', data.recommendation_run_id);
        });
        reject.addEventListener('click', () => {
            if (reject.getAttribute('aria-pressed') === 'true') return;
            reject.setAttribute('aria-pressed', 'true'); choose.setAttribute('aria-pressed', 'false'); card.classList.add('rejected'); productEvent(item, 'PRODUCT_REJECTED', data.recommendation_run_id);
        });
        const link = document.createElement('button'); link.className = 'product-link'; link.textContent = data.mode === 'mock' ? '판매 링크 연결 전' : '상품 페이지 보기 ↗'; link.disabled = !item.link_available;
        link.addEventListener('click', async () => {
            link.disabled = true;
            try {
                const response = await platform.resolveLink(item.id, {recommendation_run_id: data.recommendation_run_id, event_id: identifier(), link_kind: 'COMPARISON'});
                if (new URL(response.url).protocol !== 'https:') throw new Error('상품 주소를 확인하지 못했습니다.');
                window.location.assign(response.url);
            } catch (error) { $('recommendation-status').textContent = error.message; $('recommendation-status').classList.add('error-text'); }
            finally { link.disabled = !item.link_available; }
        });
        actions.append(choose, reject, link, wishlistButton(item, data.recommendation_run_id)); card.append(photo, brand, name, price, note, actions); impressionObserver.observe(card);
    }
}
async function recommend() {
    const obj = currentObject();
    if (!obj || recommending || busy || gesture) return;
    if (obj.change_type === 'EXISTING' || (obj.change_type === 'UNCERTAIN' && !$('confirm-object').checked)) return;
    recommending = true; controls(); const sequence = ++requestSequence, version = result.version;
    recommendationRequest ||= identifier();
    $('recommendation-status').textContent = '선택한 물체와 어울리는 상품을 찾고 있어요.';
    $('recommendation-status').classList.remove('error-text'); $('retry-recommendation').hidden = true;
    impressionObserver?.disconnect();
    emptyRecommendations('상품 찾는 중','선택한 물체와 어울리는 샘플을 찾고 있어요.');
    for (const card of productSlots) card.classList.add('skeleton');
    try {
        const data = await platform.recommend(obj.id, {scene_generation_id: result.result_id, request_id: recommendationRequest, user_confirmed: $('confirm-object').checked});
        if (sequence !== requestSequence || currentObject()?.id !== obj.id || version !== result.version) return;
        if (data.scene_object_id !== obj.id) throw new Error('추천 결과의 물체가 일치하지 않습니다. 같은 요청으로 다시 시도해 주세요.');
        recommendation = data; renderRecommendations(data);
        const completedRequest = recommendationRequest; recommendationRequest = null;
        $('recommendation-status').textContent = data.mode === 'mock' ? '화면 확인용 샘플 상품 카드입니다.' : `${data.items.length}개의 유사 상품을 확인했어요.`;
        try { sessionStorage.setItem('local_demo.recommendation.' + result.result_id, JSON.stringify({objectId: obj.id, requestId: completedRequest, version})); } catch {}
        $('recommendation-section').scrollIntoView({behavior: 'smooth', block: 'start'});
    } catch (error) {
        if (sequence !== requestSequence) return;
        $('recommendation-status').textContent = error.message; $('recommendation-status').classList.add('error-text');
        emptyRecommendations('상품 추천 지연','생성된 공간은 유지됩니다. 상품 추천만 다시 시도해 주세요.');
        if ([400,404,409,422].includes(error.status) || error.retryable === false || ['FAILED','CANCELLED','NOT_READY'].includes(error.jobStatus)) recommendationRequest=null;
        $('retry-recommendation').hidden = false;
    } finally { if (sequence === requestSequence) { recommending = false; controls(); } }
}
// Search corrections create a new object revision without editing the scene bitmap.
for (const [value, label] of Object.entries(result.category_options || {sofa:'소파',bed:'침대',desk:'책상',plant:'화분',lamp:'조명'})) {
    const option = document.createElement('option'); option.value = value; option.textContent = label; $('object-category').append(option);
}
$('object-correction').addEventListener('toggle', () => {
    if ($('object-correction').open) {
        selected ||= placements[0]?.id || null;
        showingOriginal = false; $('placements').hidden = false;
        $('view-result').setAttribute('aria-pressed', 'true'); $('view-original').setAttribute('aria-pressed', 'false');
    }
    draw();
});
$('object-category').addEventListener('change', () => { correctionRequest = null; controls(); });
for (const dimension of ['width', 'height']) $('region-' + dimension).addEventListener('change', async event => {
    const field = event.target, index = placements.findIndex(row => row.id === selected);
    if (busy || gesture || index < 0) return;
    if (!field.checkValidity()) { field.reportValidity(); return; }
    const previous = placements[index], value = field.valueAsNumber / 100;
    if (Math.abs(previous[dimension] - value) < .00005) return;
    placements[index] = resizeItem(previous, dimension === 'width' ? value - previous.width : 0,
        dimension === 'height' ? value - previous.height : 0);
    const {id, x, y, width, height} = placements[index];
    record('result.resized', {result_id: result.result_id, placement_id: id, x, y, width, height, pointer_type: 'keyboard'});
    draw(); await save(); void flushEvents();
});
$('object-correction-form').addEventListener('submit', async event => {
    event.preventDefault(); const obj = currentObject(), placement = placements.find(row => row.id === selected);
    if (busy || gesture || !obj || !placement || $('object-category').value === obj.category_id) return;
    const {x, y, width, height} = placement, box = {x, y, width, height};
    correctionRequest ||= identifier(); busy = true;
    requestSequence++; recommending = false; recommendationRequest = null; recommendation = null; emptyRecommendations(); controls();
    $('object-correction-status').textContent = '검색 조건을 반영하고 있어요.';
    try {
        result = await platform.revise(obj.id, {scene_generation_id:result.result_id, request_id:correctionRequest, category_id:$('object-category').value, bbox:box});
        correctionRequest = null; correctionPlacementId = null; placements = structuredClone(result.placements); renderObjects(); draw();
        $('object-correction-status').textContent = '적용됐어요. 생성 이미지는 유지되며 새 조건으로 상품을 찾을 수 있어요.';
    } catch (error) { $('object-correction-status').textContent = error.message; }
    finally { busy = false; controls(); }
});
$('adjust-search').addEventListener('click', () => {
    if (busy || gesture) return;
    showingOriginal = false; $('placements').hidden = false;
    $('view-result').setAttribute('aria-pressed','true'); $('view-original').setAttribute('aria-pressed','false');
    $('object-correction').open = true; controls();
    $('result-stage').scrollIntoView({behavior:'smooth',block:'center'});
});
$('recommend').addEventListener('click', recommend);
$('retry-recommendation').addEventListener('click', recommend);
renderObjects();
draw(); void flushEvents();
// Restore the previous request by its server-owned ID, never trust cached card data.
void (async () => {
    try {
        const saved = JSON.parse(sessionStorage.getItem('local_demo.recommendation.' + result.result_id) || 'null');
        if (saved?.version !== result.version) return;
        const obj = result.scene_objects?.find(row => row.id === saved?.objectId);
        if (!obj) return;
        selected = obj.placement_id; draw();
        const expected = activeObjectId, sequence = ++requestSequence;
        const data = await api('/api/jobs/' + encodeURIComponent(saved.requestId) + '/');
        if (sequence !== requestSequence || activeObjectId !== expected || data.scene_object_id !== expected || data.status !== 'SUCCEEDED') return;
        recommendationRequest = null; recommendation = data; renderRecommendations(data);
        $('recommendation-status').textContent = '이전에 확인한 추천 결과를 불러왔어요.';
    } catch { /* A stale or inaccessible request never blocks the current scene. */ }
})();



library = resultLibrary({getResult:()=>result,canSave:()=>!busy && !gesture && !showingOriginal});
initAccountState({projectId:()=>result.project_id,onChange:(state,{denied,changed})=>{
    library.setAccount(state,{denied,changed});
    if(denied){requestSequence++;impressionObserver?.disconnect();return;}
}});
