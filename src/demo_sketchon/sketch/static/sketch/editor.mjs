import {initAccountState, initialAccount} from './account-state.mjs';
import {createPlatformAPI} from './platform-api.mjs';
import {createTracker, identifier} from './events.mjs';
import {createCollector} from './collector.mjs';
import {clamp, normalizePoint, moveItem, resizeItem, strokeBounds, transformStroke} from './geometry.mjs';
import {floodRegion, outlineHit, selectedGroup, transformGroup, groupBounds, boxGroup, strokeHit, strokesTouch, fillHit, cutHit} from './paint.mjs';
const $ = id => document.getElementById(id);
const visitor = JSON.parse($('visitor-data').textContent);
let historyActor = initialAccount().actor_key;
const stage = $('stage'), ink = $('ink'), surface = $('surface');
const context = ink.getContext('2d'), objects = surface.getContext('2d');
let project = JSON.parse($('initial-data').textContent), drawing = structuredClone(project.document);
let mode = 'pen', selected = null, gesture = null, busy = false, changed = false;
let resizeMode = false;
let past = [], future = [], requestId = null, textEdit = null;
let cropSelection = null, clipboard = null, pasteCount = 0;
let marquee = null, boxed = null, thicknessBefore = null;
let savedHistory = null;
try {
    const saved = JSON.parse(sessionStorage.getItem('roompick.history.' + historyActor) || 'null');
    if (saved?.current === project.id && saved.revision === project.revision && Array.isArray(saved.past) && Array.isArray(saved.future)) savedHistory = saved;
} catch {}
for (const stroke of drawing.strokes) stroke.id ||= identifier();
const fillCanvases = new Map();
const brushSizes = {pen: 8, eraser: 35, shape: 8};
const tracker = createTracker(project.id, visitor);
const {api, record: recordEvent, flushEvents} = tracker;
const platform = createPlatformAPI(api, JSON.parse($('platform-config').textContent || '{}'));
let editingRequirements = false;
function restoreRequirements() {
    const input = project.request_context || {};
    $('text-requirements').value = input.text_requirements || '';
    $('requirements-count').textContent = $('text-requirements').value.length;
    for (const button of document.querySelectorAll('[data-preference]')) button.setAttribute('aria-pressed', String(input.style_preferences?.[0] === button.dataset.preference));
    editingRequirements = false;
}
function finishRequirements() {
    if (!editingRequirements) return;
    editingRequirements = false;
    record('requirements.changed', {text_requirements: $('text-requirements').value});
}
$('text-requirements').addEventListener('input', () => {
    if (!editingRequirements) checkpoint();
    editingRequirements = true;
    project.request_context = {...project.request_context, text_requirements: $('text-requirements').value};
    $('requirements-count').textContent = $('text-requirements').value.length;
    invalidateResult(); changed = true; saveHistory(); updateControls();
});
$('text-requirements').addEventListener('blur', finishRequirements);
for (const button of document.querySelectorAll('[data-preference]')) button.addEventListener('click', () => {
    if (busy || gesture) return;
    finishRequirements(); checkpoint();
    const value = project.request_context?.style_preferences?.[0] === button.dataset.preference ? [] : [button.dataset.preference];
    project.request_context = {...project.request_context, style_preferences: value};
    restoreRequirements(); record('style.changed', {style: value[0] || ''}); saveHistory(); updateControls();
});
const collector = createCollector({getProject: () => project, getDocument: () => drawing, surface, writer: () => project.writer_id || visitor, actor: initialAccount().actor_key, api});
function record(type, payload = {}) {
    const eventId = recordEvent(type, payload);
    collector.operation(type, eventId);
}
function message(text, error = false) {
    $('status').textContent = text; $('status').classList.toggle('error', error);
    $('generation-error').hidden = !error; $('generation-error').textContent = error ? text : '';
}
function brushKey() { return mode === 'eraser' ? 'eraser' : mode === 'pen' ? 'pen' : 'shape'; }
function brushWidth() { return brushSizes[brushKey()] / 1000; }
function selectedValue() {
    if (!selected) return null;
    return selected.type === 'item' ? drawing.items.find(item => item.id === selected.id) : drawing.strokes[selected.index];
}
function selectedBox() {
    const value = selectedValue();
    return value ? {...groupBounds(drawing, selectedGroup(drawing, value)), id: value.id, group_id: value.group_id} : null;
}
function hit(pos, box) { return pos.x >= box.x && pos.x <= box.x + box.width && pos.y >= box.y && pos.y <= box.y + box.height; }
function invalidateResult() {
    requestId = null;
}
function snapshot(before = structuredClone(drawing), source = project) {
    const {document: ignored, ...metadata} = source;
    return {project: structuredClone(metadata), drawing: before};
}
function checkpoint(before, source = project) {
    past.push(snapshot(before, source)); if (past.length > 20) past.shift();
    future = []; changed = true; invalidateResult();
    queueMicrotask(saveHistory);
}
function saveHistory() {
    if (busy || gesture || textEdit || thicknessBefore) return;
    try { sessionStorage.setItem('roompick.history.' + historyActor, JSON.stringify({current: project.id, revision: project.revision, drawing, request_context: project.request_context || {}, past, future, changed})); }
    catch { message('실행 취소 기록은 현재 페이지에서 유지됩니다. 저장 공간 부족으로 새로고침 후에는 복구되지 않을 수 있습니다.', true); }
}
function segment(stroke, first, last, target = context, size = ink) {
    const context = target, ink = size;
    context.save();
    context.globalCompositeOperation = stroke.tool === 'eraser' ? 'destination-out' : 'source-over';
    context.strokeStyle = context.fillStyle = stroke.color; context.lineWidth = stroke.width * ink.width;
    context.lineCap = context.lineJoin = 'round'; context.beginPath();
    if (first === last) { context.arc(first.x * ink.width, first.y * ink.height, context.lineWidth / 2, 0, Math.PI * 2); context.fill(); }
    else { context.moveTo(first.x * ink.width, first.y * ink.height); context.lineTo(last.x * ink.width, last.y * ink.height); context.stroke(); }
    context.restore();
}
function drawInk() {
    context.clearRect(0, 0, ink.width, ink.height);
    renderArtwork(context, ink);
}
function renderArtwork(target, size) {
    const layer = document.createElement('canvas'); layer.width = size.width; layer.height = size.height;
    const ctx = layer.getContext('2d');
    for (const value of [...drawing.items.filter(item => item.kind === 'fill'), ...drawing.strokes, ...drawing.items.filter(item => item.kind !== 'fill')]) {
        const dest = value.cuts?.length ? ctx : target;
        if (dest === ctx) ctx.clearRect(0, 0, layer.width, layer.height);
        if (value.points) {
            segment(value, value.points[0], value.points[0], dest, size);
            for (let i = 1; i < value.points.length; i++) segment(value, value.points[i - 1], value.points[i], dest, size);
        } else drawObject(value, dest, size);
        if (dest === ctx) {
            for (const cut of value.cuts) {
                segment(cut, cut.points[0], cut.points[0], ctx, size);
                for (let i = 1; i < cut.points.length; i++) segment(cut, cut.points[i - 1], cut.points[i], ctx, size);
            }
            target.drawImage(layer, 0, 0);
        }
    }
}
function fillAt(pos) {
    if (drawing.items.length >= 100) { message('삽입 요소는 100개까지 가능합니다.', true); return; }
    const buffer = document.createElement('canvas'), scale = Math.min(1, 1200 / Math.max(project.width, project.height));
    buffer.width = Math.max(1, Math.round(project.width * scale)); buffer.height = Math.max(1, Math.round(project.height * scale));
    const ctx = buffer.getContext('2d', {willReadFrequently: true}); renderArtwork(ctx, buffer);
    try {
        const pixels = ctx.getImageData(0, 0, buffer.width, buffer.height);
        const region = floodRegion(pixels.data, buffer.width, buffer.height, pos.x * buffer.width, pos.y * buffer.height);
        const color = $('color').value;
        const rgb = [1, 3, 5].map(start => parseInt(color.slice(start, start + 2), 16));
        if (region.target[3] === 255 && rgb.every((channel, index) => channel === region.target[index])) return;
        const candidates = drawing.items.filter(item => ['rectangle', 'ellipse'].includes(item.kind) && hit(pos, item) &&
            (item.kind !== 'ellipse' || Math.hypot((pos.x - item.x - item.width / 2) / (item.width / 2), (pos.y - item.y - item.height / 2) / (item.height / 2)) <= 1))
            .map(item => ({value: item, box: item}));
        const area = {x: region.bounds.x / buffer.width, y: region.bounds.y / buffer.height,
            width: region.bounds.width / buffer.width, height: region.bounds.height / buffer.height};
        for (const stroke of drawing.strokes.filter(stroke => stroke.tool === 'pen')) {
            const box = groupBounds(drawing, selectedGroup(drawing, stroke));
            if (hit(pos, box) && area.x >= box.x - .003 && area.y >= box.y - .003 && area.x + area.width <= box.x + box.width + .003 && area.y + area.height <= box.y + box.height + .003) candidates.push({value: stroke, box});
        }
        const parent = candidates.sort((a, b) => a.box.width * a.box.height - b.box.width * b.box.height)[0]?.value;
        const total = drawing.items.filter(item => item.kind === 'fill').reduce((sum, item) => sum + item.runs.length, 0);
        if (total + region.runs.length > 200000) throw new Error('채우기 저장 한도를 초과했습니다.');
        const item = {id: identifier(), kind: 'fill', color, parent_id: parent?.id || null,
            x: region.bounds.x / buffer.width, y: region.bounds.y / buffer.height,
            width: region.bounds.width / buffer.width, height: region.bounds.height / buffer.height,
            mask_width: region.bounds.width, mask_height: region.bounds.height, runs: region.runs};
        checkpoint(structuredClone(drawing)); drawing.items.push(item); selected = null;
        record('fill.applied', {item}); redraw(); void flushEvents();
        message('연결된 영역에 색을 채웠어요. 도형의 외곽선을 잡으면 채운 색도 함께 움직입니다.');
    } catch (error) { message(error.message, true); }
}
function drawObject(item, target = objects, size = surface) {
    const objects = target, surface = size;
    const x = item.x * surface.width, y = item.y * surface.height;
    const w = item.width * surface.width, h = item.height * surface.height;
    objects.save(); objects.strokeStyle = objects.fillStyle = item.color || '#245847';
    objects.lineWidth = (item.stroke_width || .008) * surface.width; objects.lineCap = 'round';
    if (item.kind === 'fill') {
        const key = item.id + item.color;
        if (!fillCanvases.has(key)) {
            const bitmap = document.createElement('canvas'); bitmap.width = item.mask_width + 2; bitmap.height = item.mask_height + 2;
            const ctx = bitmap.getContext('2d'); ctx.fillStyle = item.color;
            // Extend one pixel underneath the antialiased outline to avoid transparent seams.
            for (const [row, start, end] of item.runs) ctx.fillRect(start, row, end - start + 3, 3);
            if (fillCanvases.size >= 12) fillCanvases.delete(fillCanvases.keys().next().value);
            fillCanvases.set(key, bitmap);
        }
        objects.imageSmoothingEnabled = true; objects.imageSmoothingQuality = 'high';
        const px = w / item.mask_width, py = h / item.mask_height;
        objects.drawImage(fillCanvases.get(key), x - px, y - py, w + 2 * px, h + 2 * py);
    } else if (item.kind === 'text') {
        const lines = item.text.split('\n'), font = item.font_size * surface.width;
        objects.font = font + 'px "Malgun Gothic", "Apple SD Gothic Neo", sans-serif'; objects.textBaseline = 'top';
        const naturalWidth = Math.max(1, ...lines.map(line => objects.measureText(line).width));
        objects.translate(x, y); objects.scale(w / naturalWidth, h / (font * 1.3 * lines.length));
        lines.forEach((line, i) => objects.fillText(line, 0, i * font * 1.3));
    } else {
        objects.beginPath();
        if (item.kind === 'rectangle') objects.rect(x, y, w, h);
        else if (item.kind === 'ellipse') objects.ellipse(x + w / 2, y + h / 2, w / 2, h / 2, 0, 0, Math.PI * 2);
        else {
            const x1 = item.flip_x ? x + w : x, y1 = item.flip_y ? y + h : y;
            const x2 = item.flip_x ? x : x + w, y2 = item.flip_y ? y : y + h;
            objects.moveTo(x1, y1); objects.lineTo(x2, y2);
            if (item.kind === 'arrow') {
                const angle = Math.atan2(y2 - y1, x2 - x1), size = Math.min(Math.hypot(w, h) / 3, surface.width * .035);
                objects.moveTo(x2 - size * Math.cos(angle - .5), y2 - size * Math.sin(angle - .5));
                objects.lineTo(x2, y2); objects.lineTo(x2 - size * Math.cos(angle + .5), y2 - size * Math.sin(angle + .5));
            }
        }
        objects.stroke();
    }
    objects.restore();
}
function drawObjects() {
    objects.clearRect(0, 0, surface.width, surface.height);
    if (marquee) {
        objects.strokeStyle = '#b87839'; objects.lineWidth = 2; objects.setLineDash([6, 4]);
        objects.strokeRect(marquee.x * surface.width, marquee.y * surface.height, marquee.width * surface.width, marquee.height * surface.height);
        objects.setLineDash([]);
    }
    if (mode === 'crop' && cropSelection) {
        const box = cropSelection, x = box.x * surface.width, y = box.y * surface.height, w = box.width * surface.width, h = box.height * surface.height;
        objects.fillStyle = '#17271b80';
        objects.fillRect(0, 0, surface.width, y); objects.fillRect(0, y + h, surface.width, surface.height - y - h);
        objects.fillRect(0, y, x, h); objects.fillRect(x + w, y, surface.width - x - w, h);
        objects.strokeStyle = '#fff'; objects.lineWidth = 2; objects.strokeRect(x, y, w, h);
    }
    const box = selectedBox(); if (!box) return;
    const scale = surface.width / stage.clientWidth;
    objects.save(); objects.strokeStyle = '#b87839'; objects.lineWidth = 2 * scale;
    objects.setLineDash([6 * scale, 4 * scale]);
    objects.strokeRect(box.x * surface.width, box.y * surface.height, box.width * surface.width, box.height * surface.height);
    objects.setLineDash([]); objects.fillStyle = '#b87839';
    objects.fillRect((box.x + box.width) * surface.width - 22 * scale, (box.y + box.height) * surface.height - 22 * scale, 22 * scale, 22 * scale);
    objects.restore();
}
function updateControls() {
    $('undo').disabled = busy || !past.length; $('redo').disabled = busy || !future.length;
    $('remove-item').disabled = busy || !selectedValue();
    $('copy').disabled = busy || !selectedValue(); $('paste').disabled = busy || !clipboard;
    $('crop-actions').hidden = mode !== 'crop'; $('apply-crop').disabled = busy || !cropSelection;
    $('resize-selection').disabled = busy || !selectedValue();
    $('group-selection').disabled = busy || !boxed || boxed.items.length + boxed.strokes.length < 2;
    $('ungroup-selection').disabled = busy || !selectedValue()?.group_id;
    $('resize-selection').setAttribute('aria-pressed', String(resizeMode));
    const text = selected?.type === 'item' && selectedValue()?.kind === 'text';
    $('color-controls').hidden = !text && ['eraser', 'marquee', 'crop'].includes(mode);
    $('width-controls').hidden = ['fill', 'text', 'marquee', 'crop'].includes(mode) || text;
    if (!gesture) $('text-controls').hidden = !textEdit && (mode !== 'text' || !!text);
    $('tool-settings').hidden = $('color-controls').hidden && $('width-controls').hidden && $('text-controls').hidden;
    $('counts').textContent = drawing.strokes.length + '획 · 표시 ' + drawing.items.length + '개';
    $('empty-hint').hidden = !!project.photo_url;
    const photoHost = $(project.photo_url ? 'photo-actions-host' : 'empty-hint');
    if ($('photo-actions').parentElement !== photoHost) photoHost.append($('photo-actions'));
}
function redraw() { drawInk(); drawObjects(); updateControls(); positionTextEditor(); }
function resizeCanvases() {
    const rect = stage.getBoundingClientRect(); if (!rect.width || !rect.height) return;
    const scale = Math.min(window.devicePixelRatio || 1, 2048 / rect.width, 2048 / rect.height);
    for (const canvas of [ink, surface]) { canvas.width = Math.round(rect.width * scale); canvas.height = Math.round(rect.height * scale); }
    redraw();
}
new ResizeObserver(resizeCanvases).observe(stage);
function point(event) {
    const p = normalizePoint(event.clientX, event.clientY, surface.getBoundingClientRect());
    return {x: +p.x.toFixed(6), y: +p.y.toFixed(6), pressure: +clamp(event.pressure || .5).toFixed(3)};
}
function brushPreview(event) {
    const preview = $('brush-preview'); preview.hidden = busy || textEdit || !['pen', 'eraser'].includes(mode);
    if (preview.hidden) return;
    const rect = surface.getBoundingClientRect(), diameter = brushWidth() * rect.width;
    preview.style.left = (event.clientX - rect.left) + 'px'; preview.style.top = (event.clientY - rect.top) + 'px';
    preview.style.width = preview.style.height = diameter + 'px'; preview.classList.toggle('eraser', mode === 'eraser');
}
surface.addEventListener('pointerleave', () => { if (!gesture) $('brush-preview').hidden = true; });
function makeText(pos, text) {
    const font = Number($('font-size').value) / 1200, lines = text.split('\n');
    objects.font = (font * surface.width) + 'px sans-serif';
    const width = clamp(Math.max(...lines.map(line => objects.measureText(line).width)) / surface.width, .04, .9);
    const height = clamp(font * project.width / project.height * 1.3 * lines.length, .04, .9);
    return {id: identifier(), kind: 'text', text, color: $('color').value, font_size: font,
        x: clamp(pos.x, 0, 1 - width), y: clamp(pos.y, 0, 1 - height), width, height};
}
function beginText(pos, original = null) {
    commitText();
    if (!original && drawing.items.length >= 100) { message('삽입 요소는 100개까지 가능합니다.', true); return; }
    textEdit = {pos: original ? {x: original.x, y: original.y} : pos, original, before: structuredClone(drawing)};
    if (original) {
        drawing.items = drawing.items.filter(item => item.id !== original.id);
        $('font-size').value = Math.round(original.font_size * 1200);
        $('color').value = original.color;
    }
    selected = null;
    $('inline-text').value = original?.text || '';
    $('inline-text').hidden = false;
    $('brush-preview').hidden = true;
    redraw(); $('inline-text').focus();
    message('사진 위에 바로 입력하세요. Enter 또는 바깥쪽 클릭으로 완료합니다.');
}
function positionTextEditor() {
    if (!textEdit) return;
    const input = $('inline-text'), pos = textEdit.pos, rect = stage.getBoundingClientRect();
    const font = Number($('font-size').value) / 1200 * rect.width;
    const text = input.value || ' ';
    objects.font = font + 'px "Malgun Gothic", sans-serif';
    const naturalWidth = Math.max(80, ...text.split('\n').map(line => objects.measureText(line).width + 12));
    const width = Math.min(rect.width, naturalWidth), x = Math.min(pos.x * rect.width, rect.width - width);
    input.style.left = x + 'px'; input.style.top = Math.min(pos.y * rect.height, rect.height - font * 1.5) + 'px';
    input.style.width = width + 'px'; input.style.fontSize = font + 'px'; input.style.color = $('color').value;
    input.style.height = 'auto'; input.style.height = Math.min(rect.height - parseFloat(input.style.top), Math.max(font * 1.5, input.scrollHeight)) + 'px';
}
function commitText(cancelled = false) {
    if (!textEdit) return;
    const edit = textEdit, value = $('inline-text').value.trim(); textEdit = null;
    $('inline-text').hidden = true;
    if (cancelled) { drawing = edit.before; redraw(); return; }
    if (!value) {
        if (edit.original) { checkpoint(edit.before); record('annotation.removed', {item: edit.original}); }
        redraw(); return;
    }
    const item = makeText(edit.pos, value);
    if (edit.original) {
        item.id = edit.original.id;
        if (edit.original.group_id) item.group_id = edit.original.group_id;
        if (edit.original.cuts) item.cuts = structuredClone(edit.original.cuts);
    }
    checkpoint(edit.before); drawing.items.push(item); selected = {type: 'item', id: item.id};
    record(edit.original ? 'annotation.edited' : 'annotation.added', {item});
    mode = 'marquee';
    for (const button of document.querySelectorAll('[data-tool]')) button.setAttribute('aria-pressed', String(button.dataset.tool === mode));
    surface.style.cursor = 'grab'; redraw(); void flushEvents();
}
$('inline-text').addEventListener('input', positionTextEditor);
$('inline-text').addEventListener('blur', () => commitText());
$('inline-text').addEventListener('keydown', event => {
    if (event.isComposing) return;
    if (event.key === 'Escape') { event.preventDefault(); commitText(true); }
    if (event.key === 'Enter' && !event.shiftKey) { event.preventDefault(); commitText(); }
});
surface.addEventListener('dblclick', event => {
    if (busy || gesture || project.locked) return;
    const pos = point(event), item = [...drawing.items].reverse().find(item => item.kind === 'text' && hit(pos, item));
    if (item) beginText(pos, item);
});
function eraseObjects(pos, previous = pos) {
    if (!gesture?.stroke || gesture.stroke.tool !== 'eraser') return;
    const radius = gesture.stroke.width / 2, ry = radius * project.width / project.height;
    for (const value of [...drawing.items, ...drawing.strokes.filter(stroke => stroke.tool === 'pen')]) {
        const box = value.points ? strokeBounds(value) : value;
        const pad = (value.stroke_width || 0) / 2, py = pad * project.width / project.height;
        const touches = Math.max(pos.x, previous.x) + radius >= box.x - pad && Math.min(pos.x, previous.x) - radius <= box.x + box.width + pad && Math.max(pos.y, previous.y) + ry >= box.y - py && Math.min(pos.y, previous.y) - ry <= box.y + box.height + py;
        if (touches && !value.cuts?.includes(gesture.stroke)) {
            if ((value.cuts?.length || 0) >= 100) { message('이 요소의 부분 지우기 한도는 100회입니다.', true); continue; }
            (value.cuts ||= []).push(gesture.stroke);
        }
    }
}
function pointBudget() {
    return [...drawing.items, ...drawing.strokes].reduce((sum, value) => sum + (value.points?.length || 0) + (value.cuts || []).reduce((n, cut) => n + cut.points.length, 0), 0);
}
function itemAt(pos, rect) {
    const item = [...drawing.items].reverse().find(item => item.kind !== 'fill' && !cutHit(item, pos, rect) && outlineHit(item, pos, rect));
    if (item) return item;
    const fill = [...drawing.items].reverse().find(item => item.kind === 'fill' && fillHit(item, pos, rect));
    if (!fill) return null;
    return drawing.items.find(item => item.id === fill.parent_id) || fill;
}
function connectStroke(stroke) {
    const touched = drawing.strokes.filter(other => other !== stroke && other.tool === 'pen' && strokesTouch(stroke, other, project));
    if (!touched.length) return;
    const groups = new Set(touched.map(other => other.group_id).filter(Boolean)), ids = new Set(touched.map(other => other.id));
    const groupId = touched.find(other => other.group_id)?.group_id || identifier();
    stroke.group_id = groupId;
    for (const value of [...drawing.items, ...drawing.strokes]) if (ids.has(value.id) || groups.has(value.group_id)) value.group_id = groupId;
}
surface.addEventListener('pointerdown', event => {
    if (busy || project.locked || gesture || !event.isPrimary || event.button !== 0) return;
    if (!project.photo_url) { message('공간 사진을 먼저 업로드해 주세요.', true); return; }
    commitText();
    event.preventDefault(); brushPreview(event);
    const pos = point(event), before = structuredClone(drawing);
    if (mode === 'fill') { fillAt(pos); return; }
    if (mode === 'crop') {
        selected = null; cropSelection = null;
        gesture = {id: event.pointerId, before, start: pos, cropping: true};
        surface.setPointerCapture(event.pointerId); return;
    }
    const rect = surface.getBoundingClientRect();
    const previousBox = selectedBox();
    const handleHit = previousBox && previousBox.width * rect.width >= 56 && previousBox.height * rect.height >= 56 &&
        pos.x <= previousBox.x + previousBox.width && pos.y <= previousBox.y + previousBox.height &&
        pos.x >= previousBox.x + previousBox.width - 22 / rect.width && pos.y >= previousBox.y + previousBox.height - 22 / rect.height;
    const hitItem = handleHit && selected?.type === 'item' ? selectedValue() : itemAt(pos, rect);
    if (mode === 'text' && (!hitItem || hitItem.kind === 'text')) {
        beginText(pos, hitItem?.kind === 'text' ? hitItem : null);
        return;
    }
    const automaticSelect = hitItem && mode !== 'eraser' && mode !== 'pen' &&
        (!['line', 'arrow'].includes(mode) || ['line', 'arrow', 'text'].includes(hitItem.kind));
    if (mode === 'marquee' || automaticSelect) {
        const previousSelection = selected;
        const item = hitItem;
        const fillParent = item?.kind === 'fill' ? drawing.strokes.findIndex(stroke => stroke.id === item.parent_id) : -1;
        const strokeIndex = handleHit && selected?.type === 'stroke' ? selected.index : fillParent >= 0 ? fillParent : item ? -1 : drawing.strokes.findLastIndex(stroke => stroke.tool === 'pen' && !cutHit(stroke, pos, rect) && strokeHit(stroke, pos, rect));
        selected = strokeIndex >= 0 ? {type: 'stroke', index: strokeIndex} : item ? {type: 'item', id: item.id} : null;
        const value = selectedValue(), box = selectedBox(); drawObjects();
        if (!value) {
            boxed = null; marquee = null;
            gesture = {id: event.pointerId, before, start: pos, boxing: true};
            surface.setPointerCapture(event.pointerId); updateControls(); return;
        }
        boxed = null; marquee = null;
        if (value.points || value.stroke_width) {
            $('thickness').value = Math.round((value.points ? value.width : value.stroke_width) * 1000); $('thickness-value').value = $('thickness').value;
        }
        if (item?.kind === 'text') { $('font-size').value = Math.round(item.font_size * 1200); $('font-size-value').value = $('font-size').value; $('color').value = item.color; }
        const alreadySelected = selected?.type === previousSelection?.type && (selected?.type === 'item' ? selected.id === previousSelection?.id : selected?.index === previousSelection?.index);
        const resizing = resizeMode || (alreadySelected && box.width * rect.width >= 56 && box.height * rect.height >= 56 && (box.x + box.width - pos.x) * rect.width < 28 && (box.y + box.height - pos.y) * rect.height < 28);
        gesture = {id: event.pointerId, before, start: pos, source: structuredClone(value), box: structuredClone(box), resizing, pointerType: event.pointerType,
            group: selectedGroup(before, value)};
    } else if (mode === 'pen' || mode === 'eraser') {
        const count = pointBudget();
        if (count >= 100000 || drawing.strokes.length >= 1000) { message('그림 저장 한도에 도달했어요. 새로 시작해 주세요.', true); return; }
        selected = null;
        const stroke = {tool: mode, color: $('color').value, width: brushWidth(), pointer_type: event.pointerType, points: [pos]};
        if (mode === 'pen') { stroke.id = identifier(); drawing.strokes.push(stroke); }
        gesture = {id: event.pointerId, before, stroke, remaining: 100000 - count};
        eraseObjects(pos);
        if (pointBudget() > 100000) { drawing = before; gesture = null; message('지우기 저장 한도를 초과했습니다.', true); return; }
        collector.begin(event, stroke);
        drawInk();
    } else {
        if (drawing.items.length >= 100) { message('삽입 요소는 100개까지 가능합니다.', true); return; }
        const item = {id: identifier(), kind: mode, color: $('color').value, stroke_width: brushWidth(),
            x: Math.min(pos.x, .999), y: Math.min(pos.y, .999), width: .001, height: .001, flip_x: false, flip_y: false};
        drawing.items.push(item); selected = {type: 'item', id: item.id};
        gesture = {id: event.pointerId, before, start: pos, shape: item, pointerType: event.pointerType};
    }
    surface.setPointerCapture(event.pointerId); updateControls();
});
surface.addEventListener('pointermove', event => {
    brushPreview(event);
    if (!gesture && !['pen', 'eraser', 'text', 'fill', 'crop'].includes(mode)) surface.style.cursor = drawing.items.some(item => outlineHit(item, point(event), surface.getBoundingClientRect())) ? 'grab' : 'crosshair';
    if (!gesture || gesture.id !== event.pointerId) return; event.preventDefault();
    collector.move(event);
    if (gesture.cropping || gesture.boxing) {
        const pos = point(event);
        const box = {x: Math.min(pos.x, gesture.start.x), y: Math.min(pos.y, gesture.start.y), width: Math.abs(pos.x - gesture.start.x), height: Math.abs(pos.y - gesture.start.y)};
        if (gesture.cropping) cropSelection = box; else marquee = box;
        drawObjects(); updateControls();
    } else if (gesture.stroke) {
        const samples = event.getCoalescedEvents?.() || [];
        for (const sample of samples.length ? samples : [event]) {
            const pos = point(sample), points = gesture.stroke.points, last = points.at(-1);
            if (points.length >= Math.min(10000, gesture.remaining)) break;
            if (Math.abs(pos.x - last.x) + Math.abs(pos.y - last.y) < .0008) continue;
            if (pointBudget() + drawing.items.length + drawing.strokes.length >= 100000) { message('그림·지우기 저장 한도에 도달했습니다.', true); break; }
            points.push(pos); eraseObjects(pos, last);
        }
        drawInk();
    } else {
        const pos = point(event);
        if (gesture.shape) {
            const item = gesture.shape;
            item.x = Math.min(pos.x, gesture.start.x); item.y = Math.min(pos.y, gesture.start.y);
            item.width = Math.min(1 - item.x, Math.max(.001, Math.abs(pos.x - gesture.start.x)));
            item.height = Math.min(1 - item.y, Math.max(.001, Math.abs(pos.y - gesture.start.y)));
            item.flip_x = pos.x < gesture.start.x; item.flip_y = pos.y < gesture.start.y;
        } else {
            const box = (gesture.resizing ? resizeItem : moveItem)(gesture.box, pos.x - gesture.start.x, pos.y - gesture.start.y);
            transformGroup(drawing, gesture.before, gesture.group, gesture.box, box);
        }
        redraw();
    }
});
function endGesture(event, cancelled = false) {
    if (!gesture || (event && gesture.id !== event.pointerId)) return;
    collector.end(event, cancelled ? (event?.type === 'pointercancel' ? 'cancel' : 'lost_capture') : 'up');
    const done = gesture; gesture = null;
    resizeMode = false;
    if (done.boxing) {
        boxed = cancelled || !marquee ? null : boxGroup(drawing, marquee);
        if (boxed) record('selection.boxed', {item_count: boxed.items.length, stroke_count: boxed.strokes.length});
        if (surface.hasPointerCapture(done.id)) surface.releasePointerCapture(done.id);
        message(boxed?.items.length + boxed?.strokes.length >= 2 ? '영역 안의 요소를 선택했어요. 그룹화를 누르면 함께 이동할 수 있습니다.' : '영역 선택 도구로 요소를 누르거나 전체를 둘러싸도록 드래그하세요.');
        redraw(); return;
    }
    if (done.cropping) {
        if (cancelled) cropSelection = null;
        else if (cropSelection) record('crop.selected', cropSelection);
        if (surface.hasPointerCapture(done.id)) surface.releasePointerCapture(done.id);
        redraw(); return;
    }
    if (done.shape && !['line', 'arrow'].includes(done.shape.kind) && (done.shape.width < .01 || done.shape.height < .01)) cancelled = true;
    if (cancelled) { drawing = done.before; selected = null; record('stroke.cancelled', {pointer_type: done.stroke?.pointer_type || done.pointerType}); }
    else if (JSON.stringify(drawing) !== JSON.stringify(done.before)) {
        if (done.stroke?.tool === 'pen') connectStroke(done.stroke);
        checkpoint(done.before);
        if (done.stroke) record('stroke.completed', {stroke: done.stroke});
        else if (done.shape) record('annotation.added', {item: done.shape});
        else {
            const eventType = done.resizing ? 'annotation.resized' : 'annotation.moved';
            if (selected.type === 'item') record(eventType, {item: selectedValue(), pointer_type: done.pointerType});
            else record(eventType, {stroke_index: selected.index, ...selectedBox(), pointer_type: done.pointerType});
        }
        message('표시를 편집하거나 상품 검색하기를 눌러보세요.');
    }
    if (surface.hasPointerCapture(done.id)) surface.releasePointerCapture(done.id);
    redraw(); $('brush-preview').hidden = true; void flushEvents();
}
surface.addEventListener('pointerup', event => endGesture(event));
surface.addEventListener('pointercancel', event => endGesture(event, true));
surface.addEventListener('lostpointercapture', event => endGesture(event, true));
window.addEventListener('blur', () => endGesture(null, true));
function chooseTool(tool) {
    if (gesture || busy) return; commitText(); mode = tool;
    resizeMode = false;
    boxed = null; marquee = null;
    if (['eraser', 'fill'].includes(tool) || (tool === 'pen' && !selectedValue()?.points) || (selectedValue()?.kind === 'text' && !['text', 'marquee'].includes(tool))) selected = null;
    if (mode === 'crop') selected = null;
    for (const button of document.querySelectorAll('[data-tool]')) button.setAttribute('aria-pressed', String(button.dataset.tool === mode));
    $('thickness').value = brushSizes[brushKey()]; $('thickness-value').value = $('thickness').value;
    $('size-label').textContent = mode === 'eraser' ? '지우개 크기' : mode === 'pen' ? '펜 크기' : '도형 선 크기';
    surface.style.cursor = ['pen', 'eraser'].includes(mode) ? 'none' : mode === 'text' ? 'text' : mode === 'marquee' ? 'grab' : 'crosshair';
    $('brush-preview').hidden = true; updateControls(); record('tool.changed', {tool});
}
for (const button of document.querySelectorAll('[data-tool]')) button.addEventListener('click', () => chooseTool(button.dataset.tool));
$('thickness').addEventListener('input', () => {
    brushSizes[brushKey()] = Number($('thickness').value); $('thickness-value').value = $('thickness').value; $('brush-preview').hidden = true;
    if (!selectedValue() || busy || gesture) return;
    thicknessBefore ||= structuredClone(drawing);
    const group = selectedGroup(drawing, selectedValue()), width = Number($('thickness').value) / 1000;
    group.strokes.forEach(i => { if (drawing.strokes[i].tool === 'pen') drawing.strokes[i].width = width; });
    group.items.forEach(i => { if ('stroke_width' in drawing.items[i]) drawing.items[i].stroke_width = width; });
    redraw();
});
$('thickness').addEventListener('change', () => {
    if (thicknessBefore && JSON.stringify(thicknessBefore) !== JSON.stringify(drawing)) { checkpoint(thicknessBefore); record('annotation.edited', {width: Number($('thickness').value) / 1000}); }
    thicknessBefore = null; record('tool.changed', {tool: mode, width: brushWidth()}); updateControls();
});
$('color').addEventListener('change', () => {
    const item = selectedValue();
    if (!busy && !gesture && item?.kind === 'text' && item.color !== $('color').value) {
        checkpoint(); item.color = $('color').value;
        record('annotation.edited', {item}); redraw(); void flushEvents();
    }
    record('tool.changed', {color: $('color').value});
});
$('font-size').addEventListener('input', () => { $('font-size-value').value = $('font-size').value; positionTextEditor(); });
$('resize-selection').addEventListener('click', () => {
    if (!selectedValue() || busy || gesture) return;
    chooseTool('marquee'); resizeMode = true; updateControls();
    message('선택한 요소를 오른쪽 아래로 끌면 커지고, 반대로 끌면 작아집니다.');
});
function copySelection() {
    if (busy || gesture || !selectedValue()) return;
    commitText();
    const box = structuredClone(selectedBox());
    clipboard = {document: structuredClone(drawing), box, selection: structuredClone(selected),
        group: selectedGroup(drawing, selectedValue())};
    pasteCount = 0; record('selection.copied', {item_count: clipboard.group.items.length, stroke_count: clipboard.group.strokes.length});
    updateControls(); message('선택한 요소와 연결된 채우기를 복사했어요. Ctrl+V로 붙여넣으세요.');
}
function pasteSelection() {
    if (busy || gesture || !clipboard) return;
    const {document: source, group, box, selection} = clipboard;
    if (drawing.items.length + group.items.length > 100 || drawing.strokes.length + group.strokes.length > 1000) { message('붙여넣기 저장 한도를 초과합니다.', true); return; }
    const points = [...drawing.strokes, ...group.strokes.map(index => source.strokes[index])].reduce((sum, stroke) => sum + stroke.points.length, 0);
    const runs = [...drawing.items, ...group.items.map(index => source.items[index])].filter(item => item.kind === 'fill').reduce((sum, item) => sum + item.runs.length, 0);
    if (points > 100000 || runs > 200000) { message('점 또는 채우기 저장 한도를 초과합니다.', true); return; }
    const copied = structuredClone(source), delta = Math.min(.15, .025 * (++pasteCount));
    transformGroup(copied, source, group, box, moveItem(box, delta, delta));
    const members = [...group.items.map(i => source.items[i]), ...group.strokes.map(i => source.strokes[i])];
    const ids = new Map(members.map(value => [value.id, identifier()]));
    const groups = new Map(members.filter(value => value.group_id).map(value => [value.group_id, identifier()]));
    checkpoint(structuredClone(drawing));
    for (const index of group.items) {
        const item = copied.items[index]; item.id = ids.get(source.items[index].id);
        if (item.kind === 'fill') item.parent_id = ids.get(item.parent_id) || null;
        if (item.group_id) item.group_id = groups.get(item.group_id);
        drawing.items.push(item);
    }
    const firstStroke = drawing.strokes.length;
    for (const index of group.strokes) {
        const stroke = copied.strokes[index]; stroke.id = ids.get(stroke.id);
        if (stroke.group_id) stroke.group_id = groups.get(stroke.group_id);
        drawing.strokes.push(stroke);
    }
    selected = selection.type === 'item' ? {type: 'item', id: ids.get(selection.id)} : {type: 'stroke', index: firstStroke + group.strokes.indexOf(selection.index)};
    record('selection.pasted', {item_count: group.items.length, stroke_count: group.strokes.length});
    chooseTool('marquee'); redraw(); void flushEvents();
}
$('copy').addEventListener('click', copySelection); $('paste').addEventListener('click', pasteSelection);
$('group-selection').addEventListener('click', () => {
    if (!boxed || busy || gesture) return;
    checkpoint(structuredClone(drawing));
    const groupId = identifier(), group = boxed;
    group.items.forEach(i => { drawing.items[i].group_id = groupId; });
    group.strokes.forEach(i => { drawing.strokes[i].group_id = groupId; });
    selected = group.strokes.length ? {type: 'stroke', index: group.strokes[0]} : {type: 'item', id: drawing.items[group.items[0]].id};
    boxed = null; marquee = null;
    record('selection.grouped', {item_count: group.items.length, stroke_count: group.strokes.length}); redraw();
    message('그룹화했어요. 선택한 요소를 함께 이동·크기 조절할 수 있습니다.');
});
$('ungroup-selection').addEventListener('click', () => {
    const value = selectedValue(); if (!value?.group_id || busy || gesture) return;
    const groupId = value.group_id;
    checkpoint(structuredClone(drawing));
    for (const member of [...drawing.items, ...drawing.strokes]) if (member.group_id === groupId) delete member.group_id;
    record('selection.ungrouped'); redraw();
});
$('apply-crop').addEventListener('click', () => {
    if (!cropSelection || busy || gesture) return;
    if ((drawing.items.length || drawing.strokes.length) && !confirm('사진을 자르면 현재 스케치가 비워집니다. 선택한 사진 영역으로 새로 시작할까요?')) return;
    const region = structuredClone(cropSelection);
    void operation(async () => {
        const next = await api('/api/projects/' + project.id + '/crop/', region);
        await changePhoto(next); mode = 'pen';
    }).then(() => { if (!busy && mode === 'pen') chooseTool('pen'); });
});
$('cancel-crop').addEventListener('click', () => { cropSelection = null; chooseTool('pen'); redraw(); });
function history(backward) {
    if (gesture || busy) return; commitText(); finishRequirements();
    const source = backward ? past : future, destination = backward ? future : past; if (!source.length) return;
    void operation(async () => {
        const next = source.at(-1), current = snapshot();
        const photoChanged = next.project.id !== project.id;
        if (photoChanged) {
            const latest = await api('/api/projects/' + next.project.id + '/');
            if (latest.locked) throw new Error('이전 작업에서 이미 검색을 완료했습니다. 결과의 스케치 수정하기를 사용해 주세요.');
            if (latest.revision !== next.project.revision) throw new Error('이전 작업이 다른 탭에서 변경되어 복원할 수 없습니다. 현재 작업과 실행 취소 기록은 유지합니다.');
            await showProject({...latest, document: next.drawing});
        } else drawing = structuredClone(next.drawing);
        project.request_context = structuredClone(next.project.request_context || {}); restoreRequirements();
        destination.push(current); if (destination.length > 20) destination.shift(); source.pop();
        selected = null; boxed = null; marquee = null; changed = true; invalidateResult();
        record(backward ? 'history.undo' : 'history.redo', {photo_changed: photoChanged, previous_project_id: current.project.id});
        redraw(); message(backward ? '이전 작업 상태로 되돌렸어요.' : '취소했던 작업을 다시 적용했어요.');
    });
}
$('undo').addEventListener('click', () => history(true)); $('redo').addEventListener('click', () => history(false));
$('remove-item').addEventListener('click', () => {
    if (!selectedValue() || gesture || busy) return; checkpoint(structuredClone(drawing));
    record('annotation.removed', selected.type === 'item' ? {item: selectedValue()} : {stroke_index: selected.index});
    const group = selectedGroup(drawing, selectedValue());
    drawing.items = drawing.items.filter((_, index) => !group.items.includes(index));
    drawing.strokes = drawing.strokes.filter((_, index) => !group.strokes.includes(index));
    selected = null; redraw();
});
$('clear').addEventListener('click', () => {
    if ((!drawing.strokes.length && !drawing.items.length) || busy || gesture) return;
    if (!confirm('그림·텍스트·도형 표시를 모두 지울까요?')) return;
    checkpoint(structuredClone(drawing)); drawing = {strokes: [], items: []}; selected = null; record('sketch.cleared'); redraw();
});
document.addEventListener('keydown', event => {
    if (event.target.id === 'text-requirements' && (event.ctrlKey || event.metaKey) && ['z', 'y'].includes(event.key.toLowerCase())) {
        event.preventDefault(); history(event.key.toLowerCase() === 'z' && !event.shiftKey); return;
    }
    if (event.target.matches('textarea, input:not([type="range"]):not([type="color"]), select') || event.target.isContentEditable) return;
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'z') { event.preventDefault(); history(!event.shiftKey); }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'y') { event.preventDefault(); history(false); }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'c') { event.preventDefault(); copySelection(); }
    if ((event.ctrlKey || event.metaKey) && event.key.toLowerCase() === 'v') { event.preventDefault(); pasteSelection(); }
    if (event.target.matches('input')) return;
    if ((event.key === 'Delete' || event.key === 'Backspace') && selected) { event.preventDefault(); $('remove-item').click(); }
    if (selected && event.key.startsWith('Arrow') && !busy && !gesture) {
        event.preventDefault(); const before = structuredClone(drawing), box = selectedBox(), delta = event.shiftKey ? .02 : .005;
        const next = moveItem(box, event.key === 'ArrowRight' ? delta : event.key === 'ArrowLeft' ? -delta : 0, event.key === 'ArrowDown' ? delta : event.key === 'ArrowUp' ? -delta : 0);
        checkpoint(before);
        transformGroup(drawing, before, selectedGroup(before, selectedValue()), box, next);
        record('annotation.moved', selected.type === 'item' ? {item: selectedValue(), pointer_type: 'keyboard'} : {stroke_index: selected.index, ...next});
        redraw();
    }
});
async function operation(task) {
    if (busy || gesture) return; busy = true; surface.style.pointerEvents = 'none';
    for (const element of document.querySelectorAll('button, input, textarea')) element.disabled = true;
    try { await task(); } catch (error) { message(error.message, true); if (/10\s?MB/.test(error.message)) alert('10MB 이하의 파일을 업로드해주세요.'); }
    finally {
        busy = false; surface.style.pointerEvents = '';
        for (const element of document.querySelectorAll('button, input, textarea')) element.disabled = false;
        updateControls();
        saveHistory();
    }
}
async function loadImage(url) {
    const image = new Image();
    await new Promise((resolve, reject) => { image.onload = resolve; image.onerror = () => reject(new Error('이미지를 불러오지 못했어요. 다시 시도해 주세요.')); image.src = url; });
    return image;
}
async function showProject(next) {
    if (next.photo_url) await loadImage(next.photo_url);
    collector.projectChanging(next.id);
    project = next; drawing = structuredClone(next.document); selected = null; changed = false;
    restoreRequirements(); document.body.classList.toggle('has-photo', !!next.photo_url);
    for (const stroke of drawing.strokes) stroke.id ||= identifier();
    boxed = null; marquee = null; thicknessBefore = null;
    cropSelection = null; fillCanvases.clear();
    tracker.setProject(project.id);
    window.history.replaceState(null, '', project.editor_url);
    invalidateResult(); $('room-photo').hidden = !next.photo_url;
    if (next.photo_url) $('room-photo').src = next.photo_url; else $('room-photo').removeAttribute('src');
    stage.style.aspectRatio = next.width + ' / ' + next.height;
    stage.style.setProperty('--scene-ratio', next.width / next.height);
    mode = 'pen';
    for (const button of document.querySelectorAll('[data-tool]')) button.setAttribute('aria-pressed', String(button.dataset.tool === mode));
    surface.style.cursor = 'none';
    resizeCanvases(); message('사진 위에 원하는 내용을 그리거나 텍스트·도형으로 표시하세요.');
    collector.projectReady();
}
async function changePhoto(next) {
    const previous = snapshot();
    await showProject({...next, request_context: structuredClone(project.request_context || {})});
    checkpoint(previous.drawing, previous.project); updateControls();
}
async function newProject(file, dropped = false) {
    if (changed && !confirm('새 사진으로 바꿀까요? 현재 스케치는 비워지며 실행 취소로 복구할 수 있습니다.')) return;
    const form = new FormData(); form.set('title', '나의 공간 스케치'); form.set('source_project_id', project.id);
    if (file) {
        if (file.size > 10 * 1024 * 1024) throw new Error('10MB 이하의 파일을 업로드해주세요.');
        form.set('photo', file); record(dropped ? 'photo.dropped' : 'photo.selected', {bytes: file.size, type: file.type.slice(0, 100)});
    }
    message('사진을 준비하고 있어요.'); await changePhoto(await api('/api/projects/', form));
}
$('upload-empty').addEventListener('click', () => $('photo-file').click());
$('sample-empty').addEventListener('click', () => void operation(async () => {
    const response = await fetch('/static/sketch/assets/room-empty.png', {signal: AbortSignal.timeout(15000)});
    if (!response.ok) throw new Error('예시 공간을 불러오지 못했습니다.');
    await newProject(new File([await response.blob()], 'roompick-example.png', {type: 'image/png'}));
}));
function showEditorTab(tab, focus = false) {
    document.body.dataset.editorTab = tab;
    for (const name of ['sketch', 'description']) {
        const button = $('tab-' + name), active = name === tab;
        button.setAttribute('aria-selected', String(active)); button.tabIndex = active ? 0 : -1;
    }
    resizeCanvases(); if (focus) $('tab-' + tab).focus();
}
for (const name of ['sketch', 'description']) {
    $('tab-' + name).addEventListener('click', () => showEditorTab(name));
    $('tab-' + name).addEventListener('keydown', event => {
        if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return;
        event.preventDefault(); event.stopPropagation();
        showEditorTab(event.key === 'Home' ? 'sketch' : event.key === 'End' ? 'description' : name === 'sketch' ? 'description' : 'sketch', true);
    });
}
showEditorTab('sketch');
$('photo-file').addEventListener('change', async event => { const file = event.target.files[0]; if (file) await operation(() => newProject(file)); event.target.value = ''; });
let dragDepth = 0; const dropZone = $('drop-zone');
function clearDrop() { dragDepth = 0; dropZone.classList.remove('drag-over'); $('drop-hint').hidden = true; }
for (const name of ['dragenter', 'dragover', 'dragleave', 'drop']) document.addEventListener(name, event => { if (event.dataTransfer?.types.includes('Files')) event.preventDefault(); });
dropZone.addEventListener('dragenter', event => { if (!event.dataTransfer?.types.includes('Files') || busy) return; event.preventDefault(); dragDepth++; dropZone.classList.add('drag-over'); $('drop-hint').hidden = false; });
dropZone.addEventListener('dragover', event => { event.preventDefault(); if (event.dataTransfer) event.dataTransfer.dropEffect = 'copy'; });
dropZone.addEventListener('dragleave', () => { if (--dragDepth <= 0) clearDrop(); });
dropZone.addEventListener('drop', event => {
    event.preventDefault(); clearDrop(); const files = event.dataTransfer?.files;
    if (files?.length !== 1) { message('사진 파일 하나를 끌어 놓아 주세요.', true); return; }
    void operation(() => newProject(files[0], true));
});
function searchProducts() {
    commitText(); finishRequirements();
    if (busy || gesture) return;
    if (!project.photo_url) { showEditorTab('sketch'); message('공간 사진을 먼저 업로드하거나 예시 공간을 선택해 주세요.', true); $('upload-empty').focus(); return; }
    if (!drawing.items.length && !drawing.strokes.length && !$('text-requirements').value.trim()) {
        showEditorTab('description'); message('스케치하거나, 원하는 변화를 글로 설명해 주세요.', true); $('text-requirements').focus(); return;
    }
    return operation(async () => {
        record('search.clicked', {mode: 'mock'}); message('사진과 아이디어로 공간을 만들고 있어요.'); requestId ||= identifier();
        const dialog = $('generation-progress'); dialog.showModal();
        try {
            const next = await platform.generate(project.id, {document: drawing, revision: project.revision, request_id: requestId,
                text_requirements: $('text-requirements').value.trim(), style_preferences: project.request_context?.style_preferences || []}, {
                onStatus: status => { $('generation-status').textContent = status === 'QUEUED' ? '공간 생성 순서를 기다리고 있어요.' : '입력한 아이디어를 공간에 반영하고 있어요.'; },
            });
            project.revision = next.revision; project.locked = true;
            collector.submit(next.result_id); void flushEvents(); window.location.assign(next.page_url);
        } finally { dialog.close(); }
    });
}
$('generation-progress').addEventListener('cancel', event => event.preventDefault());
$('search').addEventListener('click', searchProducts);
$('search-top').addEventListener('click', searchProducts);
$('search-mobile').addEventListener('click', searchProducts);
document.addEventListener('click', event => { const button = event.target.closest('button'); if (button?.id) record('button.clicked', {button: button.id}); });
resizeCanvases(); updateControls(); surface.style.cursor = 'none';
void operation(async () => {
    await showProject(savedHistory ? {...project, document: savedHistory.drawing, request_context: savedHistory.request_context || project.request_context} : project);
    if (savedHistory) {
        past = savedHistory.past.slice(-20); future = savedHistory.future.slice(-20); changed = savedHistory.changed;
        savedHistory = null; redraw();
    }
    if (!project.photo_url) message('사진을 업로드하거나 끌어 놓아 시작하세요.');
});
void flushEvents();

window.addEventListener('pagehide', finishRequirements);



initAccountState({projectId: () => project.id, beforeLogin: async () => {
    if (busy || gesture) throw new Error('진행 중인 작업을 마친 후 다시 시도해 주세요.');
    commitText(); finishRequirements();
    const saved = await api(`/api/projects/${project.id}/`, {title:project.title, document:drawing, revision:project.revision, events:[], request_context:project.request_context});
    project.revision = saved.revision; saveHistory();
    void flushEvents();
}, onChange: (state, {changed:actorChanged,denied}) => {
    collector.setAccount(state, denied);
    if (denied) return;
    if (actorChanged) { historyActor=state.actor_key; past=[];future=[];saveHistory();updateControls(); }
}});
