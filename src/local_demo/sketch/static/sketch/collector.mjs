import {identifier} from './events.mjs';

const clone = value => structuredClone(value);
const byteSize = value => new TextEncoder().encode(JSON.stringify(value)).length;
// shortcut: one capture stops near 3MB; add linked segments for sustained long drawing sessions.
const CAPTURE_BYTES = 3 * 1024 * 1024;

function documentPatch(before, after) {
    const patch = {};
    for (const kind of ['strokes', 'items']) {
        if (JSON.stringify(before[kind]) === JSON.stringify(after[kind])) continue;
        const old = new Map(before[kind].map(row => [row.id, row]));
        const current = new Set(after[kind].map(row => row.id));
        patch[kind] = {
            upsert: after[kind].filter(row => JSON.stringify(old.get(row.id)) !== JSON.stringify(row)).map(clone),
            remove: before[kind].filter(row => !current.has(row.id)).map(row => row.id),
            order: after[kind].map(row => row.id),
        };
    }
    return patch;
}

export class SketchCapture {
    constructor(project, writer, document, rect, dpr) {
        this.origin = null;
        this.active = null;
        this.limited = false;
        this.pointCount = 0;
        this.sample = {
            schema_version: 'roompick-sketch-1.0.0', sample_id: identifier(), project_id: project.id, writer_id: writer,
            created_at: new Date().toISOString(), label_id: null, label_status: 'unverified', quality_status: 'pending',
            coordinate_system: 'normalized_xy_top_left_down', time_origin: 'first_pointerdown_ms',
            canvas_width: rect.width, canvas_height: rect.height, device_pixel_ratio: dpr,
            project_width: project.width, project_height: project.height,
            strokes: [], operations: [], initial_document: clone(document), final_document: clone(document),
            final_active_stroke_ids: document.strokes.filter(row => row.tool === 'pen').map(row => row.id),
            duration_ms: 0, undo_count: 0, submission_reason: 'page_exit', result_id: null, capture_status: 'complete',
        };
        this.bytes = byteSize(this.sample);
        if (this.bytes > CAPTURE_BYTES) throw new Error('기존 문서가 원본 수집 한도를 초과했습니다. 편집 기능은 계속 사용할 수 있습니다.');
    }

    relative(timestamp) { return this.origin === null ? 0 : Math.max(0, timestamp - this.origin); }
    stamp(t) { this.sample.duration_ms = Math.max(this.sample.duration_ms, t); }

    point(event, kind, rect, source = 'native') {
        const x = (event.clientX - rect.left) / rect.width, y = (event.clientY - rect.top) / rect.height;
        const point = {x, y, t: this.relative(event.timeStamp), event_type: kind, source,
            in_bounds: x >= 0 && x <= 1 && y >= 0 && y <= 1};
        if (Number.isFinite(event.pressure)) point.pressure = event.pressure;
        if (event.pointerType === 'pen') {
            if (Number.isFinite(event.tiltX)) point.tilt_x = event.tiltX;
            if (Number.isFinite(event.tiltY)) point.tilt_y = event.tiltY;
        }
        if (rect.width !== this.active.canvas_width || rect.height !== this.active.canvas_height) {
            point.canvas_width = rect.width; point.canvas_height = rect.height;
        }
        return point;
    }

    begin(event, stroke, rect, dpr) {
        if (this.active || this.limited) return;
        if (this.sample.strokes.length >= 900 || this.bytes + 1000 > CAPTURE_BYTES) { this.limit(); return; }
        if (this.origin === null) {
            this.origin = event.timeStamp;
            Object.assign(this.sample, {canvas_width: rect.width, canvas_height: rect.height, device_pixel_ratio: dpr});
        }
        this.active = {stroke_id: stroke.id || identifier(), stroke_order: this.sample.strokes.length,
            tool: stroke.tool, ink_hex: stroke.color, stroke_width_css_px: stroke.width * rect.width,
            canvas_width: rect.width, canvas_height: rect.height, device_pixel_ratio: dpr,
            pointer_type: ['mouse', 'touch', 'pen'].includes(event.pointerType) ? event.pointerType : 'unknown',
            end_reason: 'in_progress', points: []};
        this.active.points.push(this.point(event, 'down', rect));
        this.sample.strokes.push(this.active);
        this.pointCount++; this.bytes += byteSize(this.active);
        this.stamp(this.active.points[0].t);
    }

    move(event, rect) {
        if (!this.active || this.limited) return;
        let coalesced = [];
        try { coalesced = event.getCoalescedEvents?.() || []; } catch {}
        const points = [...coalesced];
        if (!points.some(row => row.timeStamp === event.timeStamp && row.clientX === event.clientX && row.clientY === event.clientY)) points.push(event);
        points.sort((a, b) => a.timeStamp - b.timeStamp);
        for (const row of points) {
            const point = this.point(row, 'move', rect, row === event ? 'native' : 'coalesced');
            const last = this.active.points.at(-1);
            if (point.t < last.t || (point.t === last.t && point.x === last.x && point.y === last.y)) continue;
            const bytes = byteSize(point) + 1;
            if (this.pointCount >= 90000 || this.bytes + bytes + 1000 > CAPTURE_BYTES) { this.limit(); return; }
            this.active.points.push(point); this.stamp(point.t);
            this.pointCount++; this.bytes += bytes;
        }
    }

    end(event, reason, rect) {
        if (!this.active) return;
        const last = this.active.points.at(-1);
        const point = event ? this.point(event, reason === 'up' ? 'up' : 'cancel', rect) :
            {...last, t: Math.max(last.t, this.relative(performance.now())), event_type: 'cancel', source: 'synthetic'};
        point.t = Math.max(last.t, point.t);
        this.active.points.push(point); this.active.end_reason = reason;
        this.pointCount++; this.bytes += byteSize(point) + 1;
        this.stamp(point.t); this.active = null;
    }

    operation(type, document, timestamp = performance.now(), eventId = null, sourceStrokeId = null) {
        if (this.limited) return;
        const patch = documentPatch(this.sample.final_document, document);
        const t = Math.max(this.relative(timestamp), this.sample.operations.at(-1)?.t || 0);
        const operation = {op_id: identifier(), type, t, client_time: new Date().toISOString(),
            target_stroke_ids: [...(patch.strokes?.upsert.map(row => row.id) || []), ...(patch.strokes?.remove || [])],
            target_item_ids: [...(patch.items?.upsert.map(row => row.id) || []), ...(patch.items?.remove || [])]};
        if (Object.keys(patch).length) operation.document_patch = patch;
        if (eventId) operation.event_id = eventId;
        if (sourceStrokeId) operation.source_stroke_id = sourceStrokeId;
        const finalIds = document.strokes.filter(row => row.tool === 'pen').map(row => row.id);
        const bytes = byteSize({...this.sample, operations: [...this.sample.operations, operation], final_document: document, final_active_stroke_ids: finalIds});
        if (this.sample.operations.length >= 950 || bytes > CAPTURE_BYTES) { this.limit(); return; }
        this.sample.operations.push(operation);
        this.sample.final_document = clone(document);
        this.sample.final_active_stroke_ids = finalIds;
        this.sample.undo_count += type === 'history.undo' ? 1 : 0;
        this.stamp(t);
        this.bytes = bytes;
    }

    limit() {
        this.limited = true;
        this.sample.capture_status = 'limited';
        this.sample.submission_reason = 'capture_limit';
        if (this.active) this.end(null, 'limited');
    }
}

export function createCollector({getProject, getDocument, surface, writer, actor, api}) {
    const currentWriter = () => typeof writer === "function" ? writer() : writer;
    let actorKey = actor, suspended = false;
    async function permitted(sample) {
        if (suspended) return false;
        const status = await api('/api/account/status/?project_id=' + encodeURIComponent(sample.project_id));
        return !suspended && (!actorKey || status.actor_key === actorKey) && status.project_access === true;
    }
    const status = document.getElementById('collection-status');
    const show = text => { if (status) status.textContent = text; };
    const warn = error => show(`원본 수집 대기: ${error.message}`);
    const rect = () => surface.getBoundingClientRect();
    let capture = null, writes = Promise.resolve(), sending = false, haltedProject = null, endedSource = null;
    const pending = new Map();
    const durable = new Set();
    const failed = new Set(), owners = new Map();
    function own(sampleId) {
        if (!navigator.locks) return;
        const lifetime = new Promise(resolve => owners.set(sampleId, resolve));
        navigator.locks.request('roompick-raw-' + sampleId, () => lifetime).catch(warn);
    }
    function release(sampleId) { owners.get(sampleId)?.(); owners.delete(sampleId); }
    function recoverIfUnowned(sample, callback) {
        // shortcut: on LAN HTTP without Web Locks, retain drafts; add a server lease for automatic recovery there.
        if (!navigator.locks) { show('미제출 원본 초안 보관 중 · 자동 복구는 localhost/HTTPS에서 지원'); return Promise.resolve(); }
        return navigator.locks.request('roompick-raw-' + sample.sample_id, {ifAvailable: true}, lock => {
            if (lock) return callback();
        });
    }
    let tabId = identifier();
    try {
        tabId = sessionStorage.getItem('local_demo.raw.tab') || tabId;
        sessionStorage.setItem('local_demo.raw.tab', tabId);
    } catch {}
    const db = new Promise((resolve, reject) => {
        const request = indexedDB.open('local-demo-sketch-v1', 2);
        request.onupgradeneeded = () => {
            for (const store of ['drafts', 'outbox', 'failed']) {
                if (!request.result.objectStoreNames.contains(store)) request.result.createObjectStore(store, {keyPath: 'sample_id'});
            }
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
        request.onblocked = () => show('원본 저장소 갱신 대기 · 다른 스케치·온 탭을 닫거나 새로고침해 주세요.');
    });
    async function transaction(mode, stores, callback) {
        const database = await db;
        return new Promise((resolve, reject) => {
            const tx = database.transaction(stores, mode);
            const result = callback(tx);
            tx.oncomplete = () => resolve(Array.isArray(result) ? result.map(request => request.result) : result?.result);
            tx.onerror = tx.onabort = () => reject(tx.error || new Error('임시 저장이 중단되었습니다.'));
        });
    }
    function write(callback) {
        writes = writes.then(() => transaction('readwrite', ['drafts', 'outbox', 'failed'], callback))
            .then(() => true).catch(error => { warn(error); return false; });
        return writes;
    }
    function persist() {
        if (!capture || (!capture.sample.strokes.length && !capture.sample.operations.length)) return;
        const sample = clone(capture.sample);
        write(tx => tx.objectStore('drafts').put({sample_id: sample.sample_id, tab_id: tabId, sample}));
    }
    async function flush() {
        if (suspended || sending || !pending.size) return;
        sending = true;
        try {
            for (const [id, sample] of pending) {
                if (failed.has(id)) continue;
                try {
                    if (!await permitted(sample)) continue;
                    const response = await api(`/api/projects/${sample.project_id}/samples/`, sample);
                    if (response.sample_id !== id) throw new Error('서버의 샘플 ID가 다릅니다.');
                    await write(tx => { tx.objectStore('outbox').delete(id); tx.objectStore('failed').delete(id); tx.objectStore('drafts').delete(id); });
                    pending.delete(id);
                    durable.delete(id); release(id);
                    show(`원본 저장 완료 · ${id.slice(0, 8)} · 품질 ${response.quality_status}${sample.capture_status === 'limited' ? ' · 한도 이후 미수집' : ''}`);
                } catch (error) {
                    // A session may change between authorization and POST. Keep
                    // its outbox retryable instead of quarantining another user's Raw.
                    if (error.status === 404 && !await permitted(sample)) continue;
                    if (![400, 404, 409, 413, 415, 422].includes(error.status)) throw error;
                    failed.add(id);
                    await write(tx => tx.objectStore('failed').put({sample_id: id, status: error.status, error: error.message}));
                }
            }
        } catch (error) { warn(error); }
        finally {
            sending = false;
            if (failed.size) show(`원본 ${failed.size}개 검토 필요 · IndexedDB에 보관 · 다른 샘플은 계속 전송`);
        }
    }
    function enqueue(sample) {
        pending.set(sample.sample_id, sample);
        const saved = write(tx => {
            tx.objectStore('outbox').put(sample);
            tx.objectStore('drafts').delete(sample.sample_id);
        });
        saved.then(persisted => {
            if (persisted) { durable.add(sample.sample_id); release(sample.sample_id); }
            void flush();
        });
        return saved;
    }
    function finish(reason, resultId = null) {
        if (!capture || (!capture.sample.strokes.length && !capture.sample.operations.length)) return;
        if (capture.active) capture.end(null, 'interrupted', rect());
        else if (!capture.limited && JSON.stringify(capture.sample.final_document) !== JSON.stringify(getDocument())) capture.operation('view.changed', getDocument());
        const sample = clone(capture.sample);
        sample.submission_reason = capture.limited ? 'capture_limit' : reason; sample.result_id = resultId;
        capture = null; enqueue(sample);
    }
    // Collection failures only update their own status; existing editing and search continue.
    function safe(callback) {
        return (...args) => { try { return callback(...args); } catch (error) { warn(error); } };
    }
    function checkLimit() {
        if (!capture?.limited) return;
        haltedProject = capture.sample.project_id;
        finish('capture_limit');
        show('원본 수집 한도 도달 · 이전 범위 저장 · 이후 작업은 새 작업/새로고침 전까지 미수집');
    }
    const controller = {
        setAccount(state, denied = false) {
            suspended = denied;
            actorKey = state.actor_key;
            if (!suspended) void flush();
        },
        begin: safe((event, stroke) => { capture?.begin(event, stroke, rect(), window.devicePixelRatio || 1); checkLimit(); persist(); }),
        move: safe(event => { capture?.move(event, rect()); checkLimit(); }),
        end: safe((event, reason) => {
            endedSource = capture?.active?.stroke_id || null;
            capture?.end(event, reason, rect()); checkLimit(); persist();
            queueMicrotask(() => { endedSource = null; });
        }),
        operation: safe((type, eventId) => {
            const current = capture, stamp = performance.now();
            const source = ['stroke.completed', 'stroke.cancelled'].includes(type) ? endedSource : null;
            if (source) endedSource = null;
            queueMicrotask(safe(() => {
                if (!current || current !== capture || current.active) return;
                current.operation(type, getDocument(), stamp, eventId, source); checkLimit(); persist();
            }));
        }),
        projectChanging: safe(id => { if (capture && capture.sample.project_id !== id) finish('project_changed'); }),
        projectReady: safe(() => {
            if (haltedProject === getProject().id) return;
            haltedProject = null;
            if (!capture || (!capture.sample.strokes.length && !capture.sample.operations.length)) {
                if (capture) release(capture.sample.sample_id);
                capture = new SketchCapture(getProject(), currentWriter(), getDocument(), rect(), window.devicePixelRatio || 1);
                own(capture.sample.sample_id);
            }
        }),
        submit: safe((resultId) => finish('search', resultId)),
    };
    async function restore() {
        const rejected = await transaction('readonly', ['failed'], tx => tx.objectStore('failed').getAll());
        const rejectedIds = new Set(rejected.map(row => row.sample_id));
        const outbox = await transaction('readonly', ['outbox'], tx => tx.objectStore('outbox').getAll());
        for (const sample of outbox) if (sample.writer_id === currentWriter() && await permitted(sample)) {
            pending.set(sample.sample_id, sample);
            durable.add(sample.sample_id);
            if (rejectedIds.has(sample.sample_id)) failed.add(sample.sample_id);
        }
        const drafts = await transaction('readonly', ['drafts'], tx => tx.objectStore('drafts').getAll());
        for (const row of drafts) {
            const sample = row.sample;
            if (row.tab_id !== tabId || sample.writer_id !== currentWriter() || sample.sample_id === capture?.sample.sample_id) continue;
            if (!await permitted(sample)) continue;
            await recoverIfUnowned(sample, async () => {
                const [draft, queued] = await transaction('readonly', ['drafts', 'outbox'], tx => [
                    tx.objectStore('drafts').get(sample.sample_id), tx.objectStore('outbox').get(sample.sample_id),
                ]);
                if (!draft || queued || draft.sample.writer_id !== currentWriter()) return;
                const recovered = draft.sample;
                for (const stroke of recovered.strokes) if (stroke.end_reason === 'in_progress') {
                    stroke.end_reason = 'interrupted';
                    stroke.points.push({...stroke.points.at(-1), event_type: 'cancel', source: 'synthetic'});
                }
                recovered.submission_reason = 'recovered';
                return enqueue(recovered);
            });
        }
        await flush();
    }
    restore().catch(warn);
    setInterval(safe(persist), 1000);
    setInterval(flush, 5000);
    window.addEventListener('online', flush);
    window.addEventListener('pagehide', safe(() => {
        finish('page_exit');
        // Unacknowledged samples stay in outbox, including after a successful beacon.
        for (const sample of pending.values()) {
            if (suspended) break;
            if (failed.has(sample.sample_id) || !durable.has(sample.sample_id)) continue;
            if (new TextEncoder().encode(JSON.stringify(sample)).length > 45000) continue;
            const form = new FormData();
            form.set('csrfmiddlewaretoken', document.querySelector('[name=csrfmiddlewaretoken]').value);
            form.set('sample', JSON.stringify(sample));
            navigator.sendBeacon(`/api/projects/${sample.project_id}/samples/`, form);
        }
    }));
    return controller;
}
