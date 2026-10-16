import {createDurableQueue} from './durable-queue.mjs';
const $ = id => document.getElementById(id);
export function identifier() {
    // randomUUID requires HTTPS; getRandomValues also works on a phone's LAN HTTP URL.
    const bytes = crypto.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 15) | 64;
    bytes[8] = (bytes[8] & 63) | 128;
    const hex = Array.from(bytes, byte => byte.toString(16).padStart(2, '0')).join('');
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function createTracker(projectId, visitor) {
    let project = {id: projectId};
    const actor = window.__sketchAccountState?.actor_key || JSON.parse(document.getElementById?.('account-initial')?.textContent || '{}').actor_key || visitor;
    const queueKey = 'roompick.events.' + actor;
    let sending = false;
    const queue = createDurableQueue(queueKey);
    // Migrate a previous version's pending rows once; identifiers still deduplicate on the server.
    try {
        const stored = JSON.parse(localStorage.getItem(queueKey) || '[]');
        if (Array.isArray(stored)) for (const row of stored) queue.add({id: row.event_id, ...row});
        localStorage.removeItem(queueKey);
    } catch {}

    function record(event_type, payload = {}) {
        const id = identifier();
        queue.add({id, project_id: project.id, event_id: id, event_type,
            payload: {...structuredClone(payload), client_time: new Date().toISOString()}});
        return id;
    }

    function csrf() { return document.querySelector('[name=csrfmiddlewaretoken]').value; }

    // Bound both response headers and body reads. Analytics never owns navigation.
    async function api(path, body, {timeoutMs = 30000, signal} = {}) {
        if (!path.startsWith('/') || path.startsWith('//')) throw new Error('Application 내부 API 주소가 필요합니다.');
        const epoch = window.__sketchAccountEpoch || 0;
        const controller = new AbortController();
        const cancel = () => controller.abort();
        signal?.addEventListener('abort', cancel, {once: true});
        if (signal?.aborted) controller.abort();
        const timer = setTimeout(cancel, timeoutMs);
        try {
            const options = {credentials: 'same-origin', signal: controller.signal, headers: {'Accept': 'application/json'}};
            if (body !== undefined) {
                options.method = 'POST';
                options.headers['X-CSRFToken'] = csrf();
                if (body instanceof FormData) options.body = body;
                else { options.headers['Content-Type'] = 'application/json'; options.body = JSON.stringify(body); }
            }
            const response = await fetch(path, options);
            if (!(response.headers.get('Content-Type') || '').includes('application/json')) {
                const error = new Error(`서버 응답을 확인해 주세요 (HTTP ${response.status}).`);
                error.status = response.status; throw error;
            }
            const data = await response.json();
            if (epoch !== (window.__sketchAccountEpoch || 0)) throw new Error("계정이 변경되었어요. 현재 계정에서 다시 시도해 주세요.");
            if (!response.ok) {
                const error = new Error(data.error || `요청 실패 (HTTP ${response.status}).`);
                error.status = response.status; throw error;
            }
            return data;
        } catch (error) {
            if (controller.signal.aborted) throw new Error('응답이 지연되고 있습니다. 입력은 유지되므로 다시 시도해 주세요.');
            throw error;
        } finally {
            clearTimeout(timer); signal?.removeEventListener('abort', cancel);
        }
    }

    function eventBatch(maxBytes = 4 * 1024 * 1024) {
        const pending = queue.rows();
        if (!pending.length) return [];
        const rows = [];
        let size = 100;
        for (const row of pending) {
            if (row.project_id !== pending[0].project_id || rows.length === 15) break;
            const bytes = new TextEncoder().encode(JSON.stringify(row)).length;
            if (size + bytes > maxBytes) break;
            rows.push(row); size += bytes;
        }
        return rows;
    }

    function quarantine(rows, reason) {
        for (const row of rows) queue.reject(row.id, reason);
    }
    async function send(batch) {
        return api(`/api/projects/${batch[0].project_id}/events/`, {events: batch.map(({id, project_id, ...event}) => event)}, {timeoutMs: 4000});
    }
    async function flushEvents() {
        if (sending || !queue.rows().length) return;
        sending = true;
        const permanent = new Set([400, 404, 409, 413, 422]);
        try {
            while (queue.rows().length) {
                const batch = eventBatch();
                if (!batch.length) { quarantine([queue.rows()[0]], '이벤트 크기 초과'); continue; }
                try {
                    await send(batch);
                    batch.forEach(row => queue.remove(row.id));
                } catch (error) {
                    if (!permanent.has(error.status)) throw error;
                    // A batch is atomic: isolate its invalid rows, deliver valid ones.
                    for (const row of batch) {
                        try { await send([row]); queue.remove(row.id); }
                        catch (single) { if (!permanent.has(single.status)) throw single; quarantine([row], single.message); }
                    }
                }
            }
        } catch (error) {
            const status = $('collection-status');
            if (status) status.textContent = '이용 기록은 임시 보관 중이며 연결 복구 후 다시 전송합니다.';
        } finally { sending = false; }
    }

    // Best effort on page exit; unacknowledged IDs remain in storage for deduplicated retry.
    window.addEventListener('pagehide', () => {
        const batch = eventBatch(45 * 1024);
        if (!batch.length) return;
        const form = new FormData();
        form.set('csrfmiddlewaretoken', csrf());
        form.set('events', JSON.stringify(batch.map(({id, project_id, ...event}) => event)));
        navigator.sendBeacon(`/api/projects/${batch[0].project_id}/events/`, form);
    });
    setInterval(flushEvents, 1500);
    window.addEventListener('online', flushEvents);



    return {api, record, flushEvents, setProject: id => { project = {id}; }};
}

