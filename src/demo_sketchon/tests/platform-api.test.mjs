import test from 'node:test';
import assert from 'node:assert/strict';
import {createPlatformAPI} from '../sketch/static/sketch/platform-api.mjs';
import {createTracker} from '../sketch/static/sketch/events.mjs';
import {createDurableQueue} from '../sketch/static/sketch/durable-queue.mjs';

const storage = new Map();
globalThis.localStorage = {get length() {return storage.size;}, key: i => [...storage.keys()][i] ?? null, getItem: key => storage.get(key) ?? null, setItem: (key, value) => storage.set(key, value), removeItem: key => storage.delete(key)};
globalThis.document = {getElementById: () => null, querySelector: () => ({value: 'test-csrf'})};
globalThis.window = {addEventListener() {}};
const nativeInterval = globalThis.setInterval;
globalThis.setInterval = () => 0;
test.after(() => { globalThis.setInterval = nativeInterval; });
const response = (status, data) => ({ok: status >= 200 && status < 300, status, headers: {get: () => 'application/json'}, json: async () => data});

test('API uses separate scene and recommendation routes and retains the same request ID', async () => {
    const calls = [];
    const api = createPlatformAPI(async (path, body) => { calls.push({path, body}); return {status: 'SUCCEEDED'}; });
    const input = {request_id: 'same-id'};
    await api.generate('project', input); await api.recommend('object', input); await api.recommend('object', input);
    assert.equal(calls[0].path, '/api/projects/project/scene-generations/');
    assert.equal(calls[1].path, '/api/scene-objects/object/recommendations/');
    assert.equal(calls[2].body.request_id, calls[1].body.request_id);
});

test('job failures cannot become successful recommendation responses', async () => {
    const api = createPlatformAPI(async () => ({job_id: 'job', status: 'FAILED', error: 'provider unavailable'}));
    await assert.rejects(() => api.recommend('object', {}), /provider unavailable/);
});

test('a hanging fetch is aborted instead of freezing callers', async () => {
    globalThis.fetch = async (path, options) => new Promise((resolve, reject) => {
        options.signal.addEventListener('abort', () => reject(new DOMException('Aborted', 'AbortError')), {once: true});
    });
    const tracker = createTracker('project', 'timeout-visitor');
    await assert.rejects(() => tracker.api('/api/test/', {}, {timeoutMs: 20}), /응답이 지연/);
});

test('a bad event is preserved separately while later valid events are delivered', async () => {
    const received = [];
    globalThis.fetch = async (path, options) => {
        const rows = JSON.parse(options.body).events;
        if (rows.some(row => row.event_type === 'invalid')) return response(400, {error: 'invalid event'});
        received.push(...rows); return response(200, {accepted: rows.length});
    };
    const tracker = createTracker('project', 'isolation-visitor');
    tracker.record('invalid'); tracker.record('button.clicked', {button: 'ok'});
    await tracker.flushEvents();
    assert.equal(received.length, 1);
    assert.equal(createDurableQueue('roompick.events.isolation-visitor').rows().length, 0);
    assert.equal([...storage.keys()].filter(key=>key.startsWith('roompick.events.isolation-visitor.rejected.')).length, 1);
});

test('transient errors retain pending IDs for a later retry', async () => {
    let failed = true;
    globalThis.fetch = async () => failed ? response(503, {error: 'temporarily offline'}) : response(200, {accepted: 1});
    const tracker = createTracker('project', 'retry-visitor');
    const id = tracker.record('button.clicked'); await tracker.flushEvents();
    assert.equal(createDurableQueue('roompick.events.retry-visitor').rows()[0].event_id, id);
    failed = false; await tracker.flushEvents();
    assert.equal(createDurableQueue('roompick.events.retry-visitor').rows().length, 0);
});
