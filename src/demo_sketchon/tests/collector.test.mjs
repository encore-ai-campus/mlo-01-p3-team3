import test from 'node:test';
import assert from 'node:assert/strict';
import {createTracker} from '../sketch/static/sketch/events.mjs';

const module = await import('../sketch/static/sketch/collector.mjs').catch(() => ({}));
const rect = {left: 10, top: 20, width: 600, height: 400};
const empty = () => ({strokes: [], items: []});
const project = {id: '11111111-1111-4111-8111-111111111111', width: 1200, height: 800};
const event = (x, t, extra = {}) => ({clientX: rect.left + x * rect.width, clientY: 140, timeStamp: t, pointerType: 'mouse', pressure: 0, ...extra});

test('raw capture preserves outside positions, zero pressure and point time without changing drawing', () => {
    assert.equal(typeof module.SketchCapture, 'function', 'raw collector is not implemented');
    const capture = new module.SketchCapture(project, 'writer', empty(), rect, 2);
    const stroke = {id: crypto.randomUUID(), tool: 'pen', color: '#245847', width: .008, pointer_type: 'mouse', points: [{x: .2, y: .3, pressure: .5}]};
    const original = structuredClone(stroke);
    capture.begin(event(.2, 100), stroke, rect, 2);
    capture.move(event(1.04, 120), rect);
    capture.end(event(1.05, 125), 'up', rect);
    assert.deepEqual(stroke, original);
    const points = capture.sample.strokes[0].points;
    assert.equal(points[1].x, 1.04);
    assert.equal(points[1].t, 20);
    assert.equal(points[1].pressure, 0);
    assert.equal(points[1].in_bounds, false);
    assert.deepEqual(points.map(p => p.event_type), ['down', 'move', 'up']);
});

test('clear and undo keep raw stroke and patches restore the final document', () => {
    assert.equal(typeof module.SketchCapture, 'function');
    const capture = new module.SketchCapture(project, 'writer', empty(), rect, 1);
    const stroke = {id: crypto.randomUUID(), tool: 'pen', color: '#245847', width: .008, pointer_type: 'mouse', points: [{x: .2, y: .3, pressure: .5}]};
    capture.begin(event(.2, 100), stroke, rect, 1);
    capture.end(event(.2, 110), 'up', rect);
    const drawing = {strokes: [stroke], items: []};
    capture.operation('stroke.completed', drawing, 110);
    capture.operation('sketch.cleared', empty(), 120);
    capture.operation('history.undo', drawing, 130);
    assert.equal(capture.sample.strokes.length, 1);
    assert.deepEqual(capture.sample.operations[1].target_stroke_ids, [stroke.id]);
    assert.deepEqual(capture.sample.final_active_stroke_ids, [stroke.id]);
    assert.equal(capture.sample.undo_count, 1);
    assert.deepEqual(capture.sample.final_document, drawing);
});

test('coalesced and synthetic cancel points retain their source and pen tilt', () => {
    assert.equal(typeof module.SketchCapture, 'function');
    const capture = new module.SketchCapture(project, 'writer', empty(), rect, 1);
    const stroke = {tool: 'eraser', color: '#245847', width: .035};
    capture.begin(event(.2, 100, {pointerType: 'pen', pressure: .6, tiltX: 15, tiltY: -20}), stroke, rect, 1);
    capture.move(event(.3, 120, {getCoalescedEvents: () => [event(.25, 110)]}), rect);
    capture.end(null, 'lost_capture', rect);
    const raw = capture.sample.strokes[0];
    assert.equal(raw.tool, 'eraser');
    assert.deepEqual(raw.points.map(p => p.source), ['native', 'coalesced', 'native', 'synthetic']);
    assert.equal(raw.points[0].tilt_x, 15);
    assert.equal(raw.end_reason, 'lost_capture');
    assert.deepEqual(capture.sample.final_document, empty());
});

test('raw collection stops explicitly before an oversized history can poison delivery', () => {
    assert.equal(typeof module.SketchCapture, 'function');
    const capture = new module.SketchCapture(project, 'writer', empty(), rect, 1);
    for (let i = 0; i < 1001; i++) capture.operation('button.clicked', empty(), i);
    assert.equal(capture.sample.capture_status, 'limited');
    assert.ok(capture.sample.operations.length < 1000);
    assert.deepEqual(capture.sample.final_document, empty());
});

test('partial erase operation explicitly links its raw path to the affected document strokes', () => {
    const capture = new module.SketchCapture(project, 'writer', empty(), rect, 1);
    capture.begin(event(.2, 100), {tool: 'eraser', color: '#245847', width: .035}, rect, 1);
    capture.end(event(.3, 110), 'up', rect);
    const source = capture.sample.strokes[0].stroke_id;
    capture.operation('stroke.completed', empty(), 110, null, source);
    assert.equal(capture.sample.operations[0].source_stroke_id, source);
});

test('HTML 404 keeps its status so rejected samples cannot block the upload queue', async () => {
    const original = {window: globalThis.window, document: globalThis.document, localStorage: globalThis.localStorage, fetch: globalThis.fetch, setInterval: globalThis.setInterval};
    try {
        globalThis.window = {addEventListener() {}};
        globalThis.document = {querySelector: () => ({value: 'csrf'})};
        globalThis.localStorage = {getItem: () => null};
        globalThis.setInterval = () => 0;
        globalThis.fetch = async () => new Response('<html>Not Found</html>', {status: 404, headers: {'Content-Type': 'text/html'}});
        const tracker = createTracker(project.id, 'writer');
        await assert.rejects(tracker.api('/missing/', {}), error => error.status === 404);
    } finally {
        for (const [key, value] of Object.entries(original)) {
            if (value === undefined) delete globalThis[key]; else globalThis[key] = value;
        }
    }
});
