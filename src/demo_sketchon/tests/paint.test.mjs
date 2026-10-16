import test from 'node:test';
import assert from 'node:assert/strict';
import {floodRegion, outlineHit, selectedGroup, transformGroup, strokeHit, strokesTouch, fillHit, boxGroup} from '../sketch/static/sketch/paint.mjs';

test('fill stays inside rectangle and cannot cross dividing line', () => {
    const width = 11, height = 11, pixels = new Uint8ClampedArray(width * height * 4);
    const boundary = (x, y) => { pixels[(y * width + x) * 4 + 3] = 255; };
    for (let i = 1; i <= 9; i++) { boundary(i, 1); boundary(i, 9); boundary(1, i); boundary(9, i); boundary(5, i); }
    const left = floodRegion(pixels, width, height, 3, 4);
    assert.deepEqual(left.bounds, {x: 2, y: 2, width: 3, height: 7});
    assert.equal(left.runs.reduce((n, run) => n + run[2] - run[1] + 1, 0), 21);
    const right = floodRegion(pixels, width, height, 7, 4);
    assert.deepEqual(right.bounds, {x: 6, y: 2, width: 3, height: 7});
});
test('only shape outline is hit; empty interior can contain another shape', () => {
    const item = {kind: 'rectangle', x: .1, y: .1, width: .6, height: .6, stroke_width: .004};
    const rect = {width: 1000, height: 700};
    assert.equal(outlineHit(item, {x: .4, y: .4}, rect), false);
    assert.equal(outlineHit(item, {x: .1, y: .4}, rect), true);
    assert.equal(outlineHit({...item, kind: 'ellipse'}, {x: .4, y: .4}, rect), false);
});

test('arrow head outline can be selected independently of its shaft', () => {
    const item = {kind: 'arrow', x: .1, y: .1, width: .6, height: .001, stroke_width: .002, flip_x: false, flip_y: false};
    const rect = {width: 1000, height: 700}, angle = Math.atan2(.7, 600), size = 35;
    const point = {x: .7 - size * Math.cos(angle - .5) / 1000, y: .101 - size * Math.sin(angle - .5) / 700};
    assert.equal(outlineHit(item, point, rect), true);
});
test('only attached fill moves with outer shape; interior objects stay independent', () => {
    const shape = {id: 'outer', kind: 'rectangle', x: .1, y: .1, width: .4, height: .4};
    const doc = {strokes: [], items: [shape, {id: 'fill', kind: 'fill', parent_id: 'outer', x: .11, y: .11, width: .18, height: .38},
        {id: 'line', kind: 'line', x: .3, y: .1, width: .001, height: .4}, {id: 'inner', kind: 'ellipse', x: .2, y: .2, width: .1, height: .1}]};
    const group = selectedGroup(doc, shape);
    const result = structuredClone(doc);
    transformGroup(result, doc, group, shape, {...shape, x: .3, width: .6});
    assert.ok(Math.abs(result.items[1].x - .315) < 1e-10);
    assert.equal(result.items[2].x, .3);
    assert.equal(result.items[3].width, .1);
});

test('explicit groups and connected pen strokes include fills but not unrelated strokes', () => {
    const a = {id: 'a', tool: 'pen', width: .01, group_id: 'g', points: [{x: .1, y: .1}, {x: .4, y: .4}]};
    const b = {...a, id: 'b', points: [{x: .1, y: .4}, {x: .4, y: .1}]};
    const c = {...a, id: 'c', group_id: undefined, points: [{x: .2, y: .2}, {x: .3, y: .2}]};
    const doc = {strokes: [a, b, c], items: [{id: 'fill', parent_id: 'a', kind: 'fill', x: .2, y: .2, width: .1, height: .1}]};
    assert.deepEqual(selectedGroup(doc, a), {items: [0], strokes: [0, 1]});
    assert.deepEqual(selectedGroup(doc, doc.items[0]), {items: [0], strokes: [0, 1]});
    assert.equal(strokesTouch(a, b, {width: 1000, height: 700}), true);
    assert.equal(strokesTouch(a, {...b, points: [{x: .7, y: .7}]}, {width: 1000, height: 700}), false);
    assert.equal(strokeHit(a, {x: .25, y: .3}, {width: 1000, height: 700}), false);
    assert.equal(strokeHit(a, {x: .25, y: .25}, {width: 1000, height: 700}), true);
    assert.deepEqual(boxGroup(doc, {x: .05, y: .05, width: .4, height: .4}).strokes, [0, 1, 2]);
});

test('fill hit follows mask holes and partial erasure; masks follow element movement', () => {
    const fill = {id: 'f', kind: 'fill', x: .1, y: .1, width: .2, height: .2, mask_width: 2, mask_height: 2, runs: [[0, 0, 0], [1, 0, 1]],
        cuts: [{tool: 'eraser', width: .02, points: [{x: .15, y: .15}]}]};
    assert.equal(fillHit(fill, {x: .25, y: .15}), false);
    assert.equal(fillHit(fill, {x: .15, y: .25}), true);
    assert.equal(fillHit(fill, {x: .15, y: .15}, {width: 1000, height: 700}), false);
    const doc = {strokes: [], items: [fill]}, after = structuredClone(doc);
    transformGroup(after, doc, {items: [0], strokes: []}, fill, {...fill, x: .2});
    assert.ok(Math.abs(after.items[0].cuts[0].points[0].x - .25) < 1e-10);
});

test('moving a divider on the outer edge keeps every element in photo bounds', () => {
    const shape = {id: 'outer', kind: 'rectangle', x: .1, y: .1, width: .4, height: .4};
    const doc = {strokes: [], items: [shape, {id: 'edge', kind: 'line', x: .5, y: .1, width: .001, height: .4}]};
    const result = structuredClone(doc);
    transformGroup(result, doc, selectedGroup(doc, shape), shape, {...shape, x: .6});
    assert.ok(result.items.every(item => item.x + item.width <= 1 && item.y + item.height <= 1));
});
