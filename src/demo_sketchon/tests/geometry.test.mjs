import test from 'node:test';
import assert from 'node:assert/strict';
import {normalizePoint, moveItem, resizeItem, strokeBounds, transformStroke, alphaBounds} from '../sketch/static/sketch/geometry.mjs';

test('placement bounds use visible pixels including the final pixel edge', () => {
    const pixels = new Uint8ClampedArray(10 * 8 * 4);
    for (let y = 2; y <= 5; y++) for (let x = 3; x <= 7; x++) pixels[(y * 10 + x) * 4 + 3] = 255;
    pixels[3] = 15; // Transparent/near-transparent annotation residue is ignored.
    assert.deepEqual(alphaBounds(pixels, 10, 8), {x_min:.3, y_min:.25, x_max:.8, y_max:.75});
    for (let x = 3; x <= 7; x++) pixels[(2 * 10 + x) * 4 + 3] = 0;
    assert.equal(alphaBounds(pixels, 10, 8).y_min, 3 / 8);
});

test('fully erased artwork supplies no placement override', () => {
    assert.equal(alphaBounds(new Uint8ClampedArray(16 * 4), 4, 4), null);
});

test('photo center stays normalized across desktop and phone layouts', () => {
    assert.deepEqual(normalizePoint(510, 320, {left: 10, top: 20, width: 1000, height: 600}), {x: .5, y: .5});
    assert.deepEqual(normalizePoint(160, 110, {left: 10, top: 20, width: 300, height: 180}), {x: .5, y: .5});
    assert.deepEqual(normalizePoint(-10, 999, {left: 0, top: 0, width: 100, height: 100}), {x: 0, y: 1});
});
test('furniture stays inside photo when dragged past an edge', () => {
    const item = {x: .4, y: .4, width: .2, height: .3};
    assert.deepEqual(moveItem(item, 1, -1), {...item, x: .8, y: 0});
});
test('resize respects minimum and photo boundary', () => {
    const item = {x: .8, y: .8, width: .1, height: .1};
    const large = resizeItem(item, 1, 1);
    assert.ok(Math.abs(large.x + large.width - 1) < 1e-10);
    assert.ok(Math.abs(large.y + large.height - 1) < 1e-10);
    const result = resizeItem(item, -.5, -.5);
    assert.equal(result.width, .04);
    assert.equal(result.height, .04);
});

test('freehand drawing moves and resizes with normalized point coordinates', () => {
    const stroke = {width: .01, points: [{x: .2, y: .3, pressure: .5}, {x: .4, y: .5, pressure: .7}]};
    const bounds = strokeBounds(stroke);
    const moved = transformStroke(stroke, bounds, moveItem(bounds, .2, .1));
    assert.ok(Math.abs(moved.points[0].x - .4) < 1e-10);
    assert.ok(Math.abs(moved.points[1].y - .6) < 1e-10);
    assert.equal(moved.points[1].pressure, .7);
});
