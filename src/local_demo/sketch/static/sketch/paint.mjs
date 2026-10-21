import {clamp, transformStroke, strokeBounds} from './geometry.mjs';

export function floodRegion(pixels, width, height, seedX, seedY) {
    if (!Number.isInteger(width) || !Number.isInteger(height) || width < 1 || height < 1 || width * height > 1440000 || pixels.length !== width * height * 4) throw new Error('채우기 이미지 크기가 올바르지 않습니다.');
    const x = Math.floor(clamp(seedX, 0, width - 1)), y = Math.floor(clamp(seedY, 0, height - 1));
    const seed = (y * width + x) * 4, target = pixels.slice(seed, seed + 4);
    const matches = index => {
        const offset = index * 4;
        if (target[3] <= 16) return pixels[offset + 3] <= 16;
        return Math.abs(pixels[offset + 3] - target[3]) <= 24 && [0, 1, 2].every(c => Math.abs(pixels[offset + c] - target[c]) <= 24);
    };
    const visited = new Uint8Array(width * height), queue = new Uint32Array(width * height);
    let read = 0, count = 1, left = x, right = x, top = y, bottom = y;
    queue[0] = y * width + x; visited[queue[0]] = 1;
    const enqueue = index => { if (!visited[index] && matches(index)) { visited[index] = 1; queue[count++] = index; } };
    while (read < count) {
        const index = queue[read++], px = index % width, py = Math.floor(index / width);
        left = Math.min(left, px); right = Math.max(right, px); top = Math.min(top, py); bottom = Math.max(bottom, py);
        if (px > 0) enqueue(index - 1); if (px + 1 < width) enqueue(index + 1);
        if (py > 0) enqueue(index - width); if (py + 1 < height) enqueue(index + width);
    }
    const runs = [];
    for (let row = top; row <= bottom; row++) {
        for (let col = left; col <= right; col++) {
            if (!visited[row * width + col]) continue;
            const start = col;
            while (col < right && visited[row * width + col + 1]) col++;
            runs.push([row - top, start - left, col - left]);
            if (runs.length > 100000) throw new Error('채우기 영역이 너무 복잡합니다. 경계를 단순하게 그려 주세요.');
        }
    }
    return {bounds: {x: left, y: top, width: right - left + 1, height: bottom - top + 1}, runs, target: Array.from(target)};
}

function distanceToLine(x, y, ax, ay, bx, by) {
    const dx = bx - ax, dy = by - ay, length = dx * dx + dy * dy;
    const t = length ? clamp(((x - ax) * dx + (y - ay) * dy) / length) : 0;
    return Math.hypot(x - ax - t * dx, y - ay - t * dy);
}
export function outlineHit(item, pos, rect, tolerance = 6) {
    if (item.kind === 'fill') return false;
    const x = (pos.x - item.x) * rect.width, y = (pos.y - item.y) * rect.height;
    const w = item.width * rect.width, h = item.height * rect.height;
    const t = tolerance + (item.stroke_width || 0) * rect.width / 2;
    if (item.kind === 'text' || item.kind === 'sticker' || !item.kind) return x >= 0 && y >= 0 && x <= w && y <= h;
    if (item.kind === 'rectangle') return Math.min(distanceToLine(x, y, 0, 0, w, 0), distanceToLine(x, y, w, 0, w, h), distanceToLine(x, y, w, h, 0, h), distanceToLine(x, y, 0, h, 0, 0)) <= t;
    if (item.kind === 'ellipse') {
        const rx = w / 2, ry = h / 2;
        return rx > 0 && ry > 0 && Math.abs(Math.hypot((x - rx) / rx, (y - ry) / ry) - 1) * Math.min(rx, ry) <= t;
    }
    const ax = item.flip_x ? w : 0, ay = item.flip_y ? h : 0, bx = item.flip_x ? 0 : w, by = item.flip_y ? 0 : h;
    if (distanceToLine(x, y, ax, ay, bx, by) <= t) return true;
    if (item.kind === 'arrow') {
        const angle = Math.atan2(by - ay, bx - ax), size = Math.min(Math.hypot(w, h) / 3, rect.width * .035);
        return [-.5, .5].some(turn => distanceToLine(x, y, bx, by, bx - size * Math.cos(angle + turn), by - size * Math.sin(angle + turn)) <= t);
    }
    return false;
}

export function strokeHit(stroke, pos, rect, tolerance = 6) {
    const points = stroke.points, radius = tolerance + stroke.width * rect.width / 2;
    return points.some((b, i) => {
        const a = points[Math.max(0, i - 1)];
        return distanceToLine(pos.x * rect.width, pos.y * rect.height, a.x * rect.width, a.y * rect.height, b.x * rect.width, b.y * rect.height) <= radius;
    });
}
export function cutHit(value, pos, rect) {
    return !!rect && (value.cuts || []).some(cut => strokeHit(cut, pos, rect, 0));
}
export function fillHit(item, pos, rect) {
    if (cutHit(item, pos, rect)) return false;
    const col = Math.floor((pos.x - item.x) / item.width * item.mask_width), row = Math.floor((pos.y - item.y) / item.height * item.mask_height);
    return item.runs.some(([y, start, end]) => row === y && col >= start && col <= end);
}
export function strokesTouch(a, b, rect) {
    const ab = strokeBounds(a), bb = strokeBounds(b), pad = (a.width + b.width) * rect.width / rect.height;
    if (ab.x > bb.x + bb.width + pad || bb.x > ab.x + ab.width + pad || ab.y > bb.y + bb.height + pad || bb.y > ab.y + ab.height + pad) return false;
    const pixel = p => ({x: p.x * rect.width, y: p.y * rect.height});
    const radius = (a.width + b.width) * rect.width / 2;
    const cross = (p, q, r) => (q.x - p.x) * (r.y - p.y) - (q.y - p.y) * (r.x - p.x);
    for (let i = 0; i < a.points.length; i++) {
        const p = pixel(a.points[Math.max(0, i - 1)]), q = pixel(a.points[i]);
        for (let j = 0; j < b.points.length; j++) {
            const r = pixel(b.points[Math.max(0, j - 1)]), s = pixel(b.points[j]);
            const crosses = cross(p, q, r) * cross(p, q, s) < 0 && cross(r, s, p) * cross(r, s, q) < 0;
            if (crosses || Math.min(distanceToLine(p.x, p.y, r.x, r.y, s.x, s.y), distanceToLine(q.x, q.y, r.x, r.y, s.x, s.y), distanceToLine(r.x, r.y, p.x, p.y, q.x, q.y), distanceToLine(s.x, s.y, p.x, p.y, q.x, q.y)) <= radius) return true;
        }
    }
    return false;
}
export function selectedGroup(document, root) {
    const values = [...document.items, ...document.strokes];
    const byId = new Map(values.map(value => [value.id, value])), groups = new Map(), fills = new Map();
    for (const value of values) {
        if (value.group_id) { if (!groups.has(value.group_id)) groups.set(value.group_id, []); groups.get(value.group_id).push(value.id); }
        if (value.parent_id) { if (!fills.has(value.parent_id)) fills.set(value.parent_id, []); fills.get(value.parent_id).push(value.id); }
    }
    const members = new Set(), queue = [root.id];
    while (queue.length) {
        const id = queue.pop(), value = byId.get(id);
        if (members.has(id) || !value) continue;
        members.add(id);
        queue.push(...(groups.get(value.group_id) || []), ...(fills.get(id) || []));
        if (value.kind === 'fill' && value.parent_id) queue.push(value.parent_id);
    }
    return {items: document.items.map((item, i) => members.has(item.id) ? i : -1).filter(i => i >= 0),
        strokes: document.strokes.map((stroke, i) => members.has(stroke.id) ? i : -1).filter(i => i >= 0)};
}
export function groupBounds(document, group) {
    const boxes = [...group.items.map(i => document.items[i]), ...group.strokes.map(i => strokeBounds(document.strokes[i]))];
    if (!boxes.length) return null;
    const x = Math.min(...boxes.map(b => b.x)), y = Math.min(...boxes.map(b => b.y));
    return {x, y, width: Math.max(.01, Math.max(...boxes.map(b => b.x + b.width)) - x), height: Math.max(.01, Math.max(...boxes.map(b => b.y + b.height)) - y)};
}
export function boxGroup(document, box) {
    const inside = value => value.x >= box.x && value.y >= box.y && value.x + value.width <= box.x + box.width && value.y + value.height <= box.y + box.height;
    const items = new Set(), strokes = new Set();
    for (const value of [...document.items, ...document.strokes]) {
        if (!inside(value.points ? strokeBounds(value) : value)) continue;
        const group = selectedGroup(document, value);
        group.items.forEach(i => items.add(i)); group.strokes.forEach(i => strokes.add(i));
    }
    return {items: [...items], strokes: [...strokes]};
}
export function transformGroup(document, beforeDocument, group, before, after) {
    for (const index of group.items) {
        const item = beforeDocument.items[index];
        const minimum = item.kind === 'fill' ? .000001 : ['line', 'arrow'].includes(item.kind) ? .001 : .01;
        const width = clamp(item.width / before.width * after.width, minimum), height = clamp(item.height / before.height * after.height, minimum);
        document.items[index] = {...item, x: clamp(after.x + (item.x - before.x) / before.width * after.width, 0, 1 - width),
            y: clamp(after.y + (item.y - before.y) / before.height * after.height, 0, 1 - height), width, height};
        if (item.cuts) document.items[index].cuts = item.cuts.map(cut => ({...transformStroke(cut, before, after), width: clamp(cut.width * after.width / before.width, .001, .08)}));
    }
    for (const index of group.strokes) document.strokes[index] = transformStroke(beforeDocument.strokes[index], before, after);
}
