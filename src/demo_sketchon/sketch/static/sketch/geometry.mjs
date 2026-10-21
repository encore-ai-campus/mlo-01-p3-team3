export const clamp = (value, min = 0, max = 1) => Math.max(min, Math.min(max, value));

export function normalizePoint(clientX, clientY, rect) {
    return {x: clamp((clientX - rect.left) / rect.width), y: clamp((clientY - rect.top) / rect.height)};
}

export function moveItem(item, dx, dy) {
    return {...item, x: clamp(item.x + dx, 0, 1 - item.width), y: clamp(item.y + dy, 0, 1 - item.height)};
}

export function resizeItem(item, dx, dy) {
    return {...item, width: clamp(item.width + dx, .04, 1 - item.x), height: clamp(item.height + dy, .04, 1 - item.y)};
}

export function strokeBounds(stroke) {
    const xs = stroke.points.map(point => point.x), ys = stroke.points.map(point => point.y);
    const x = clamp(Math.min(...xs) - stroke.width / 2), y = clamp(Math.min(...ys) - stroke.width / 2);
    return {x, y, width: Math.max(.01, Math.min(1 - x, Math.max(...xs) + stroke.width / 2 - x)),
        height: Math.max(.01, Math.min(1 - y, Math.max(...ys) + stroke.width / 2 - y))};
}

export function transformStroke(stroke, before, after) {
    const result = {...stroke, points: stroke.points.map(point => ({...point,
        x: clamp(after.x + (point.x - before.x) / before.width * after.width),
        y: clamp(after.y + (point.y - before.y) / before.height * after.height)}))};
    if (stroke.cuts) result.cuts = stroke.cuts.map(cut => ({...transformStroke(cut, before, after), width: clamp(cut.width * after.width / before.width, .001, .08)}));
    return result;
}
