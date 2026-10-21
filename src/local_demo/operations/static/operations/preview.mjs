const fillCanvases = new Map();
function segment(stroke, first, last, target, size) {
    const context = target, ink = size;
    context.save();
    context.globalCompositeOperation = stroke.tool === 'eraser' ? 'destination-out' : 'source-over';
    context.strokeStyle = context.fillStyle = stroke.color; context.lineWidth = stroke.width * ink.width;
    context.lineCap = context.lineJoin = 'round'; context.beginPath();
    if (first === last) { context.arc(first.x * ink.width, first.y * ink.height, context.lineWidth / 2, 0, Math.PI * 2); context.fill(); }
    else { context.moveTo(first.x * ink.width, first.y * ink.height); context.lineTo(last.x * ink.width, last.y * ink.height); context.stroke(); }
    context.restore();
}
export function drawDocument(target, drawing, size) {
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
function drawObject(item, target, size) {
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
