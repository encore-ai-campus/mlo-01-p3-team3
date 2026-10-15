/* Browser-only client for the existing RoomPick Canvas. No provider keys in JS. */
function csrfToken() {
    const cookie = document.cookie.split(';').map(v => v.trim()).find(v => v.startsWith('csrftoken='));
    return cookie ? decodeURIComponent(cookie.slice(10)) : document.querySelector('[name=csrfmiddlewaretoken]')?.value;
}
const blobOf = canvas => new Promise((resolve, reject) => canvas.toBlob(blob => blob ? resolve(blob) : reject(new Error('스케치 내보내기 실패')), 'image/png'));

export function createRoomPickClient({baseUrl = '/ai-mvp'} = {}) {
    const base = baseUrl.replace(/\/$/, '');
    async function api(path, options = {}) {
        const response = await fetch(base + path, {credentials: 'same-origin', ...options,
            headers: {...options.headers, 'X-CSRFToken': csrfToken() || ''}});
        const value = await response.json();
        if (!response.ok) throw new Error(value.error || `AI 요청 실패 (${response.status})`);
        return value;
    }
    async function submitCanvas({project, document: drawing, renderSketch, text = '', requestId = crypto.randomUUID()}) {
        if (!project.photo_url) throw new Error('공간 사진을 먼저 업로드하세요.');
        const photoResponse = await fetch(project.photo_url, {credentials: 'same-origin'});
        if (!photoResponse.ok) throw new Error('공간 사진 원본을 읽지 못했습니다.');
        const photo = await photoResponse.blob();
        const hasDrawing = !!(drawing.strokes?.length || drawing.items?.length);
        const form = new FormData();
        form.set('photo', photo, 'room-upload');
        form.set('project', JSON.stringify({id: project.id, revision: project.revision, width: project.width, height: project.height}));
        form.set('document', JSON.stringify(drawing));
        form.set('text', text);
        form.set('request_id', requestId);
        if (hasDrawing) {
            if (typeof renderSketch !== 'function') throw new Error('스케치 전용 렌더링 함수를 연결하세요.');
            const bitmap = await createImageBitmap(photo, {imageOrientation: 'from-image'});
            const canvas = window.document.createElement('canvas');
            canvas.width = bitmap.width; canvas.height = bitmap.height; bitmap.close();
            // Transparent art only; server creates the white-background API copy.
            renderSketch(canvas.getContext('2d'), canvas);
            form.set('sketch', await blobOf(canvas), 'sketch.png');
        }
        return api('/api/runs', {method: 'POST', body: form});
    }
    return {api, submitCanvas, resultUrl: runId => base + '/?run=' + encodeURIComponent(runId)};
}
