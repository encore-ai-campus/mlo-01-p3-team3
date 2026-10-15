/* Paste the import near the imports in the EXISTING editor.mjs.
 * Replace its existing searchProducts function with this function.
 * Its two existing click listeners can stay as they are.
 * This example relies on the existing editor's lexical variables/renderArtwork.
 */
import {createRoomPickClient} from '/ai-mvp/ui/roompick-client.mjs';
const aiMvp = createRoomPickClient({baseUrl: '/ai-mvp'});

function searchProducts() {
    commitText();
    return operation(async () => {
        if (!project.photo_url) throw new Error('공간 사진을 먼저 업로드해 주세요.');
        const text = document.getElementById('requirements')?.value?.trim() || '';
        if (!drawing.items.length && !drawing.strokes.length && !text) {
            throw new Error('스케치 또는 텍스트 요구사항을 입력해 주세요.');
        }
        requestId ||= identifier();
        record('generation.requested', {mode: 'interior_recommender', request_id: requestId});
        message('입력을 저장하고 템플릿 후보를 준비하고 있어요.');
        const next = await aiMvp.submitCanvas({
            project,
            document: structuredClone(drawing),
            renderSketch: (ctx, size) => renderArtwork(ctx, size),
            text,
            requestId,
        });
        // AI run UUID is not an existing RoomPick mock result_id.
        // Keep original collection behavior without assigning an invalid result FK.
        collector.submit(null);
        await flushEvents();
        window.location.assign(aiMvp.resultUrl(next.run_id));
    });
}
