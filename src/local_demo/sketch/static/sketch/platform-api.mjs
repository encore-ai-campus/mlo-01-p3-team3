/** Application API boundary. Keep provider credentials and the Ad server on the server. */
export function createPlatformAPI(request, config = {}) {
    const routes = {
        generate: '/api/projects/{id}/scene-generations/',
        scene: '/api/scene-generations/{id}/',
        recommend: '/api/scene-objects/{id}/recommendations/',
        revise: '/api/scene-objects/{id}/revisions/',
        link: '/api/recommendation-items/{id}/link/',
        ...config.routes,
    };
    const route = (name, id) => routes[name].replace('{id}', encodeURIComponent(id));
    async function completed(response, {signal, onStatus = () => {}} = {}) {
        const terminal = new Set(['SUCCEEDED', 'COMPLETED', 'FAILED', 'CANCELLED', 'NOT_READY']);
        const deadline = Date.now() + (config.jobTimeoutMs || 180000);
        while (response.job_id && !terminal.has(response.status)) {
            onStatus(response.status || 'QUEUED');
            if (signal?.aborted) throw new DOMException('Cancelled', 'AbortError');
            if (Date.now() >= deadline) throw new Error('작업이 계속 진행 중입니다. 잠시 후 다시 확인해 주세요.');
            await new Promise(resolve => setTimeout(resolve, 1200));
            const url = response.status_url || `/api/jobs/${encodeURIComponent(response.job_id)}/`;
            if (!url.startsWith('/') || url.startsWith('//')) throw new Error('작업 조회 주소가 올바르지 않습니다.');
            response = await request(url, undefined, {signal});
        }
        if (['FAILED', 'CANCELLED', 'NOT_READY'].includes(response.status)) {
            const error = new Error(response.error || '작업을 완료하지 못했습니다. 입력과 생성 결과는 유지됩니다.');
            error.jobStatus = response.status;
            error.retryable = response.status === 'FAILED';
            throw error;
        }
        return response;
    }
    return {
        generate: async (projectId, input, options) => completed(await request(route('generate', projectId), input, {signal: options?.signal}), options),
        scene: id => request(route('scene', id)),
        recommend: async (objectId, input, options) => completed(await request(route('recommend', objectId), input, {signal: options?.signal}), options),
        revise: (objectId, input) => request(route('revise', objectId), input),
        resolveLink: (itemId, input) => request(route('link', itemId), input),
    };
}
