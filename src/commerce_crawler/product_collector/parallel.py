"""Bounded page fetching; the caller alone merges results and writes checkpoints."""
import copy
from collections import deque
from concurrent.futures import ThreadPoolExecutor, wait, FIRST_COMPLETED


def page_results(adapter, client, states, workers, stop_reason, on_start):
    queue = deque(states)

    def prepare(state):
        if 'adapter' not in state:
            state['adapter'] = copy.copy(adapter)
            if hasattr(adapter, 'pagination_templates'):
                state['adapter'].pagination_templates = copy.deepcopy(adapter.pagination_templates)
            on_start(state)

    def fetch(state):
        try:
            result = state['adapter'].fetch_listing_page(client, state['category'], state['page'])
            return result, None
        except Exception as exc:
            return None, exc

    if workers == 1:
        while queue and not stop_reason():
            state = queue.popleft()
            prepare(state)
            result, error = fetch(state)
            yield state, result, error
            if not state['done']:
                queue.appendleft(state)
        return

    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='danawa-list')
    pending = {}
    try:
        while queue or pending:
            while queue and len(pending) < workers and not stop_reason():
                state = queue.popleft()
                prepare(state)
                pending[pool.submit(fetch, state)] = state
            if not pending:
                break
            ready, _ = wait(pending, return_when=FIRST_COMPLETED)
            for future in sorted(ready, key=lambda f: pending[f]['index']):
                state = pending.pop(future)
                result, error = future.result()
                yield state, result, error
                if not state['done']:
                    queue.appendleft(state)
    except BaseException:
        client.cancel()
        raise
    finally:
        for future in pending:
            future.cancel()
        pool.shutdown(wait=True, cancel_futures=True)
