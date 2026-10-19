export function initialAccount() {
    return window.__sketchAccountState || JSON.parse(document.getElementById('account-initial')?.textContent || '{"authenticated":false,"actor_key":"unknown"}');
}

export function initAccountState({projectId, onChange, beforeLogin} = {}) {
    let state = initialAccount(), serial = 0, destroyed = false;
    const nav = document.querySelector('[data-account-nav]');
    function render(next) {
        window.__sketchAccountState = next;
        nav?.querySelectorAll('.account-login').forEach(node => node.hidden = next.authenticated);
        nav?.querySelectorAll('.account-member').forEach(node => node.hidden = !next.authenticated);
        nav?.querySelectorAll('[data-ops-link]').forEach(node => { node.hidden = !next.can_access_ops; node.href = next.ops_url || '/ops/'; });
        nav?.querySelectorAll('.account-nickname').forEach(node => node.textContent = next.nickname);
        document.querySelectorAll('[data-member-only]').forEach(node => { node.hidden = !next.authenticated; });
        for (const token of document.querySelectorAll('[name=csrfmiddlewaretoken]')) if (next.csrf_token) token.value = next.csrf_token;
        const quota = document.getElementById('guest-quota');
        if (quota && next.quota) quota.textContent = next.authenticated ? '회원은 공간을 제한 없이 생성할 수 있어요.' : `비회원 오늘 ${next.quota.remaining}회 남음 · 매일 한국 시간 00:00 초기화`;
    }
    async function refresh({conceal = false} = {}) {
        const current = ++serial;
        if (conceal) { document.body.classList.add('account-revalidating'); nav?.querySelectorAll('.account-member').forEach(node => node.hidden = true); }
        try {
            const id = typeof projectId === 'function' ? projectId() : projectId;
            const params=new URLSearchParams();
            if(id)params.set('project_id',id);
            if(document.body.dataset.opsPermission)params.set('ops_permission',document.body.dataset.opsPermission);
            const query=params.toString();
            const response = await fetch('/api/account/status/' + (query ? '?' + query : ''), {credentials:'same-origin',cache:'no-store',headers:{Accept:'application/json'}});
            if (!response.ok) throw new Error('로그인 상태를 확인하지 못했어요. 새로고침 후 다시 시도해 주세요.');
            const next = await response.json();
            if (destroyed || current !== serial) return state;
            const changed = next.actor_key !== state.actor_key;
            if (changed) window.__sketchAccountEpoch = (window.__sketchAccountEpoch || 0) + 1;
            if ((document.body.classList.contains('ops-page') && (!next.can_access_ops || next.ops_page_access === false)) || next.project_access === false || (changed && document.body.classList.contains('member-page'))) {
                // Remove protected content before it can be exposed from the BFCache.
                const main = document.querySelector('main');
                if (main) { main.replaceChildren(); const text = document.createElement('p'); text.textContent = '계정이 변경되었어요. 현재 계정으로 다시 시작해 주세요.'; const link = document.createElement('a'); link.href = '/'; link.textContent = '홈으로 돌아가기'; main.append(text,link); }
                state = next; render(next);
                document.body.classList.add('account-inaccessible');
                onChange?.(next,{changed,denied:true});
            } else {
                state = next; render(next); onChange?.(next,{changed,denied:false});
            }
            document.body.classList.remove('account-revalidating');
            return next;
        } catch (error) {
            if (current !== serial || destroyed) return state;
            // A failed revalidation keeps private cached content concealed.
            let notice = document.getElementById('account-status-error');
            if (!notice) { notice = document.createElement('p'); notice.id='account-status-error'; notice.setAttribute('role','alert'); document.body.append(notice); }
            notice.textContent = error.message;
            return null;
        }
    }
    async function loginClick(event) {
        const link = event.target.closest('.account-login');
        if (!link || !beforeLogin) return;
        event.preventDefault();
        try {
            await beforeLogin();
            window.location.assign('/accounts/login/?next=' + encodeURIComponent(location.pathname + location.search));
        } catch (error) {
            const status = document.getElementById('status');
            if (status) status.textContent = '입력을 저장하지 못해 로그인 이동을 멈췄어요. ' + error.message;
        }
    }
    const pageshow = event => { void refresh({conceal:!!event.persisted}); };
    const visibility = () => { if (document.visibilityState === 'visible') void refresh({conceal:true}); };
    window.addEventListener('pageshow',pageshow); document.addEventListener('visibilitychange',visibility);
    document.addEventListener('click',loginClick);
    render(state); void refresh();
    return {refresh,destroy(){destroyed=true;serial++;window.removeEventListener('pageshow',pageshow);document.removeEventListener('visibilitychange',visibility);document.removeEventListener('click',loginClick);}};
}
