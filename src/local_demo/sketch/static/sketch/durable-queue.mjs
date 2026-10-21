// One storage key per event: acknowledging one tab's batch cannot erase another tab's events.
export function createDurableQueue(namespace, storage) {
    try { storage ??= globalThis.localStorage; } catch {}
    const prefix = namespace + '.pending.', rejected = namespace + '.rejected.';
    const memory = new Map();
    function rows() {
        const found = new Map(memory);
        try {
            for (let i = 0; i < storage.length; i++) {
                const key = storage.key(i);
                if (!key?.startsWith(prefix)) continue;
                try { const row = JSON.parse(storage.getItem(key)); if (row?.id) found.set(row.id, row); } catch {}
            }
        } catch {}
        return [...found.values()];
    }
    function add(row) {
        if (!row || typeof row.id !== 'string') throw new Error('Queue record needs an id');
        const value = structuredClone(row);
        try { storage.setItem(prefix + row.id, JSON.stringify(value)); }
        catch { memory.set(row.id, value); }
    }
    function remove(id) {
        memory.delete(id);
        try { storage.removeItem(prefix + id); } catch {}
    }
    function reject(id, reason) {
        const row = rows().find(row => row.id === id);
        if (!row) return;
        // Do not drop a rejected event if its diagnostic record cannot be retained.
        storage.setItem(rejected + id, JSON.stringify({row, reason, rejected_at: new Date().toISOString()}));
        remove(id);
    }
    return {add, rows, remove, reject};
}
