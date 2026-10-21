import test from 'node:test';
import assert from 'node:assert/strict';
import {createDurableQueue} from '../sketch/static/sketch/durable-queue.mjs';
class Storage {
    values = new Map();
    get length() { return this.values.size; }
    key(i) { return [...this.values.keys()][i] ?? null; }
    getItem(key) { return this.values.get(key) ?? null; }
    setItem(key, value) { this.values.set(key, String(value)); }
    removeItem(key) { this.values.delete(key); }
}
test('tabs share individual records without overwriting unacknowledged rows', () => {
    const storage = new Storage(), a = createDurableQueue('test', storage), b = createDurableQueue('test', storage);
    a.add({id:'a', value:1}); b.add({id:'b', value:2});
    assert.deepEqual(a.rows().map(row => row.id).sort(), ['a','b']);
    b.remove('a'); a.add({id:'c', value:3});
    assert.deepEqual(b.rows().map(row => row.id).sort(), ['b','c']);
    assert.deepEqual(a.rows().map(row => row.id).sort(), ['b','c']);
});
test('rejected rows are retained separately and do not block later delivery', () => {
    const storage = new Storage(), queue = createDurableQueue('test', storage);
    queue.add({id:'bad'}); queue.add({id:'good'}); queue.reject('bad', 'invalid');
    assert.deepEqual(queue.rows().map(row => row.id), ['good']);
    assert.match([...storage.values.values()].join(''), /invalid/);
});
test('blocked storage still allows in-memory acknowledgement', () => {
    const queue = createDurableQueue('test', {get length(){throw Error('blocked');}, setItem(){throw Error('blocked');}});
    queue.add({id:'a'}); assert.equal(queue.rows().length, 1);
    queue.remove('a'); assert.equal(queue.rows().length, 0);
});
test('a browser denying access to localStorage can still enqueue events', () => {
    const previous=Object.getOwnPropertyDescriptor(globalThis,'localStorage');
    Object.defineProperty(globalThis,'localStorage',{configurable:true,get(){throw Error('denied');}});
    try {const queue=createDurableQueue('test'); queue.add({id:'fallback'}); assert.equal(queue.rows().length,1);}
    finally {if(previous) Object.defineProperty(globalThis,'localStorage',previous); else delete globalThis.localStorage;}
});
