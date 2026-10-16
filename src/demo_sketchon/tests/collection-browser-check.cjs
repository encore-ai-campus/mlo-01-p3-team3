const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {chromium} = require('playwright');
const baseURL = process.env.ROOMPICK_TEST_URL || 'http://127.0.0.1:8003';

(async () => {
    const browser = await chromium.launch({headless: true, channel: process.env.ROOMPICK_TEST_BROWSER || 'msedge'});
    try {
        const context = await browser.newContext({viewport: {width: 1440, height: 1000}});
        const page = await context.newPage(), errors = [], events = [];
        page.on('pageerror', error => errors.push(error.message));
        page.on('request', request => {
            if (request.url().endsWith('/events/') && request.headers()['content-type'] === 'application/json') events.push(...request.postDataJSON().events);
        });
        // Reject only raw uploads: drawing, old events and mock search must still work.
        await context.route('**/samples/', route => route.fulfill({status: 503, contentType: 'application/json', body: '{"error":"test offline"}'}));
        await require('./browser-member.cjs')(context,baseURL);
        await page.goto(baseURL);
        const homeClick = page.waitForResponse(response => response.url().endsWith('/events/') && response.status() === 200);
        await page.locator('#start-sketch').click();
        await homeClick; await page.waitForURL(url => url.pathname.startsWith('/editor/'));
        assert.ok(events.some(event => event.event_type === 'button.clicked' && event.payload.button === 'start-sketch' && event.payload.page === 'home'));
        const visitor = JSON.parse(await page.locator('#visitor-data').textContent());
        const transfer = await page.evaluateHandle(() => {
            const canvas = document.createElement('canvas'); canvas.width = 1200; canvas.height = 800;
            canvas.getContext('2d').fillRect(0, 0, 1200, 800);
            const bytes = Uint8Array.from(atob(canvas.toDataURL('image/png').split(',')[1]), c => c.charCodeAt(0));
            const data = new DataTransfer(); data.items.add(new File([bytes], 'room.png', {type: 'image/png'})); return data;
        });
        await page.locator('#drop-zone').dispatchEvent('drop', {dataTransfer: transfer});
        await page.waitForFunction(() => !document.querySelector('#room-photo').hidden);
        async function drag(start, end, cancel = false) {
            await page.locator('#surface').scrollIntoViewIfNeeded();
            const box = await page.locator('#surface').boundingBox();
            await page.mouse.move(box.x + start[0] * box.width, box.y + start[1] * box.height);
            await page.mouse.down();
            await page.mouse.move(box.x + end[0] * box.width, box.y + end[1] * box.height, {steps: 8});
            if (cancel) await page.evaluate(() => window.dispatchEvent(new Event('blur')));
            await page.mouse.up();
        }
        await drag([.2, .2], [.6, .2]);
        const readDrafts = () => page.evaluate(() => new Promise(resolve => {
            const open = indexedDB.open('roompick-sketch-v1');
            open.onsuccess = () => {
                const database = open.result, get = database.transaction('drafts').objectStore('drafts').getAll();
                get.onsuccess = () => { resolve(get.result); database.close(); };
            };
        }));
        const liveDraft = (await readDrafts()).find(row => row.sample.strokes.length);
        assert.ok(liveDraft);
        const duplicate = await context.newPage();
        await duplicate.addInitScript(id => sessionStorage.setItem('roompick.raw.tab', id), liveDraft.tab_id);
        await duplicate.goto(page.url());
        await duplicate.waitForTimeout(500);
        assert.ok((await readDrafts()).some(row => row.sample_id === liveDraft.sample_id), 'a copied tab must not finalize another live tab draft');
        await duplicate.close();
        await page.locator('#eraser').click(); await drag([.4, .2], [.41, .2]);
        await page.locator('#undo').click(); await page.locator('#redo').click();
        await page.locator('#pen').click(); await drag([.8, .3], [.9, .3], true);
        page.once('dialog', dialog => dialog.accept()); await page.locator('#clear').click();
        await page.locator('#undo').click();
        assert.match(await page.locator('#counts').textContent(), /^1획/);
        await page.locator('#search').click(); await page.waitForURL('**/results/**');

        const readOutbox = () => page.evaluate(() => new Promise((resolve, reject) => {
            const open = indexedDB.open('roompick-sketch-v1');
            open.onerror = () => reject(open.error);
            open.onsuccess = () => {
                const database = open.result;
                const request = database.transaction('outbox').objectStore('outbox').getAll();
                request.onsuccess = () => { resolve(request.result); database.close(); };
            };
        }));
        const sample = (await readOutbox()).find(row => row.submission_reason === 'search');
        assert.ok(sample, 'failed upload must remain in IndexedDB outbox');
        assert.equal(sample.writer_id, visitor);
        assert.equal(sample.strokes.length, 3);
        assert.ok(sample.strokes.some(row => row.tool === 'eraser'));
        const eraseId = sample.strokes.find(row => row.tool === 'eraser').stroke_id;
        assert.ok(sample.operations.some(row => row.source_stroke_id === eraseId && row.target_stroke_ids.length));
        assert.ok(sample.strokes.some(row => row.end_reason === 'lost_capture' && row.points.at(-1).source === 'synthetic'));
        assert.equal(sample.undo_count, 2);
        assert.equal(sample.final_document.strokes.length, 1);
        assert.ok(sample.final_document.strokes[0].cuts.length);
        assert.ok(sample.operations.some(row => row.type === 'history.redo'));
        assert.ok(sample.operations.some(row => row.type === 'sketch.cleared' && row.target_stroke_ids.length));
        assert.equal(sample.label_id, null);

        const placement = page.locator('.placement').first(), box = await placement.boundingBox();
        await page.mouse.move(box.x + box.width / 3, box.y + box.height / 3); await page.mouse.down();
        await page.mouse.move(box.x + box.width / 3 + 20, box.y + box.height / 3 + 15, {steps: 5});
        const adjusted = page.waitForResponse(response => response.url().endsWith('/placements/'));
        await page.mouse.up(); assert.equal((await adjusted).status(), 200);
        await page.waitForFunction(() => !document.querySelector('#download-png').disabled);
        const download = page.waitForEvent('download'); await page.locator('#download-png').click(); await download;

        await context.unroute('**/samples/');
        const missing = structuredClone(sample), bad = structuredClone(sample), good = structuredClone(sample);
        missing.sample_id = crypto.randomUUID().replace(/^.{8}/, '00000000'); missing.project_id = crypto.randomUUID();
        bad.sample_id = crypto.randomUUID().replace(/^.{8}/, '00000001'); bad.label_status = 'verified';
        good.sample_id = crypto.randomUUID().replace(/^.{8}/, 'ffffffff');
        await page.evaluate(rows => new Promise(resolve => {
            const open = indexedDB.open('roompick-sketch-v1');
            open.onsuccess = () => {
                const database = open.result, tx = database.transaction('outbox', 'readwrite');
                for (const sample of rows) tx.objectStore('outbox').put(sample);
                tx.oncomplete = () => { database.close(); resolve(); };
            };
        }), [missing, bad, good]);
        const laterAccepted = page.waitForResponse(response => response.url().endsWith('/samples/') &&
            response.request().headers()['content-type'] === 'application/json' && response.request().postDataJSON().sample_id === good.sample_id);
        const accepted = page.waitForResponse(response => response.url().endsWith('/samples/') &&
            response.request().headers()['content-type'] === 'application/json' &&
            response.request().postDataJSON().sample_id === sample.sample_id);
        await page.locator('#edit-sketch').click(); await page.waitForURL('**/editor/**');
        const response = await accepted;
        assert.ok([200, 201].includes(response.status()), await response.text());
        const laterResponse = await laterAccepted;
        assert.ok([200, 201].includes(laterResponse.status()), await laterResponse.text());
        await page.waitForFunction(async id => {
            const database = await new Promise(resolve => { const open = indexedDB.open('roompick-sketch-v1'); open.onsuccess = () => resolve(open.result); });
            const saved = await new Promise(resolve => { const get = database.transaction('outbox').objectStore('outbox').get(id); get.onsuccess = () => resolve(get.result); });
            database.close(); return !saved;
        }, sample.sample_id);
        assert.ok((await readOutbox()).some(row => row.sample_id === bad.sample_id), 'rejected sample must remain available for review');
        assert.ok((await readOutbox()).some(row => row.sample_id === missing.sample_id), 'HTML 404 sample must be retained without blocking later samples');
        assert.ok(events.some(event => event.event_type === 'result.moved'));
        assert.ok(events.some(event => event.event_type === 'button.clicked' && event.payload.button === 'download-png'));
        assert.deepEqual(errors, []);
        if (process.env.ROOMPICK_TEST_RAW_ROOT) {
            const saved = JSON.parse(fs.readFileSync(path.join(process.env.ROOMPICK_TEST_RAW_ROOT, sample.sample_id + '.json'), 'utf8'));
            assert.deepEqual(saved, sample);
        }
        console.log('PASS: home click, original result actions, partial eraser/undo/redo/clear/cancel raw history, IndexedDB retention, copied-tab protection, rejected sample does not block later uploads, retry and actual immutable JSON.');
    } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exit(1); });
