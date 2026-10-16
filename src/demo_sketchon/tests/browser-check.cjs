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
        page.on('request', request => { if (request.url().endsWith('/events/') && request.headers()['content-type'] === 'application/json') events.push(...request.postDataJSON().events); });
        fs.mkdirSync(path.join(__dirname, '../test-results'), {recursive: true});
        const capture = name => page.screenshot({path: path.join(__dirname, '../test-results/' + name + '.png'), fullPage: true});
        await page.goto(baseURL); assert.equal(await page.locator('#surface').count(), 0); await capture('home');
        await page.locator('.start-button').click(); await page.waitForURL('**/editor/');
        const visitor = await page.locator('#visitor-data').textContent();
        assert.equal(await page.locator('#before-upload, #before-crop').count(), 0);
        assert.equal(await page.locator('#undo').isDisabled(), true);
        assert.equal(await page.locator('input#title').count(), 0);
        assert.equal(await page.locator('#title').textContent(), '나의 공간 스케치');
        const oversized = await page.evaluateHandle(() => { const data = new DataTransfer(); data.items.add(new File([new Uint8Array(10 * 1024 * 1024 + 1)], 'large.png', {type:'image/png'})); return data; });
        const sizeDialog = page.waitForEvent('dialog');
        const oversizedDrop = page.locator('#drop-zone').dispatchEvent('drop', {dataTransfer: oversized});
        const warning = await sizeDialog; assert.match(warning.message(), /10MB 이하/); await warning.accept();
        await oversizedDrop;
        assert.equal(await page.locator('#room-photo').isVisible(), false);
        assert.deepEqual(errors, []);
        const makeTransfer = () => page.evaluateHandle(() => {
            const canvas = document.createElement('canvas'); canvas.width = 1200; canvas.height = 800;
            const ctx = canvas.getContext('2d'); ctx.fillStyle = '#ede5d8'; ctx.fillRect(0, 0, 1200, 800);
            ctx.fillStyle = '#b99e7d'; ctx.fillRect(0, 570, 1200, 230); ctx.fillStyle = '#e5f0ed'; ctx.fillRect(100, 90, 300, 350);
            ctx.strokeStyle = '#fffdf5'; ctx.lineWidth = 18; ctx.strokeRect(100, 90, 300, 350);
            const bytes = Uint8Array.from(atob(canvas.toDataURL('image/png').split(',')[1]), c => c.charCodeAt(0));
            const data = new DataTransfer(); data.items.add(new File([bytes], 'room.png', {type: 'image/png'})); return data;
        });
        const transfer = await makeTransfer();
        await page.locator('#drop-zone').dispatchEvent('drop', {dataTransfer: transfer});
        await page.waitForFunction(() => !document.querySelector('#room-photo').hidden);
        assert.equal(await page.locator('#undo').isDisabled(), false);
        const uploadedURL = page.url();
        await page.locator('#undo').click();
        await page.waitForFunction(() => document.querySelector('#room-photo').hidden);
        assert.equal(await page.locator('#visitor-data').textContent(), visitor);
        assert.equal(await page.locator('#undo').isDisabled(), true);
        await page.locator('#redo').click(); await page.waitForURL(uploadedURL);
        await page.waitForFunction(() => !document.querySelector('#room-photo').hidden);
        async function at(x, y, action) {
            await page.locator('#surface').scrollIntoViewIfNeeded();
            const box = await page.locator('#surface').boundingBox();
            await action(box.x + x * box.width, box.y + y * box.height, box);
        }
        async function drag(start, end) {
            await at(start[0], start[1], async (x, y, box) => {
                await page.mouse.move(x, y); await page.mouse.down();
                await page.mouse.move(box.x + end[0] * box.width, box.y + end[1] * box.height, {steps: 8}); await page.mouse.up();
            });
        }
        const click = (x, y) => at(x, y, (px, py) => page.mouse.click(px, py));
        const sample = (x, y) => page.locator('#ink').evaluate((canvas, pos) => Array.from(canvas.getContext('2d').getImageData(Math.floor(pos.x * canvas.width), Math.floor(pos.y * canvas.height), 1, 1).data), {x, y});
        async function toolDrag(tool, start, end) { await page.locator('#' + tool).click(); await drag(start, end); }
        const beforeCrop = page.url();
        await toolDrag('crop', [.1, .1], [.9, .9]); await page.locator('#apply-crop').click();
        await page.waitForURL(url => url.toString() !== beforeCrop && url.pathname.includes('/editor/'));
        await page.waitForFunction(() => !document.querySelector('#room-photo').hidden);
        let projectId = page.url().split('/').filter(Boolean).at(-1);
        const cropped = await context.request.get(baseURL + '/api/projects/' + projectId + '/').then(r => r.json());
        assert.deepEqual([cropped.width, cropped.height], [960, 640]);
        await page.reload(); await page.waitForFunction(() => !document.querySelector('#room-photo').hidden);
        await page.locator('#undo').click(); await page.waitForURL(beforeCrop);
        const original = await context.request.get(baseURL + '/api/projects/' + page.url().split('/').filter(Boolean).at(-1) + '/').then(r => r.json());
        assert.deepEqual([original.width, original.height], [1200, 800]);
        await page.locator('#redo').click();
        await page.waitForURL(url => url.toString() !== beforeCrop && url.pathname.includes('/editor/'));
        assert.equal(await page.locator('#visitor-data').textContent(), visitor);
        const toolbar = await page.locator('#toolbar').boundingBox(), surface = await page.locator('#surface').boundingBox();
        assert.ok(toolbar.y + toolbar.height <= surface.y);

        // A closed freehand contour is connected automatically. Fill selects its complete contour.
        await toolDrag('pen', [.2, .2], [.4, .2]); await drag([.4, .2], [.4, .4]);
        await drag([.4, .4], [.2, .4]); await drag([.2, .4], [.2, .2]);
        await page.locator('#fill').click(); await page.locator('#color').fill('#ee3344'); await click(.3, .3);
        await page.locator('#box-select').click(); await drag([.3, .3], [.35, .3]);
        assert.equal((await sample(.2, .3))[3], 0);
        assert.deepEqual(await sample(.35, .3), [238, 51, 68, 255]);
        assert.ok((await sample(.25, .3))[3] > 200);
        await page.locator('#pen').click(); await page.locator('#thickness').fill('25'); await page.locator('#thickness').dispatchEvent('change');
        assert.ok((await sample(.26, .3))[3] > 200);
        await page.locator('#eraser').click(); await click(.25, .3);
        assert.equal((await sample(.25, .3))[3], 0);
        assert.ok((await sample(.25, .35))[3] > 200);
        await page.locator('#box-select').click(); await drag([.35, .3], [.4, .3]);
        assert.equal((await sample(.30, .3))[3], 0);
        assert.ok((await sample(.30, .35))[3] > 200);
        page.once('dialog', dialog => dialog.accept()); await page.locator('#clear').click();

        // Disconnected strokes move separately until the user explicitly groups an area.
        await page.locator('#pen').click(); await page.locator('#thickness').fill('8'); await page.locator('#thickness').dispatchEvent('change');
        await drag([.65, .08], [.75, .08]); await drag([.65, .14], [.75, .14]);
        await page.locator('#box-select').click(); await drag([.70, .08], [.70, .09]);
        assert.equal((await sample(.7, .08))[3], 0); assert.ok((await sample(.7, .14))[3] > 200);
        await toolDrag('box-select', [.60, .04], [.82, .2]);
        await page.locator('#group-selection').click();
        await page.locator('#box-select').click(); await drag([.70, .09], [.73, .09]);
        assert.ok((await sample(.78, .14))[3] > 200); assert.equal((await sample(.65, .14))[3], 0);
        await page.locator('#ungroup-selection').click();
        await drag([.73, .09], [.73, .10]);
        assert.ok((await sample(.78, .14))[3] > 200);
        page.once('dialog', dialog => dialog.accept()); await page.locator('#clear').click();

        // Cropping and restoration also retain the pre-crop sketch and its undo stack.
        await toolDrag('rectangle', [.2, .2], [.4, .4]);
        await toolDrag('crop', [.1, .1], [.9, .9]);
        page.once('dialog', dialog => dialog.accept()); await page.locator('#apply-crop').click();
        await page.waitForFunction(() => document.querySelector('#counts').textContent.includes('표시 0개'));
        const secondCrop = page.url();
        await page.locator('#undo').click();
        await page.waitForFunction(() => document.querySelector('#counts').textContent.includes('표시 1개'));
        assert.ok((await sample(.2, .3))[3] > 200);
        await page.locator('#undo').click(); assert.match(await page.locator('#counts').textContent(), /표시 0개/);
        await page.locator('#redo').click(); assert.match(await page.locator('#counts').textContent(), /표시 1개/);
        await page.locator('#redo').click(); await page.waitForURL(secondCrop);
        await page.locator('#undo').click();
        await page.waitForFunction(() => document.querySelector('#counts').textContent.includes('표시 1개'));
        await page.locator('#undo').click(); assert.match(await page.locator('#counts').textContent(), /표시 0개/);

        // Replacing a photo is also a single step; failure cannot consume undo history.
        await toolDrag('rectangle', [.2, .2], [.4, .4]);
        const priorPhotoURL = page.url();
        const priorPhotoAPI = baseURL + '/api/projects/' + priorPhotoURL.split('/').filter(Boolean).at(-1) + '/';
        const priorPhoto = await context.request.get(priorPhotoAPI).then(r => r.json());
        page.once('dialog', dialog => dialog.accept());
        await page.locator('#drop-zone').dispatchEvent('drop', {dataTransfer: await makeTransfer()});
        await page.waitForURL(url => url.toString() !== priorPhotoURL && url.pathname.includes('/editor/'));
        const replacementURL = page.url();
        assert.match(await page.locator('#counts').textContent(), /표시 0개/);
        await page.route(priorPhotoAPI, route => route.fulfill({status: 200, contentType:'application/json', body:JSON.stringify({...priorPhoto, revision:priorPhoto.revision + 1})}));
        await page.locator('#undo').click();
        await page.waitForFunction(() => document.querySelector('#status').textContent.includes('다른 탭'));
        assert.equal(page.url(), replacementURL); assert.match(await page.locator('#counts').textContent(), /표시 0개/);
        assert.equal(await page.locator('#undo').isDisabled(), false);
        await page.unroute(priorPhotoAPI);
        await page.locator('#undo').click(); await page.waitForURL(priorPhotoURL);
        assert.match(await page.locator('#counts').textContent(), /표시 1개/);
        await page.locator('#redo').click(); await page.waitForURL(replacementURL);
        assert.match(await page.locator('#counts').textContent(), /표시 0개/);
        await page.locator('#undo').click(); await page.waitForURL(priorPhotoURL);
        await page.locator('#undo').click(); assert.match(await page.locator('#counts').textContent(), /표시 0개/);

        await toolDrag('rectangle', [.15, .2], [.7, .75]);
        assert.equal(await page.locator('#redo').isDisabled(), true);
        // Start inside the existing rectangle: create a child shape rather than moving its parent.
        await toolDrag('ellipse', [.25, .28], [.42, .42]);
        assert.match(await page.locator('#counts').textContent(), /표시 2개/);
        // A line can start on the rectangle outline and divide its interior.
        await toolDrag('line', [.48, .2], [.48, .75]);
        await page.locator('#color').fill('#172a26');
        await toolDrag('pen', [.28, .46], [.4, .51]);
        await page.locator('#color').fill('#ee3344'); await page.locator('#fill').click(); await click(.22, .6);
        let pixel = await sample(.22, .6);
        assert.deepEqual(pixel, [238, 51, 68, 255]);
        assert.equal((await sample(.6, .6))[3], 0);
        assert.equal((await sample(.1, .6))[3], 0);
        assert.equal((await sample(.335, .35))[3], 0);
        await page.locator('#color').fill('#4477ee'); await click(.6, .6);
        assert.deepEqual(await sample(.6, .6), [68, 119, 238, 255]);
        assert.deepEqual(await sample(.22, .6), [238, 51, 68, 255]);
        await page.locator('#color').fill('#66bb55'); await click(.335, .35);
        assert.deepEqual(await sample(.335, .35), [102, 187, 85, 255]);
        assert.deepEqual(await sample(.28, .46), [23, 42, 38, 255]);
        // Only the parent and its fill move. Nested ellipse and divider stay independent.
        await page.locator('#rectangle').click(); await drag([.15, .5], [.2, .5]);
        assert.deepEqual(await sample(.27, .6), [238, 51, 68, 255]);
        assert.deepEqual(await sample(.65, .6), [68, 119, 238, 255]);
        assert.deepEqual(await sample(.335, .35), [102, 187, 85, 255]);
        await drag([.747, .747], [.797, .797]);
        assert.deepEqual(await sample(.3, .65), [238, 51, 68, 255]);
        // Filled interior selects the parent; selected outline thickness changes, then undo.
        await page.locator('#rectangle').click(); await click(.3, .65);
        await page.locator('#thickness').fill('25'); await page.locator('#thickness').dispatchEvent('change');
        await page.keyboard.press('Control+z');
        await page.locator('#box-select').click(); await click(.3, .65);
        await page.keyboard.press('Control+c'); await page.keyboard.press('Control+v');
        assert.match(await page.locator('#counts').textContent(), /표시 9개/);
        await page.keyboard.press('Control+z'); assert.match(await page.locator('#counts').textContent(), /표시 6개/);
        await page.keyboard.press('Control+y'); assert.match(await page.locator('#counts').textContent(), /표시 9개/);
        await page.keyboard.press('Control+z'); assert.match(await page.locator('#counts').textContent(), /표시 6개/);
        // Erase only touched pixels, retaining the rest of the filled shape.
        await page.locator('#eraser').click(); await click(.3, .65);
        assert.equal((await sample(.3, .65))[3], 0);
        assert.deepEqual(await sample(.35, .65), [238, 51, 68, 255]);
        assert.match(await page.locator('#counts').textContent(), /표시 6개/);
        await page.keyboard.press('Control+z'); assert.deepEqual(await sample(.3, .65), [238, 51, 68, 255]);
        await page.keyboard.press('Control+y'); assert.equal((await sample(.3, .65))[3], 0);
        // Keep the erased region in the search snapshot to verify restoration.

        await toolDrag('pen', [.8, .08], [.9, .12]); await drag([.8, .13], [.9, .18]); await drag([.8, .18], [.9, .22]);
        await page.locator('#text').click(); await click(.1, .9);
        await page.keyboard.insertText('화분');
        await page.locator('#inline-text').dispatchEvent('keydown', {key: 'Enter', isComposing: true});
        assert.equal(await page.locator('#inline-text').isVisible(), true);
        await page.keyboard.press('Enter');
        await capture('desktop-edit');
        await page.locator('#search-top').click(); await page.waitForURL('**/results/**'); await page.waitForSelector('.placement');
        const result = JSON.parse(await page.locator('#result-data').textContent()), resultURL = page.url();
        assert.equal(result.placements.length, 1);
        assert.equal(await page.locator('.placement').count(), 1);
        assert.equal(await page.locator('#pen, #upload, #surface').count(), 0);
        assert.equal(await page.locator('.product-card').count(), 3);
        let data = await context.request.get(baseURL + result.data_url).then(r => r.json());
        assert.equal(data.input_document.strokes.length, 4);
        assert.equal(data.input_document.items.filter(row => row.kind === 'fill').length, 3);
        assert.ok(data.input_document.items.some(row => row.cuts?.length));
        assert.equal(result.placements[0].product_id, 'plant-01');
        const item = page.locator('.placement').first(); let box = await item.boundingBox();
        await page.mouse.move(box.x + box.width / 3, box.y + box.height / 3); await page.mouse.down();
        await page.mouse.move(box.x + box.width / 3 - 25, box.y + box.height / 3 - 15, {steps: 6});
        const saved = page.waitForResponse(response => response.url().endsWith('/placements/'));
        await page.mouse.up(); assert.equal((await saved).status(), 200);
        await page.waitForFunction(() => !document.querySelector('#download-png').disabled);
        for (const [id, name] of [['download-png', 'sketchon-result.png']]) {
            const download = page.waitForEvent('download'); await page.locator('#' + id).click();
            assert.equal((await download).suggestedFilename(), name);
        }
        await capture('desktop');

        await page.locator('#edit-sketch').click(); await page.waitForURL('**/editor/**');
        await page.waitForFunction(() => !document.querySelector('#room-photo').hidden);
        const restored = JSON.parse(await page.locator('#initial-data').textContent());
        assert.deepEqual(restored.document, data.input_document);
        assert.equal(await page.locator('#visitor-data').textContent(), visitor);
        assert.equal(await page.locator('#undo').isDisabled(), true);
        assert.equal((await sample(.3, .65))[3], 0);
        assert.deepEqual(await sample(.35, .65), [238, 51, 68, 255]);

        await page.goto(resultURL); await page.locator('#redraw').click(); await page.waitForURL('**/editor/**');
        await page.waitForFunction(() => !document.querySelector('#room-photo').hidden);
        assert.match(await page.locator('#counts').textContent(), /^0획 · 표시 0개/);
        assert.equal(await page.locator('#visitor-data').textContent(), visitor);
        assert.equal((await sample(.3, .65))[3], 0);

        // Touch crop/fill and draw on a phone viewport.
        await page.setViewportSize({width: 390, height: 844});
        await toolDrag('rectangle', [.15, .2], [.7, .75]);
        await page.locator('#fill').click(); await page.locator('#color').fill('#ee3344');
        await page.locator('#surface').scrollIntoViewIfNeeded(); box = await page.locator('#surface').boundingBox();
        const cdp = await context.newCDPSession(page), touch = (type, points) => cdp.send('Input.dispatchTouchEvent', {type, touchPoints: points});
        const first = {id: 1, x: box.x + box.width * .3, y: box.y + box.height * .5, force: .6};
        await touch('touchStart', [first]); await touch('touchEnd', []);
        assert.deepEqual(await sample(.3, .5), [238, 51, 68, 255]);
        await capture('mobile-edit');
        await page.locator('#search-mobile').click(); await page.waitForURL('**/results/**'); await page.waitForSelector('.placement');
        assert.equal(await page.locator('.placement').count(), 1); await capture('mobile');
        await page.goto(resultURL); await page.locator('#new-search').click(); await page.waitForURL('**/editor/**');
        await page.waitForSelector('#surface');
        assert.equal(await page.locator('#room-photo').isVisible(), false);
        assert.match(await page.locator('#counts').textContent(), /^0획 · 표시 0개/);
        assert.equal(await page.locator('#visitor-data').textContent(), visitor);
        await page.reload(); assert.equal(await page.locator('#visitor-data').textContent(), visitor);
        await page.goto(baseURL); await page.locator('.start-button').click(); assert.equal(await page.locator('#visitor-data').textContent(), visitor);
        assert.deepEqual(errors, []);
        assert.ok(events.some(event => event.event_type === 'fill.applied'));
        assert.ok(events.some(event => event.event_type === 'selection.pasted'));
        console.log('PASS: unified upload/crop/sketch undo and redo including reload, photo replacement, redo invalidation after branching, 10MB alert, partial erasure, connected and explicit groups, selected line width, one mock product, 3 product slots, PNG/JSON, stable visitor, mobile touch, no browser errors.');
    } finally { await browser.close(); }
})().catch(error => { console.error(error); process.exit(1); });

