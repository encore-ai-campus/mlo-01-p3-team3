// Run against an isolated local Django server; no recommendation or paid API calls.
const assert = require('node:assert/strict');
const {chromium} = require('playwright');
const base = process.env.ROOMPICK_TEST_URL || 'http://127.0.0.1:18037';

(async () => {
    const browser = await chromium.launch({headless: true, channel: 'msedge'});
    try {
        for (const width of [1440, 390, 320]) {
            const context = await browser.newContext({viewport: {width, height: 1000}});
            try {
                const page = await context.newPage(), errors = [];
                page.on('pageerror', error => errors.push(error.message));
                await require('./browser-member.cjs')(context, base);
                await page.goto(base);
                await page.locator('#start-sketch').click();
                await page.waitForFunction(() => Object.keys(sessionStorage).some(key => key.startsWith('local_demo.history.')) && !document.querySelector('#sample-empty').disabled);
                await page.locator('#sample-empty').click();
                await page.waitForFunction(() => !document.querySelector('#room-photo').hidden && !document.querySelector('#search').disabled);
                await page.locator('[data-sticker="sofa-cream"]').click();
                await page.waitForFunction(() => Object.keys(sessionStorage).some(key => key.startsWith('local_demo.history.') && JSON.parse(sessionStorage.getItem(key)).drawing.items.some(item => item.kind === 'sticker')));
                const item = () => page.evaluate(() => {
                    const key = Object.keys(sessionStorage).find(key => key.startsWith('local_demo.history.'));
                    return JSON.parse(sessionStorage.getItem(key)).drawing.items.find(item => item.kind === 'sticker');
                });
                const drag = async (value, dx, dy, corner = true) => {
                    await page.locator('#surface').scrollIntoViewIfNeeded();
                    const box = await page.locator('#surface').boundingBox();
                    const inset = corner ? Math.min(8, value.width * box.width / 4, value.height * box.height / 4) : 0;
                    const x = box.x + (value.x + value.width * (corner ? 1 : .5)) * box.width - inset;
                    const y = box.y + (value.y + value.height * (corner ? 1 : .5)) * box.height - inset;
                    await page.mouse.move(x, y);
                    await page.mouse.down();
                    await page.mouse.move(x + dx, y + dy, {steps: 8});
                    await page.mouse.up();
                };
                const initial = await item();
                await drag(initial, 25, 20);
                const expanded = await item();
                assert.ok(expanded.width > initial.width && expanded.height > initial.height, `corner enlarges sticker at ${width}px`);
                assert.equal(expanded.x, initial.x, 'resizing keeps the top left corner fixed');
                assert.equal(expanded.y, initial.y);
                await drag(expanded, -15, -12);
                const shrunk = await item();
                assert.ok(shrunk.width < expanded.width && shrunk.height < expanded.height, 'corner shrinks sticker');
                await page.locator('#undo').click();
                assert.deepEqual(await item(), expanded);
                await page.locator('#redo').click();
                assert.deepEqual(await item(), shrunk);
                await page.locator('#surface').scrollIntoViewIfNeeded();
                const box = await page.locator('#surface').boundingBox();
                await page.mouse.click(box.x + (shrunk.x + shrunk.width / 2) * box.width, box.y + (shrunk.y + shrunk.height / 2) * box.height);
                await page.locator('#resize-selection').click();
                await drag(shrunk, 20, 15, false);
                const buttonResized = await item();
                assert.ok(buttonResized.width > shrunk.width && buttonResized.height > shrunk.height, 'toolbar resize remains available');
                await page.reload();
                await page.waitForFunction(() => !document.querySelector('#search').disabled);
                assert.deepEqual(await item(), buttonResized, 'reload restores sticker size');
                assert.deepEqual(errors, []);
                console.log(`PASS ${width}px: corner enlarge/shrink, toolbar resize, undo/redo, reload`);
            } finally {
                await context.close();
            }
        }
    } finally {
        await browser.close();
    }
})().catch(error => { console.error(error); process.exitCode = 1; });
