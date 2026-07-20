import type { Locator, Page } from '@playwright/test';

// Playwright videos do not render the OS mouse cursor, and its high-level actions
// teleport the mouse to each target in a single hop. This module (a) injects a
// visible fake cursor that tracks the synthetic mouse exactly (no lag), and
// (b) provides glide* helpers that move the mouse to a target in timed steps -- so
// in the recording the cursor visibly travels between elements and ARRIVES before
// the click fires (the earlier CSS-transition approach lagged the real mouse, so
// clicks appeared to happen before the cursor got there).

export async function installMouseHelper(page: Page): Promise<void> {
  await page.addInitScript(() => {
    const w = window as unknown as { __mouseHelper?: boolean };
    if (w.__mouseHelper) {
      return;
    }
    w.__mouseHelper = true;

    const install = () => {
      const cursor = document.createElement('div');
      cursor.setAttribute('data-mouse-helper', '');
      const style = document.createElement('style');
      // No left/top transition: the cursor sits exactly where the (stepped) mouse
      // is, so a click lands under it. Only the press animation is eased.
      style.textContent = `
        [data-mouse-helper] {
          pointer-events: none; position: absolute; top: 0; left: 0;
          z-index: 2147483647; width: 22px; height: 22px; margin: -11px 0 0 -11px;
          border: 2px solid rgba(20,20,20,.85); border-radius: 50%;
          background: rgba(255,255,255,.35);
          box-shadow: 0 0 0 2px rgba(255,255,255,.7), 0 1px 4px rgba(0,0,0,.4);
          transition: transform .1s ease, background .1s ease;
        }
        [data-mouse-helper].down { transform: scale(.55); background: rgba(56,128,255,.75); }
      `;
      document.head.appendChild(style);
      document.body.appendChild(cursor);

      document.addEventListener(
        'mousemove',
        (e) => {
          cursor.style.left = e.pageX + 'px';
          cursor.style.top = e.pageY + 'px';
        },
        true
      );
      document.addEventListener('mousedown', () => cursor.classList.add('down'), true);
      document.addEventListener('mouseup', () => cursor.classList.remove('down'), true);
    };

    if (document.readyState === 'loading') {
      window.addEventListener('DOMContentLoaded', install);
    } else {
      install();
    }
  });
}

type Point = { x: number; y: number };

// cursorViewportPos reads the fake cursor's current VIEWPORT position (via
// getBoundingClientRect, so it is correct regardless of scroll). Because the cursor
// tracks the real mouse -- including moves made by Playwright's own high-level
// actions -- reading it means each glide starts from where the mouse actually is,
// avoiding a jump after a plugin-e2e helper moved the mouse.
async function cursorViewportPos(page: Page): Promise<Point> {
  return page.evaluate(() => {
    const c = document.querySelector('[data-mouse-helper]');
    if (!c) {
      return { x: 80, y: 80 };
    }
    const r = c.getBoundingClientRect();
    return { x: r.x + r.width / 2, y: r.y + r.height / 2 };
  });
}

async function centerOf(locator: Locator): Promise<Point> {
  await locator.scrollIntoViewIfNeeded();
  const box = await locator.boundingBox();
  if (!box) {
    throw new Error('glide: element has no bounding box');
  }
  return { x: box.x + box.width / 2, y: box.y + box.height / 2 };
}

// glideToLocator moves the mouse from its current position to the element's center
// in timed steps, so the recorded cursor travels there smoothly and ends on it.
export async function glideToLocator(
  page: Page,
  locator: Locator,
  { steps = 22, delay = 16 }: { steps?: number; delay?: number } = {}
): Promise<void> {
  const from = await cursorViewportPos(page);
  const to = await centerOf(locator);
  for (let i = 1; i <= steps; i++) {
    const t = i / steps;
    await page.mouse.move(from.x + (to.x - from.x) * t, from.y + (to.y - from.y) * t);
    await page.waitForTimeout(delay);
  }
}

// glideClick glides to the element, then clicks it -- the mouse is already on the
// target, so the click lands under the visible cursor.
export async function glideClick(page: Page, locator: Locator, opts?: { steps?: number; delay?: number }): Promise<void> {
  await glideToLocator(page, locator, opts);
  await locator.click();
}
