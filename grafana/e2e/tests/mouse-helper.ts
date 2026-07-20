import type { Page } from '@playwright/test';

// Playwright videos do not render the OS mouse cursor. installMouseHelper injects a
// visible fake cursor that follows Playwright's synthetic mouse events and animates
// on click, so the recorded walkthrough shows where the pointer is and when it
// clicks. Call it before the first navigation (init scripts persist across pages).
export async function installMouseHelper(page: Page): Promise<void> {
  await page.addInitScript(() => {
    // Guard against double-install across navigations.
    const w = window as unknown as { __mouseHelper?: boolean };
    if (w.__mouseHelper) {
      return;
    }
    w.__mouseHelper = true;

    const install = () => {
      const cursor = document.createElement('div');
      cursor.setAttribute('data-mouse-helper', '');
      const style = document.createElement('style');
      style.textContent = `
        [data-mouse-helper] {
          pointer-events: none; position: absolute; top: 0; left: 0;
          z-index: 2147483647; width: 22px; height: 22px; margin: -11px 0 0 -11px;
          border: 2px solid rgba(20,20,20,.85); border-radius: 50%;
          background: rgba(255,255,255,.35);
          box-shadow: 0 0 0 2px rgba(255,255,255,.7), 0 1px 4px rgba(0,0,0,.4);
          /* Ease left/top so the cursor GLIDES between targets instead of
             teleporting (Playwright moves the mouse in one hop). slowMo (400ms)
             > this duration, so it arrives before the click fires. */
          transition: left .32s ease-out, top .32s ease-out, transform .12s ease, background .12s ease;
        }
        [data-mouse-helper].down {
          transform: scale(.55); background: rgba(56,128,255,.75);
        }
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
