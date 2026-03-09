/* LaminarDB — Minimal interactivity. No animations, no particles. */

document.addEventListener('DOMContentLoaded', () => {

  // --- Mobile nav toggle ---
  const navToggle = document.querySelector('.nav-toggle');
  const mobileMenu = document.querySelector('.mobile-menu');
  if (navToggle && mobileMenu) {
    navToggle.addEventListener('click', () => {
      navToggle.classList.toggle('open');
      mobileMenu.classList.toggle('open');
    });
  }

  // --- Smooth scroll for anchor links ---
  document.querySelectorAll('a[href^="#"]').forEach((link) => {
    link.addEventListener('click', (e) => {
      const href = link.getAttribute('href');
      if (href === '#') return;
      const target = document.querySelector(href);
      if (target) {
        e.preventDefault();
        target.scrollIntoView({ behavior: 'smooth' });
        // Close mobile menu if open
        if (mobileMenu) {
          mobileMenu.classList.remove('open');
          navToggle?.classList.remove('open');
        }
      }
    });
  });

  // --- Code tabs ---
  document.querySelectorAll('.code-tab').forEach((tab) => {
    tab.addEventListener('click', () => {
      const targetId = tab.dataset.tab;
      if (!targetId) return;
      const block = tab.closest('.code-block');
      if (!block) return;

      block.querySelectorAll('.code-tab').forEach((t) => {
        t.classList.toggle('active', t === tab);
      });
      block.querySelectorAll('.tab-panel').forEach((panel) => {
        panel.classList.toggle('active', panel.id === targetId);
      });
    });
  });

  // --- Copy code button ---
  document.querySelectorAll('.copy-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      const block = btn.closest('.code-block');
      if (!block) return;
      const activePanel = block.querySelector('.tab-panel.active');
      const code = activePanel?.querySelector('code')?.textContent || '';
      navigator.clipboard.writeText(code).then(() => {
        const original = btn.innerHTML;
        btn.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg> Copied';
        btn.classList.add('copied');
        setTimeout(() => {
          btn.innerHTML = original;
          btn.classList.remove('copied');
        }, 2000);
      });
    });
  });

  // --- Hero install command copy ---
  document.querySelectorAll('.hero-install').forEach((el) => {
    el.addEventListener('click', () => {
      const cmd = el.querySelector('.cmd')?.textContent || '';
      navigator.clipboard.writeText(cmd).then(() => {
        const hint = el.querySelector('.copy-hint');
        if (hint) {
          const original = hint.textContent;
          hint.textContent = 'copied!';
          hint.style.color = '#22c55e';
          setTimeout(() => {
            hint.textContent = original;
            hint.style.color = '';
          }, 1500);
        }
      });
    });
    el.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        el.click();
      }
    });
  });

});
