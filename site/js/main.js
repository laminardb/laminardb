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

  // --- Dynamic version badge ---
  // Fetches latest release tag from GitHub API, caches in sessionStorage.
  // Elements with data-version="tag" get "vX.Y.Z", data-version="bare" get "X.Y.Z".
  const CACHE_KEY = 'laminardb_latest_version';
  const CACHE_TTL = 3600000; // 1 hour

  function applyVersion(tag) {
    const bare = tag.replace(/^v/, '');
    document.querySelectorAll('[data-version]').forEach((el) => {
      const fmt = el.getAttribute('data-version');
      el.textContent = fmt === 'bare' ? bare : tag;
    });
    // Update structured data if present
    const ld = document.querySelector('script[type="application/ld+json"]');
    if (ld) {
      try {
        const data = JSON.parse(ld.textContent);
        data.softwareVersion = bare;
        ld.textContent = JSON.stringify(data, null, 2);
      } catch (_) { /* ignore */ }
    }
  }

  const cached = sessionStorage.getItem(CACHE_KEY);
  if (cached) {
    try {
      const { tag, ts } = JSON.parse(cached);
      if (Date.now() - ts < CACHE_TTL) {
        applyVersion(tag);
      }
    } catch (_) { /* ignore stale cache */ }
  }

  fetch('https://api.github.com/repos/laminardb/laminardb/releases/latest')
    .then((r) => r.ok ? r.json() : Promise.reject())
    .then((data) => {
      if (data.tag_name) {
        applyVersion(data.tag_name);
        sessionStorage.setItem(CACHE_KEY, JSON.stringify({ tag: data.tag_name, ts: Date.now() }));
      }
    })
    .catch(() => { /* keep hardcoded fallback */ });

});
