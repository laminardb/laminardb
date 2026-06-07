/* LaminarDB — Interactive modern client logic. */

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

  // --- Copy CLI command ---
  document.querySelectorAll('.hero-install').forEach((el) => {
    el.addEventListener('click', () => {
      const cmd = el.querySelector('.cmd')?.textContent;
      if (cmd) {
        navigator.clipboard.writeText(cmd).then(() => {
          const hint = el.querySelector('.copy-hint');
          if (hint) {
            const original = hint.textContent;
            hint.textContent = 'copied!';
            hint.style.color = 'var(--green)';
            setTimeout(() => {
              hint.textContent = original;
              hint.style.color = '';
            }, 2000);
          }
        });
      }
    });

    // Keyboard accessibility
    el.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') {
        e.preventDefault();
        el.click();
      }
    });
  });

  // --- Copy code button ---
  document.querySelectorAll('.copy-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      const block = btn.closest('.code-block');
      if (!block) return;
      const activePanel = block.querySelector('.tab-panel.active');
      const code = activePanel ? activePanel.querySelector('code')?.textContent : block.querySelector('code')?.textContent;
      if (code) {
        navigator.clipboard.writeText(code).then(() => {
          btn.classList.add('copied');
          const originalHtml = btn.innerHTML;
          btn.innerHTML = `
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="width:14px;height:14px;"><polyline points="20 6 9 17 4 12"></polyline></svg>
            Copied
          `;
          setTimeout(() => {
            btn.classList.remove('copied');
            btn.innerHTML = originalHtml;
          }, 2000);
        });
      }
    });
  });

  // --- Expand/Collapse Code ---
  document.querySelectorAll('.expand-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      const container = btn.closest('.editor-container');
      if (!container) return;
      const isCollapsed = container.classList.contains('collapsed');
      if (isCollapsed) {
        container.classList.remove('collapsed');
        btn.innerHTML = `
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="width:12px;height:12px;transform:rotate(180deg)"><polyline points="6 9 12 15 18 9"></polyline></svg>
          Collapse Code
        `;
      } else {
        container.classList.add('collapsed');
        btn.innerHTML = `
          <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="width:12px;height:12px;"><polyline points="6 9 12 15 18 9"></polyline></svg>
          Expand Code
        `;
        // Scroll the container back into view if it went off screen
        container.closest('.code-block').scrollIntoView({ behavior: 'smooth', block: 'nearest' });
      }
    });
  });

  // --- Dynamic version badge ---
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

  // --- Hero Canvas Animation (Streaming Data Wave) ---
  const canvas = document.getElementById('hero-canvas');
  if (canvas) {
    const ctx = canvas.getContext('2d');
    let width = canvas.width = canvas.offsetWidth;
    let height = canvas.height = canvas.offsetHeight;

    window.addEventListener('resize', () => {
      width = canvas.width = canvas.offsetWidth;
      height = canvas.height = canvas.offsetHeight;
    });

    const particles = [];
    const count = 48;

    for (let i = 0; i < count; i++) {
      // Create a grid of points moving as a wave across the horizontal span
      const progress = i / count;
      particles.push({
        x: progress * width,
        baseY: height / 2 + Math.sin(progress * Math.PI * 3.5) * 70,
        y: 0,
        angle: progress * Math.PI * 4,
        speed: 0.01 + Math.random() * 0.008,
        amplitude: 50 + Math.random() * 30,
        radius: Math.random() * 2.5 + 1.5,
        // Cyan color and Purple color alternates
        color: i % 2 === 0 ? 'rgba(14, 165, 233, 0.45)' : 'rgba(139, 92, 246, 0.45)'
      });
    }

    function animate() {
      ctx.clearRect(0, 0, width, height);

      // Update particle heights based on wave equation
      particles.forEach(p => {
        p.angle += p.speed;
        p.y = p.baseY + Math.sin(p.angle) * p.amplitude;
      });

      // Draw connecting lines with gradients
      for (let i = 0; i < particles.length; i++) {
        for (let j = i + 1; j < particles.length; j++) {
          const dx = particles[i].x - particles[j].x;
          const dy = particles[i].y - particles[j].y;
          const dist = Math.sqrt(dx * dx + dy * dy);

          if (dist < 120) {
            ctx.beginPath();
            ctx.moveTo(particles[i].x, particles[i].y);
            ctx.lineTo(particles[j].x, particles[j].y);
            
            const lineGrad = ctx.createLinearGradient(particles[i].x, particles[i].y, particles[j].x, particles[j].y);
            const alpha = (1 - dist / 120) * 0.2;
            lineGrad.addColorStop(0, particles[i].color.replace('0.45', alpha));
            lineGrad.addColorStop(1, particles[j].color.replace('0.45', alpha));
            
            ctx.strokeStyle = lineGrad;
            ctx.lineWidth = 1.2;
            ctx.stroke();
          }
        }
      }

      // Draw particle nodes
      particles.forEach(p => {
        ctx.beginPath();
        ctx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
        ctx.fillStyle = p.color;
        ctx.fill();
      });

      requestAnimationFrame(animate);
    }
    animate();
  }

});
