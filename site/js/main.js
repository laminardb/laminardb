/* LaminarDB — Interactive modern client logic. */

document.addEventListener('DOMContentLoaded', () => {

  // --- Mobile nav toggle ---
  const navToggle = document.querySelector('.nav-toggle');
  const mobileMenu = document.querySelector('.mobile-menu');
  if (navToggle && mobileMenu) {
    navToggle.addEventListener('click', () => {
      const isOpen = navToggle.classList.toggle('open');
      mobileMenu.classList.toggle('open');
      navToggle.setAttribute('aria-expanded', isOpen ? 'true' : 'false');
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
          navToggle?.setAttribute('aria-expanded', 'false');
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
        const hint = el.querySelector('.copy-hint');
        if (navigator.clipboard) {
          navigator.clipboard.writeText(cmd)
            .then(() => {
              if (hint) {
                const original = hint.textContent;
                hint.textContent = 'copied!';
                hint.style.color = 'var(--green)';
                setTimeout(() => {
                  hint.textContent = original;
                  hint.style.color = '';
                }, 2000);
               }
            })
            .catch((err) => {
              console.error('Failed to copy command to clipboard:', err);
              if (hint) {
                const original = hint.textContent;
                hint.textContent = 'copy failed';
                hint.style.color = 'var(--red)';
                setTimeout(() => {
                  hint.textContent = original;
                  hint.style.color = '';
                }, 2000);
              }
            });
        } else {
          console.warn('Clipboard API not available');
          if (hint) {
            const original = hint.textContent;
            hint.textContent = 'unsupported';
            hint.style.color = 'var(--red)';
            setTimeout(() => {
              hint.textContent = original;
              hint.style.color = '';
            }, 2000);
          }
        }
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
        if (navigator.clipboard) {
          navigator.clipboard.writeText(code)
            .then(() => {
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
            })
            .catch((err) => {
              console.error('Failed to copy code to clipboard:', err);
              btn.classList.add('copy-error');
              const originalHtml = btn.innerHTML;
              btn.innerHTML = `
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round" style="width:14px;height:14px;"><line x1="18" y1="6" x2="6" y2="18"></line><line x1="6" y1="6" x2="18" y2="18"></line></svg>
                Failed
              `;
              setTimeout(() => {
                btn.classList.remove('copy-error');
                btn.innerHTML = originalHtml;
              }, 2000);
            });
        } else {
          console.warn('Clipboard API not available');
          btn.classList.add('copy-error');
          const originalHtml = btn.innerHTML;
          btn.innerHTML = 'Unsupported';
          setTimeout(() => {
            btn.classList.remove('copy-error');
            btn.innerHTML = originalHtml;
          }, 2000);
        }
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
        const block = container.closest('.code-block');
        if (block) {
          const rect = block.getBoundingClientRect();
          if (rect.top < 0 || rect.bottom > window.innerHeight) {
            block.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
          }
        }
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
      if (fmt === 'tag') el.textContent = tag;
      else if (fmt === 'bare') el.textContent = bare;
    });
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

    const particles = [];
    const count = 48;
    let rafId = null;
    let isRunning = false;

    const resizeHandler = () => {
      width = canvas.width = canvas.offsetWidth;
      height = canvas.height = canvas.offsetHeight;
      particles.forEach(p => {
        p.x = p.progress * width;
        p.baseY = height / 2 + Math.sin(p.progress * Math.PI * 3.5) * 70;
      });
    };

    window.addEventListener('resize', resizeHandler);

    // Color structured component helper
    const parseRgba = (colorStr) => {
      const match = colorStr.match(/rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)/);
      if (match) {
        return {
          r: parseInt(match[1], 10),
          g: parseInt(match[2], 10),
          b: parseInt(match[3], 10),
          a: match[4] !== undefined ? parseFloat(match[4]) : 1.0
        };
      }
      // Fallback
      return { r: 14, g: 165, b: 233, a: 0.45 };
    };

    const setAlpha = (colorStr, alpha) => {
      const c = parseRgba(colorStr);
      return `rgba(${c.r}, ${c.g}, ${c.b}, ${alpha})`;
    };

    for (let i = 0; i < count; i++) {
      const progress = i / count;
      particles.push({
        progress: progress,
        x: progress * width,
        baseY: height / 2 + Math.sin(progress * Math.PI * 3.5) * 70,
        y: 0,
        angle: progress * Math.PI * 4,
        speed: 0.01 + Math.random() * 0.008,
        amplitude: 50 + Math.random() * 30,
        radius: Math.random() * 2.5 + 1.5,
        color: i % 2 === 0 ? 'rgba(14, 165, 233, 0.45)' : 'rgba(139, 92, 246, 0.45)'
      });
    }

    function animate() {
      if (!isRunning) return;
      ctx.clearRect(0, 0, width, height);

      particles.forEach(p => {
        p.angle += p.speed;
        p.y = p.baseY + Math.sin(p.angle) * p.amplitude;
      });

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
            lineGrad.addColorStop(0, setAlpha(particles[i].color, alpha));
            lineGrad.addColorStop(1, setAlpha(particles[j].color, alpha));
            
            ctx.strokeStyle = lineGrad;
            ctx.lineWidth = 1.2;
            ctx.stroke();
          }
        }
      }

      particles.forEach(p => {
        ctx.beginPath();
        ctx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
        ctx.fillStyle = p.color;
        ctx.fill();
      });

      rafId = requestAnimationFrame(animate);
    }

    const startAnimation = () => {
      if (!isRunning) {
        isRunning = true;
        animate();
      }
    };

    const stopAnimation = () => {
      if (isRunning) {
        isRunning = false;
        if (rafId) {
          cancelAnimationFrame(rafId);
          rafId = null;
        }
      }
    };

    const teardown = () => {
      stopAnimation();
      window.removeEventListener('resize', resizeHandler);
      if (observer) {
        observer.disconnect();
      }
    };

    // Lifecycle visibility check
    const visibilityHandler = () => {
      if (document.hidden) {
        stopAnimation();
      } else if (isCanvasOnScreen) {
        startAnimation();
      }
    };
    document.addEventListener('visibilitychange', visibilityHandler);

    // Lifecycle IntersectionObserver
    let isCanvasOnScreen = true;
    const observer = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        isCanvasOnScreen = entry.isIntersecting;
        if (isCanvasOnScreen && !document.hidden) {
          startAnimation();
        } else {
          stopAnimation();
        }
      });
    }, { threshold: 0.05 });
    observer.observe(canvas);

    // Lifecycle DOM removal cleanup using MutationObserver
    const domObserver = new MutationObserver(() => {
      if (!document.body.contains(canvas)) {
        teardown();
        document.removeEventListener('visibilitychange', visibilityHandler);
        domObserver.disconnect();
      }
    });
    if (canvas.parentNode) {
      domObserver.observe(canvas.parentNode, { childList: true });
    }
  }

});
