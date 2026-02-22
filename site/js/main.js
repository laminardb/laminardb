/* ============================================
   LaminarDB v2 — Interactivity & Animations
   ============================================ */

document.addEventListener('DOMContentLoaded', () => {

  // ---- Navigation scroll effect ----
  const nav = document.querySelector('.nav');
  const onScroll = () => {
    nav.classList.toggle('scrolled', window.scrollY > 40);
  };
  window.addEventListener('scroll', onScroll, { passive: true });
  onScroll();

  // ---- Sticky top bar (appears after scrolling past hero) ----
  const stickyBar = document.getElementById('sticky-bar');
  const heroSection = document.getElementById('hero');
  if (stickyBar && heroSection) {
    const stickyObserver = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            stickyBar.classList.remove('visible');
            stickyBar.setAttribute('aria-hidden', 'true');
          } else {
            stickyBar.classList.add('visible');
            stickyBar.setAttribute('aria-hidden', 'false');
          }
        });
      },
      { threshold: 0, rootMargin: '-80px 0px 0px 0px' }
    );
    stickyObserver.observe(heroSection);

    // Copy on click for sticky bar install
    const stickyInstall = stickyBar.querySelector('.sticky-bar-install');
    if (stickyInstall) {
      stickyInstall.addEventListener('click', () => {
        const cmd = stickyInstall.querySelector('.cmd')?.textContent || '';
        navigator.clipboard.writeText(cmd).then(() => {
          const cmdEl = stickyInstall.querySelector('.cmd');
          if (cmdEl) {
            const original = cmdEl.textContent;
            cmdEl.textContent = 'Copied!';
            cmdEl.style.color = 'var(--green-400)';
            setTimeout(() => {
              cmdEl.textContent = original;
              cmdEl.style.color = '';
            }, 1500);
          }
        });
      });
    }
  }

  // ---- Active nav link highlight ----
  const sections = document.querySelectorAll('section[id]');
  const navLinks = document.querySelectorAll('.nav-links a[href^="#"]');
  const highlightNav = () => {
    const scrollY = window.scrollY + 120;
    sections.forEach((section) => {
      const top = section.offsetTop;
      const height = section.offsetHeight;
      const id = section.getAttribute('id');
      if (scrollY >= top && scrollY < top + height) {
        navLinks.forEach((link) => {
          link.classList.toggle('active', link.getAttribute('href') === '#' + id);
        });
      }
    });
  };
  window.addEventListener('scroll', highlightNav, { passive: true });

  // ---- Smooth scroll ----
  document.querySelectorAll('a[href^="#"]').forEach((link) => {
    link.addEventListener('click', (e) => {
      const href = link.getAttribute('href');
      if (href === '#') return;
      const target = document.querySelector(href);
      if (target) {
        e.preventDefault();
        target.scrollIntoView({ behavior: 'smooth' });
        mobileMenu?.classList.remove('open');
        navToggle?.classList.remove('open');
      }
    });
  });

  // ---- Mobile nav ----
  const navToggle = document.querySelector('.nav-toggle');
  const mobileMenu = document.querySelector('.mobile-menu');
  navToggle?.addEventListener('click', () => {
    navToggle.classList.toggle('open');
    mobileMenu?.classList.toggle('open');
  });

  // ---- Scroll reveal (IntersectionObserver) ----
  const revealElements = document.querySelectorAll('.reveal');
  if (revealElements.length > 0) {
    const revealObserver = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            entry.target.classList.add('visible');
            revealObserver.unobserve(entry.target);
          }
        });
      },
      { threshold: 0.1, rootMargin: '0px 0px -40px 0px' }
    );
    revealElements.forEach((el) => revealObserver.observe(el));
  }

  // ---- Animated counters ----
  function animateCounter(el, from, to, duration, suffix) {
    const start = performance.now();
    suffix = suffix || '';
    const step = (now) => {
      const elapsed = now - start;
      const progress = Math.min(elapsed / duration, 1);
      // Ease-out expo
      const eased = progress === 1 ? 1 : 1 - Math.pow(2, -10 * progress);
      const current = Math.round(from + (to - from) * eased);
      el.textContent = current.toLocaleString() + suffix;
      if (progress < 1) {
        requestAnimationFrame(step);
      }
    };
    requestAnimationFrame(step);
  }

  // Observe counter elements
  document.querySelectorAll('[data-count-to]').forEach((el) => {
    let counted = false;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && !counted) {
          counted = true;
          const to = parseInt(el.dataset.countTo, 10);
          const from = parseInt(el.dataset.countFrom || '0', 10);
          const duration = parseInt(el.dataset.countDuration || '2000', 10);
          const suffix = el.dataset.countSuffix || '';
          animateCounter(el, from, to, duration, suffix);
          observer.unobserve(el);
        }
      },
      { threshold: 0.5 }
    );
    observer.observe(el);
  });

  // ---- Code tabs ----
  const tabButtons = document.querySelectorAll('.tab-btn');
  tabButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      const target = btn.dataset.tab;
      if (!target) return; // Skip buttons without a data-tab (e.g., Python showcase static tab)
      const container = btn.closest('.code-block');
      const buttons = container.querySelectorAll('.tab-btn');
      const panels = container.querySelectorAll('.tab-panel');
      buttons.forEach((b) => b.classList.toggle('active', b === btn));
      panels.forEach((p) => p.classList.toggle('active', p.id === target));
    });
  });

  // ---- Copy code button ----
  document.querySelectorAll('.copy-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      const container = btn.closest('.code-block');
      const activePanel = container?.querySelector('.tab-panel.active');
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

  // ---- Install snippet copy (all .hero-install elements) ----
  document.querySelectorAll('.hero-install').forEach((installSnippet) => {
    installSnippet.addEventListener('click', () => {
      const cmd = installSnippet.querySelector('.cmd')?.textContent || '';
      navigator.clipboard.writeText(cmd).then(() => {
        const hint = installSnippet.querySelector('.copy-hint');
        if (hint) {
          const original = hint.textContent;
          hint.textContent = 'Copied!';
          hint.style.color = 'var(--green-400)';
          hint.style.borderColor = 'var(--green-400)';
          setTimeout(() => {
            hint.textContent = original;
            hint.style.color = '';
            hint.style.borderColor = '';
          }, 2000);
        }
      });
    });
  });

  // ---- Install language tabs (Rust/Python toggle in hero) ----
  document.querySelectorAll('.install-tab').forEach((tab) => {
    tab.addEventListener('click', () => {
      const lang = tab.dataset.install;
      const wrapper = tab.closest('.hero-install-wrapper');
      if (!wrapper) return;

      // Toggle tab active state
      wrapper.querySelectorAll('.install-tab').forEach((t) => {
        t.classList.toggle('active', t === tab);
      });

      // Toggle install panels
      wrapper.querySelectorAll('.hero-install[data-install-panel]').forEach((panel) => {
        panel.style.display = panel.dataset.installPanel === lang ? 'inline-flex' : 'none';
      });
    });
  });

  // ---- Floating particles ----
  const particlesContainer = document.querySelector('.hero-particles');
  if (particlesContainer) {
    const count = 20;
    for (let i = 0; i < count; i++) {
      const particle = document.createElement('div');
      particle.className = 'particle';
      particle.style.left = Math.random() * 100 + '%';
      particle.style.animationDuration = (8 + Math.random() * 12) + 's';
      particle.style.animationDelay = (Math.random() * 10) + 's';
      particle.style.width = (1 + Math.random() * 2) + 'px';
      particle.style.height = particle.style.width;
      particle.style.opacity = 0;
      particlesContainer.appendChild(particle);
    }
  }

  // ---- Subtle parallax on hero ----
  const heroGlow = document.querySelector('.hero-glow');
  if (heroGlow && window.matchMedia('(prefers-reduced-motion: no-preference)').matches) {
    let ticking = false;
    window.addEventListener('scroll', () => {
      if (!ticking) {
        requestAnimationFrame(() => {
          const scrollY = window.scrollY;
          if (scrollY < window.innerHeight) {
            heroGlow.style.transform = `translateY(${scrollY * 0.15}px)`;
          }
          ticking = false;
        });
        ticking = true;
      }
    }, { passive: true });
  }
});
