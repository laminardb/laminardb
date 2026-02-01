/* ============================================
   LaminarDB — Interactivity
   ============================================ */

document.addEventListener('DOMContentLoaded', () => {
  /* --- Scroll-based nav background --- */
  const nav = document.querySelector('.nav');
  const onScroll = () => {
    nav.classList.toggle('scrolled', window.scrollY > 40);
  };
  window.addEventListener('scroll', onScroll, { passive: true });
  onScroll();

  /* --- Active nav link highlight --- */
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

  /* --- Smooth scroll for nav links --- */
  document.querySelectorAll('a[href^="#"]').forEach((link) => {
    link.addEventListener('click', (e) => {
      const target = document.querySelector(link.getAttribute('href'));
      if (target) {
        e.preventDefault();
        target.scrollIntoView({ behavior: 'smooth' });
        // Close mobile menu if open
        mobileMenu?.classList.remove('open');
        navToggle?.classList.remove('open');
      }
    });
  });

  /* --- Mobile nav toggle --- */
  const navToggle = document.querySelector('.nav-toggle');
  const mobileMenu = document.querySelector('.mobile-menu');
  navToggle?.addEventListener('click', () => {
    navToggle.classList.toggle('open');
    mobileMenu?.classList.toggle('open');
  });

  /* --- IntersectionObserver: scroll reveal --- */
  const revealElements = document.querySelectorAll('.reveal');
  const revealObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add('visible');
          revealObserver.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.15 }
  );
  revealElements.forEach((el) => revealObserver.observe(el));

  /* --- Latency counter animation --- */
  const latencyEl = document.getElementById('latency-counter');
  if (latencyEl) {
    let counted = false;
    const counterObserver = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting && !counted) {
          counted = true;
          animateCounter(latencyEl, 0, 325, 2000);
          counterObserver.unobserve(latencyEl);
        }
      },
      { threshold: 0.5 }
    );
    counterObserver.observe(latencyEl);
  }

  function animateCounter(el, from, to, duration) {
    const start = performance.now();
    const step = (now) => {
      const elapsed = now - start;
      const progress = Math.min(elapsed / duration, 1);
      // Ease-out cubic
      const eased = 1 - Math.pow(1 - progress, 3);
      el.textContent = Math.round(from + (to - from) * eased);
      if (progress < 1) {
        requestAnimationFrame(step);
      }
    };
    requestAnimationFrame(step);
  }

  /* --- Performance bars animation --- */
  const perfBars = document.querySelector('.perf-bars');
  if (perfBars) {
    const perfObserver = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) {
          perfBars.classList.add('animated');
          perfObserver.unobserve(perfBars);
        }
      },
      { threshold: 0.3 }
    );
    perfObserver.observe(perfBars);
  }

  /* --- Code tab switching --- */
  const tabButtons = document.querySelectorAll('.tab-btn');
  const tabPanels = document.querySelectorAll('.tab-panel');
  tabButtons.forEach((btn) => {
    btn.addEventListener('click', () => {
      const target = btn.dataset.tab;
      tabButtons.forEach((b) => b.classList.toggle('active', b === btn));
      tabPanels.forEach((p) => p.classList.toggle('active', p.id === target));
    });
  });

  /* --- Copy code button --- */
  document.querySelectorAll('.copy-btn').forEach((btn) => {
    btn.addEventListener('click', () => {
      const panel = btn.closest('.tab-panel');
      const code = panel?.querySelector('code')?.textContent || '';
      navigator.clipboard.writeText(code).then(() => {
        btn.textContent = 'Copied!';
        btn.classList.add('copied');
        setTimeout(() => {
          btn.textContent = 'Copy';
          btn.classList.remove('copied');
        }, 2000);
      });
    });
  });

  /* --- Feature category toggle --- */
  document.querySelectorAll('.feature-category-header').forEach((header) => {
    header.addEventListener('click', () => {
      header.parentElement.classList.toggle('open');
    });
  });
});
