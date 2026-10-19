/* LaminarDB site: colour theme toggle and copy buttons for code blocks. */

(function () {
  var MODES = ['system', 'light', 'dark'];
  var media = window.matchMedia('(prefers-color-scheme: dark)');

  function storedMode() {
    try {
      return localStorage.getItem('theme') || 'system';
    } catch (e) {
      return 'system';
    }
  }

  function applyMode(mode) {
    var dark = mode === 'dark' || (mode === 'system' && media.matches);
    document.documentElement.setAttribute('data-theme', dark ? 'dark' : 'light');
    document.querySelectorAll('.theme-toggle').forEach(function (button) {
      button.setAttribute('data-mode', mode);
      button.setAttribute('aria-label', 'Colour theme: ' + mode + '. Change theme');
      button.title = 'Colour theme: ' + mode;
    });
  }

  document.addEventListener('DOMContentLoaded', function () {
    applyMode(storedMode());

    document.querySelectorAll('.theme-toggle').forEach(function (button) {
      button.addEventListener('click', function () {
        var next = MODES[(MODES.indexOf(storedMode()) + 1) % MODES.length];
        try {
          localStorage.setItem('theme', next);
        } catch (e) {}
        applyMode(next);
      });
    });

    document.querySelectorAll('pre:not([tabindex])').forEach(function (pre) {
      if (pre.scrollWidth > pre.clientWidth) pre.tabIndex = 0;
    });

    document.querySelectorAll('.code').forEach(function (block) {
      var pre = block.querySelector('pre');
      if (!pre || !navigator.clipboard) return;
      var button = document.createElement('button');
      button.type = 'button';
      button.className = 'copy-button';
      button.textContent = 'Copy';
      var label = block.previousElementSibling;
      if (label && label.classList.contains('code-label')) {
        button.setAttribute('aria-label', 'Copy ' + label.textContent.trim());
      }
      button.addEventListener('click', function () {
        navigator.clipboard.writeText(pre.innerText).then(
          function () { button.textContent = 'Copied'; },
          function () { button.textContent = 'Copy failed'; }
        );
        setTimeout(function () { button.textContent = 'Copy'; }, 2000);
      });
      block.appendChild(button);
    });
  });

  media.addEventListener('change', function () {
    if (storedMode() === 'system') applyMode('system');
  });
})();
