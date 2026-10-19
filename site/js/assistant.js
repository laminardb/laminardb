/* Docs assistant: sends the conversation to the LaminarDB chatbot worker and shows its reply. */

(function () {
  var WORKER_URL = 'https://laminardb-chatbot.sujitnair.workers.dev';
  var SUGGESTIONS = [
    'How do I define a tumbling window?',
    'How do I connect a Kafka source?',
    'Which sources does cluster mode accept?',
    'What does EMIT ON WINDOW CLOSE do?',
  ];

  function escapeHtml(text) {
    return text.replace(/[&<>"']/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c];
    });
  }

  function format(text) {
    return escapeHtml(text)
      .replace(/```\w*\n?([\s\S]*?)```/g, function (_, code) { return '<pre><code>' + code.trim() + '</code></pre>'; })
      .replace(/`([^`]+)`/g, '<code>$1</code>')
      .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
      .replace(/\n{2,}/g, '</p><p>')
      .replace(/\n/g, '<br>')
      .replace(/^/, '<p>')
      .replace(/$/, '</p>');
  }

  document.addEventListener('DOMContentLoaded', function () {
    var toggle = document.createElement('button');
    toggle.type = 'button';
    toggle.className = 'assistant-toggle';
    toggle.setAttribute('aria-label', 'Ask the LaminarDB docs assistant');
    toggle.setAttribute('aria-expanded', 'false');
    toggle.setAttribute('aria-controls', 'assistant-panel');
    toggle.innerHTML = '<svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.75" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M20 15a2 2 0 0 1-2 2H8l-4 4V5a2 2 0 0 1 2-2h12a2 2 0 0 1 2 2z"/></svg>';

    var panel = document.createElement('section');
    panel.id = 'assistant-panel';
    panel.className = 'assistant-panel';
    panel.hidden = true;
    panel.setAttribute('aria-label', 'Docs assistant');
    panel.innerHTML =
      '<div class="assistant-head"><span>Docs assistant</span><button type="button" data-close>Close</button></div>' +
      '<div class="assistant-log" aria-live="polite">' +
      '<div class="assistant-msg"><span class="who">assistant</span><p>Ask about SQL syntax, connectors or server configuration.</p>' +
      '<div class="assistant-suggestions"></div></div></div>' +
      '<form class="assistant-form"><label class="sr-only" for="assistant-input">Question</label>' +
      '<textarea id="assistant-input" rows="1" placeholder="Ask a question"></textarea>' +
      '<button type="submit">Send</button></form>' +
      '<p class="assistant-note">Answers are generated and can be wrong. Check them against the docs.</p>';

    document.body.appendChild(toggle);
    document.body.appendChild(panel);

    var log = panel.querySelector('.assistant-log');
    var form = panel.querySelector('form');
    var input = panel.querySelector('textarea');
    var send = form.querySelector('button');
    var suggestions = panel.querySelector('.assistant-suggestions');
    var history = [];
    var busy = false;

    function setOpen(open) {
      panel.hidden = !open;
      toggle.setAttribute('aria-expanded', String(open));
      if (open) input.focus();
      else toggle.focus();
    }

    function add(role, html) {
      var msg = document.createElement('div');
      msg.className = 'assistant-msg';
      msg.innerHTML = '<span class="who">' + (role === 'user' ? 'you' : 'assistant') + '</span>' + html;
      log.appendChild(msg);
      log.scrollTop = log.scrollHeight;
      return msg;
    }

    function ask(text) {
      if (!text || busy) return;
      busy = true;
      send.disabled = true;
      input.value = '';
      if (suggestions.parentNode) suggestions.remove();
      add('user', format(text));
      history.push({ role: 'user', content: text });
      var pending = add('assistant', '<p>…</p>');
      fetch(WORKER_URL, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ messages: history }),
      })
        .then(function (res) {
          if (!res.ok) throw new Error('HTTP ' + res.status);
          return res.json();
        })
        .then(function (data) {
          var reply = data.reply || 'No answer was returned.';
          history.push({ role: 'assistant', content: reply });
          var sources = (data.sources || []).slice(0, 5).map(function (path) {
            return '<code title="' + escapeHtml(path) + '">' + escapeHtml(path.split('/').pop()) + '</code>';
          });
          pending.innerHTML = '<span class="who">assistant</span>' + format(reply) +
            (sources.length ? '<p class="assistant-sources">Sources: ' + sources.join(' ') + '</p>' : '');
        })
        .catch(function (err) {
          pending.innerHTML = '<span class="who">assistant</span><p>The assistant is unavailable (' + escapeHtml(err.message) + ').</p>';
        })
        .then(function () {
          busy = false;
          send.disabled = false;
          input.focus();
        });
    }

    SUGGESTIONS.forEach(function (text) {
      var b = document.createElement('button');
      b.type = 'button';
      b.textContent = text;
      b.addEventListener('click', function () { ask(text); });
      suggestions.appendChild(b);
    });

    toggle.addEventListener('click', function () { setOpen(panel.hidden); });
    panel.querySelector('[data-close]').addEventListener('click', function () { setOpen(false); });
    panel.addEventListener('keydown', function (e) {
      if (e.key === 'Escape') setOpen(false);
    });
    form.addEventListener('submit', function (e) {
      e.preventDefault();
      ask(input.value.trim());
    });
    input.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        ask(input.value.trim());
      }
    });
  });
})();
