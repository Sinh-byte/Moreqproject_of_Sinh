/* global window, document */
(function () {
  function ensureHost() {
    var host = document.getElementById('ui-toast-host');
    if (!host) {
      host = document.createElement('div');
      host.id = 'ui-toast-host';
      host.className = 'fixed top-4 right-4 z-[9999] flex max-w-sm flex-col gap-2';
      document.body.appendChild(host);
    }
    return host;
  }

  function removeToast(el) {
    if (!el) return;
    el.classList.remove('opacity-100', 'translate-y-0');
    el.classList.add('opacity-0', '-translate-y-2');
    setTimeout(function () {
      if (el && el.parentNode) el.parentNode.removeChild(el);
    }, 220);
  }

  function toast(message, type, durationMs) {
    var host = ensureHost();
    var duration = typeof durationMs === 'number' ? durationMs : 3000;
    var toastEl = document.createElement('div');
    var base = 'pointer-events-auto rounded-lg border px-3 py-2 text-sm shadow-lg transition-all duration-200 opacity-0 -translate-y-2';
    var color = 'border-slate-500 bg-slate-800/95 text-slate-100';
    if (type === 'success') color = 'border-emerald-500 bg-emerald-600/95 text-emerald-50';
    if (type === 'error') color = 'border-rose-500 bg-rose-600/95 text-rose-50';
    if (type === 'warning') color = 'border-amber-500 bg-amber-500/95 text-amber-950';
    toastEl.className = base + ' ' + color;
    toastEl.textContent = String(message || '');
    host.appendChild(toastEl);
    requestAnimationFrame(function () {
      toastEl.classList.remove('opacity-0', '-translate-y-2');
      toastEl.classList.add('opacity-100', 'translate-y-0');
    });
    var timer = setTimeout(function () { removeToast(toastEl); }, duration);
    toastEl.addEventListener('click', function () {
      clearTimeout(timer);
      removeToast(toastEl);
    });
  }

  window.UIToast = {
    show: toast,
    success: function (msg, durationMs) { toast(msg, 'success', durationMs); },
    error: function (msg, durationMs) { toast(msg, 'error', durationMs); },
    warn: function (msg, durationMs) { toast(msg, 'warning', durationMs); },
  };
})();
