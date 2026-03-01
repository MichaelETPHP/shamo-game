// SHAMO Admin — Shared JS (Real API edition)
// FIXED API BASE FOR VPS (March 2026)
// Forces correct path: /shamo/api instead of /api
const API_BASE = (function () {
  // Priority 1: Use window variable if set in HTML
  if (typeof window.SHAMO_API_BASE_URL === 'string' && window.SHAMO_API_BASE_URL) {
    return window.SHAMO_API_BASE_URL.replace(/\/$/, '') + '/api';
  }

  // Priority 2: Force correct VPS path
  return '/shamo/api';
})();
const API = API_BASE;   // alias used in login.html + index.html checkDB
const TOKEN_KEY = 'shamo_admin_token';
const USER_KEY = 'shamo_admin_user';

// ─── Global Spinner ───────────────────────────────────────────────────────────
(function initSpinner() {
  const style = document.createElement('style');
  style.textContent = `
    #g-spinner-overlay {
      display: none;
      position: fixed;
      inset: 0;
      z-index: 9999;
      background: rgba(0,0,0,.45);
      backdrop-filter: blur(2px);
      align-items: center;
      justify-content: center;
    }
    #g-spinner-overlay.show { display: flex; }
    .g-spinner-box {
      background: #1A1F2E;
      border: 1px solid rgba(255,255,255,.1);
      border-radius: 16px;
      padding: 1.75rem 2.25rem;
      display: flex;
      align-items: center;
      gap: 1rem;
      box-shadow: 0 20px 60px rgba(0,0,0,.6);
    }
    .g-spinner-ring {
      width: 32px;
      height: 32px;
      border: 3px solid rgba(232,184,75,.2);
      border-top-color: #E8B84B;
      border-radius: 50%;
      animation: g-spin .7s linear infinite;
      flex-shrink: 0;
    }
    .g-spinner-text {
      font-size: .9rem;
      font-weight: 600;
      color: #F1F5F9;
      letter-spacing: .02em;
    }
    @keyframes g-spin { to { transform: rotate(360deg); } }
  `;
  document.head.appendChild(style);

  const overlay = document.createElement('div');
  overlay.id = 'g-spinner-overlay';
  overlay.innerHTML = `
    <div class="g-spinner-box">
      <div class="g-spinner-ring"></div>
      <span class="g-spinner-text" id="g-spinner-msg">Processing…</span>
    </div>`;
  document.body.appendChild(overlay);
})();

let _spinnerCount = 0;
function showSpinner(msg = 'Processing…') {
  _spinnerCount++;
  document.getElementById('g-spinner-msg').textContent = msg;
  document.getElementById('g-spinner-overlay').classList.add('show');
}
function hideSpinner() {
  _spinnerCount = Math.max(0, _spinnerCount - 1);
  if (_spinnerCount === 0)
    document.getElementById('g-spinner-overlay').classList.remove('show');
}

// ─── Auth ─────────────────────────────────────────────────────────────────────
function getToken() { return localStorage.getItem(TOKEN_KEY) || ''; }
function getAdminUser() { try { return JSON.parse(localStorage.getItem(USER_KEY) || '{}'); } catch { return {}; } }

function requireAuth() {
  if (!getToken()) { window.location.href = 'login.html'; }
}

function logout() {
  apiFetch('/auth/logout', { method: 'POST' }).catch(() => { });
  localStorage.removeItem(TOKEN_KEY);
  localStorage.removeItem(USER_KEY);
  window.location.href = 'login.html';
}

// ─── API fetch helper ────────────────────────────────────────────────────────
async function apiFetch(path, opts = {}) {
  const url = API_BASE + path;
  const res = await fetch(url, {
    headers: {
      'Content-Type': 'application/json',
      'X-Admin-Token': getToken(),
      ...(opts.headers || {}),
    },
    ...opts,
    body: opts.body ? (typeof opts.body === 'string' ? opts.body : JSON.stringify(opts.body)) : undefined,
  });

  if (res.status === 401) {
    localStorage.removeItem(TOKEN_KEY);
    window.location.href = 'login.html';
    return;
  }

  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const msg = data?.detail || data?.error || `API error ${res.status}`;
    throw new Error(msg);
  }
  return data;
}

// Shorthand helpers — GET is silent; POST/PUT/DELETE auto-show spinner
const apiGet = (path) => apiFetch(path);

async function apiPost(path, body) {
  showSpinner('Saving…');
  try { return await apiFetch(path, { method: 'POST', body }); }
  finally { hideSpinner(); }
}
async function apiPut(path, body) {
  showSpinner('Updating…');
  try { return await apiFetch(path, { method: 'PUT', body }); }
  finally { hideSpinner(); }
}
async function apiDelete(path) {
  showSpinner('Deleting…');
  try { return await apiFetch(path, { method: 'DELETE' }); }
  finally { hideSpinner(); }
}

// ─── Toast ────────────────────────────────────────────────────────────────────
function showToast(message, type = 'success', duration = 3500) {
  let container = document.getElementById('toast-container');
  if (!container) {
    container = document.createElement('div');
    container.id = 'toast-container';
    document.body.appendChild(container);
  }
  const icons = { success: '✅', error: '❌', info: 'ℹ️', warning: '⚠️' };
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerHTML = `<span>${icons[type] || 'ℹ️'}</span><span>${message}</span>`;
  container.appendChild(toast);
  setTimeout(() => { toast.style.animation = 'slideIn .3s ease reverse'; setTimeout(() => toast.remove(), 300); }, duration);
}

// ─── Modal helpers ────────────────────────────────────────────────────────────
function openModal(id) { const el = document.getElementById(id); if (el) el.classList.add('open'); }
function closeModal(id) { const el = document.getElementById(id); if (el) el.classList.remove('open'); }
document.addEventListener('click', e => { if (e.target.classList.contains('modal-overlay')) e.target.classList.remove('open'); });

// ─── Loading state ────────────────────────────────────────────────────────────
function setLoading(id, loading) {
  const el = document.getElementById(id);
  if (!el) return;
  if (loading) el.innerHTML = `<tr><td colspan="20" style="text-align:center;padding:3rem;color:var(--muted)">
    <div style="font-size:1.5rem;margin-bottom:.5rem">⏳</div>Loading from database…</td></tr>`;
}

// ─── Ethiopia timezone (GMT+3, Africa/Addis_Ababa — no DST) ──────────────────
const ET_TZ = 'Africa/Addis_Ababa';
const ET_LOCALE = 'en-US';

/** Format a UTC date/timestamp as a readable date in Ethiopian time */
function fmtDate(d) {
  if (!d) return '—';
  try {
    return new Date(d).toLocaleDateString(ET_LOCALE, {
      timeZone: ET_TZ, month: 'short', day: 'numeric', year: 'numeric'
    });
  } catch { return '—'; }
}

/** Format a UTC date/timestamp as date + time in Ethiopian time (GMT+3) */
function fmtTime(d) {
  if (!d) return '—';
  try {
    return new Date(d).toLocaleString(ET_LOCALE, {
      timeZone: ET_TZ, month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit', hour12: true
    }) + ' (ET)';
  } catch { return '—'; }
}

/** Format time-only in Ethiopian timezone */
function fmtTimeOnly(d) {
  if (!d) return '—';
  try {
    return new Date(d).toLocaleTimeString(ET_LOCALE, {
      timeZone: ET_TZ, hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true
    }) + ' (ET)';
  } catch { return '—'; }
}

/** Full Ethiopian date+time — "Friday, Feb 27 2026, 10:30 PM (GMT+3)" */
function fmtFull(d) {
  if (!d) return '—';
  try {
    return new Date(d).toLocaleString(ET_LOCALE, {
      timeZone: ET_TZ, weekday: 'short', month: 'short', day: 'numeric',
      year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: true
    }) + ' GMT+3';
  } catch { return '—'; }
}

/** Convert a UTC ISO string to a datetime-local value in Ethiopia time (for form inputs) */
function utcToEtInput(iso) {
  if (!iso) return '';
  try {
    const d = new Date(iso);
    // Get date parts in ET timezone
    const parts = new Intl.DateTimeFormat('en-CA', {
      timeZone: ET_TZ,
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', hour12: false
    }).formatToParts(d);
    const get = t => parts.find(p => p.type === t)?.value || '00';
    const h = get('hour') === '24' ? '00' : get('hour');
    return `${get('year')}-${get('month')}-${get('day')}T${h}:${get('minute')}`;
  } catch { return iso.substring(0, 16); }
}

/** Convert a datetime-local value (treated as Ethiopia GMT+3) to UTC ISO string for the API */
function etInputToUtc(localStr) {
  if (!localStr) return null;
  try {
    // Parse as ET time: subtract 3 hours to get UTC
    const d = new Date(localStr + ':00');          // local naive parse
    const utc = new Date(d.getTime() - 3 * 60 * 60 * 1000);
    return utc.toISOString();
  } catch { return localStr; }
}

/** Get "now" as a datetime-local string in Ethiopian time (for pre-filling forms) */
function nowEtInput(offsetHours = 0) {
  return utcToEtInput(new Date(Date.now() + offsetHours * 3600000).toISOString());
}

/** Ethiopian "now" as a formatted string */
function nowEtFmt() { return fmtFull(new Date()); }

function timeAgo(d) {
  if (!d) return '—';
  const s = Math.floor((Date.now() - new Date(d)) / 1000);
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

// ─── Format helpers ───────────────────────────────────────────────────────────
function fmt$(n) { return '$' + Number(n || 0).toFixed(2); }
function fmtEtb(n) { return Number(n || 0).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) + ' ETB'; }
function fmtN(n) { return Number(n || 0).toLocaleString(); }

// ─── Badge ────────────────────────────────────────────────────────────────────
const STATUS_MAP = {
  active: 'badge-green', approved: 'badge-green', completed: 'badge-green', won: 'badge-green', correct: 'badge-green',
  pending: 'badge-gold', scheduled: 'badge-gold', draft: 'badge-muted', processing: 'badge-blue', ended: 'badge-blue',
  suspended: 'badge-orange', rejected: 'badge-red', failed: 'badge-red', cancelled: 'badge-red', banned: 'badge-red',
  easy: 'badge-green', medium: 'badge-gold', hard: 'badge-red',
  player: 'badge-blue', company: 'badge-purple', admin: 'badge-gold',
};
function badge(status) {
  return `<span class="badge ${STATUS_MAP[status] || 'badge-muted'}">${status}</span>`;
}

// ─── Pagination renderer ─────────────────────────────────────────────────────
function renderPagination(containerId, page, total, perPage, onPage) {
  const totalPages = Math.ceil(total / perPage);
  const el = document.getElementById(containerId);
  if (!el) return;
  let html = '';
  const start = Math.max(1, page - 2), end = Math.min(totalPages, page + 2);
  if (start > 1) html += `<button class="pag-btn" onclick="(${onPage})(1)">1</button>${start > 2 ? '<span style="color:var(--muted);font-size:.8rem">…</span>' : ''}`;
  for (let i = start; i <= end; i++) html += `<button class="pag-btn ${i === page ? 'active' : ''}" onclick="(${onPage})(${i})">${i}</button>`;
  if (end < totalPages) html += `${end < totalPages - 1 ? '<span style="color:var(--muted);font-size:.8rem">…</span>' : ''}<button class="pag-btn" onclick="(${onPage})(${totalPages})">${totalPages}</button>`;
  html += `<span class="pag-info">${fmtN(total)} total</span>`;
  el.innerHTML = html;
}

// ─── Stats cache for sidebar badges ─────────────────────────────────────────
let _statsCache = null;
async function getStats() {
  if (_statsCache) return _statsCache;
  try { _statsCache = await apiGet('/stats'); } catch { _statsCache = {}; }
  return _statsCache;
}

// ─── Sidebar ──────────────────────────────────────────────────────────────────
async function buildSidebar(activePage) {
  const user = getAdminUser();
  const initials = (user.username || 'A').substring(0, 2).toUpperCase();

  const nav = [
    { icon: '📊', label: 'Dashboard', href: 'index.html', page: 'dashboard' },
    { icon: '🎮', label: 'Games', href: 'games.html', page: 'games' },
    { icon: '❓', label: 'Questions', href: 'questions.html', page: 'questions', badgeKey: 'pending_questions' },
    { icon: '👥', label: 'Users', href: 'users.html', page: 'users' },
    { icon: '💸', label: 'Withdrawals', href: 'withdrawals.html', page: 'withdrawals', badgeKey: 'pending_withdrawals' },
    { icon: '🏢', label: 'Companies', href: 'companies.html', page: 'companies', badgeKey: 'pending_companies' },
    { icon: '💳', label: 'Deposits', href: 'deposits.html', page: 'deposits', badgeKey: 'pending_deposits' },
    { icon: '📲', label: 'QR Manager', href: 'qr-manager.html', page: 'qr' },
    { icon: '📈', label: 'Analytics', href: 'analytics.html', page: 'analytics' },
    { icon: '⚙️', label: 'Settings', href: 'settings.html', page: 'settings' },
  ];

  const el = document.getElementById('sidebar');
  if (!el) return;

  el.innerHTML = `
    <div class="sidebar-logo">
      <div class="icon">🎡</div>
      <div class="brand"><h2>SHAMO</h2><p>Admin Portal</p></div>
    </div>
    <div class="sidebar-section">
      <div class="sidebar-section-label">Navigation</div>
      ${nav.map(n => `
        <a class="nav-item ${n.page === activePage ? 'active' : ''}" href="${n.href}"
           onclick="_navSpinner(event,'${n.href}','${n.page}','${activePage}')">
          <span class="icon">${n.icon}</span>${n.label}
          ${n.badgeKey ? `<span class="nav-badge" id="badge-${n.badgeKey}" style="display:none">0</span>` : ''}
        </a>`).join('')}
    </div>
    <div class="sidebar-footer">
      <div class="user-chip" onclick="logout()">
        <div class="user-avatar">${initials}</div>
        <div class="info"><p>${user.username || 'Admin'}</p><span>Sign out</span></div>
      </div>
      <div class="admin-dev-footer">Dev By MICHAEL</div>
    </div>`;

  // Load badges
  try {
    const stats = await getStats();
    nav.filter(n => n.badgeKey).forEach(n => {
      const v = stats[n.badgeKey] || 0;
      const el2 = document.getElementById(`badge-${n.badgeKey}`);
      if (el2 && v > 0) { el2.textContent = v; el2.style.display = ''; }
    });
  } catch { }

  // Inject main-area footer (Dev By MICHAEL) if not present
  const main = document.querySelector('.main-area');
  if (main && !document.getElementById('admin-page-footer')) {
    const footer = document.createElement('div');
    footer.id = 'admin-page-footer';
    footer.className = 'admin-page-footer';
    footer.textContent = 'Dev By MICHAEL';
    main.appendChild(footer);
  }
}

// Show a full-page spinner when navigating between pages
function _navSpinner(e, href, targetPage, currentPage) {
  if (targetPage === currentPage) return; // same page — no spinner
  e.preventDefault();
  // Show page-transition overlay
  const ov = document.createElement('div');
  ov.id = 'nav-spinner-overlay';
  ov.style.cssText = `
    position:fixed;inset:0;z-index:10000;
    background:rgba(11,13,18,.92);display:flex;flex-direction:column;
    align-items:center;justify-content:center;gap:1rem;`;
  ov.innerHTML = `
    <div style="width:44px;height:44px;border:3px solid rgba(232,184,75,.2);
      border-top-color:#E8B84B;border-radius:50%;animation:g-spin .7s linear infinite"></div>
    <div style="font-size:.85rem;font-weight:600;color:#7A839A;letter-spacing:.05em">Loading…</div>`;
  document.body.appendChild(ov);
  setTimeout(() => { window.location.href = href; }, 30); // tiny delay so overlay renders
}

// ─── Ethiopian clock — auto-inject into any .topbar-actions ─────────────────
(function injectEtClock() {
  function mount() {
    const bar = document.querySelector('.topbar-actions');
    if (!bar || document.getElementById('et-clock')) return;
    const el = document.createElement('span');
    el.id = 'et-clock';
    el.title = 'Ethiopian time (GMT+3, Africa/Addis_Ababa)';
    el.style.cssText = 'font-size:.72rem;color:#7A839A;letter-spacing:.01em;white-space:nowrap;display:flex;align-items:center;gap:.3rem';
    el.innerHTML = '<span style="font-size:.8rem">🇪🇹</span><span id="et-clock-txt">—</span>';
    bar.prepend(el);
    function tick() {
      const t = document.getElementById('et-clock-txt');
      if (t) t.textContent = fmtTimeOnly(new Date());
    }
    tick();
    setInterval(tick, 1000);
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount);
  else mount();
})();

// Run on every page load
requireAuth();
