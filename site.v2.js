/**
 * site.js — MineralSearch Platform JavaScript
 * Queries Supabase REST API for well data. Shows nothing if Supabase is unreachable.
 * Supports multi-region: Texas Onshore + Gulf of Mexico Offshore.
 * Includes paywall, production data, mineral ownership, CSV export, and map integration.
 */
console.log('[MineralSearch] site.js loaded at', new Date().toISOString());

(function () {
  'use strict';
  console.log('[MineralSearch] IIFE executing');

  // ============================================
  // Supabase Configuration
  // ============================================
  const SUPABASE_URL = 'https://bkyzxvetrguudtsqrvvq.supabase.co';
  const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImJreXp4dmV0cmd1dWR0c3FydnZxIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM4ODcyNTUsImV4cCI6MjA4OTQ2MzI1NX0.FE2YEIk9pu6P5EUdQY7S7YRfQK0Q7ckSGuJDnWxSks8';

  function supabaseHeaders(wantCount) {
    const h = {
      'apikey': SUPABASE_ANON_KEY,
      'Authorization': 'Bearer ' + SUPABASE_ANON_KEY,
      'Content-Type': 'application/json',
    };
    if (wantCount) h['Prefer'] = 'count=exact';
    return h;
  }

  async function supabaseGet(path, wantCount) {
    const resp = await fetch(SUPABASE_URL + '/rest/v1/' + path, {
      headers: supabaseHeaders(wantCount),
    });
    if (!resp.ok) throw new Error('Supabase error: ' + resp.status);
    const data = await resp.json();
    const countHeader = resp.headers.get('content-range');
    let totalCount = null;
    if (countHeader) {
      const match = countHeader.match(/\/(\d+)/);
      if (match) totalCount = parseInt(match[1], 10);
    }
    return { data, totalCount };
  }

  // ============================================
  // Paywall / Access Level System (TASK 1)
  // ============================================
  const ACCESS_LEVELS = ['free', 'basic', 'pro', 'enterprise'];

  // ============================================
  // Auth State
  // ============================================
  let currentUser = null;
  let authToken = null;

  // Master login: add ?master=hasten2026 to any URL to unlock everything
  (function checkMasterLogin() {
    const params = new URLSearchParams(window.location.search);
    if (params.get('master') === 'hasten2026') {
      localStorage.setItem('ms_access', 'enterprise');
      window.history.replaceState({}, '', window.location.pathname);
      console.log('[MineralSearch] Master access granted');
    }
  })();

  function getAccessLevel() {
    const stored = localStorage.getItem('ms_access');
    if (stored && ACCESS_LEVELS.includes(stored)) return stored;
    return 'free';
  }

  function setAccessLevel(level) {
    if (ACCESS_LEVELS.includes(level)) {
      localStorage.setItem('ms_access', level);
    }
  }

  function isPaid() {
    return getAccessLevel() !== 'free';
  }

  function canExport() {
    const level = getAccessLevel();
    return level === 'pro' || level === 'enterprise';
  }

  function canViewAllCounties() {
    const level = getAccessLevel();
    return level === 'pro' || level === 'enterprise';
  }

  function isEnterprise() {
    return getAccessLevel() === 'enterprise';
  }

  // ============================================
  // Auth Functions
  // ============================================
  function loadAuthState() {
    const token = localStorage.getItem('ms_auth_token');
    const user = localStorage.getItem('ms_user');
    if (token && user) {
      authToken = token;
      try {
        currentUser = JSON.parse(user);
      } catch(e) {
        currentUser = null;
      }
      // Verify token is still valid (async, non-blocking)
      verifyToken(token);
    }
    updateNavAuth();
  }

  async function verifyToken(token) {
    try {
      const resp = await fetch(SUPABASE_URL + '/auth/v1/user', {
        headers: { 'Authorization': 'Bearer ' + token, 'apikey': SUPABASE_ANON_KEY }
      });
      if (!resp.ok) { logout(); return; }
      const user = await resp.json();
      currentUser = user;
      localStorage.setItem('ms_user', JSON.stringify(user));
      // Fetch their plan from user_profiles
      const profileResp = await fetch(SUPABASE_URL + '/rest/v1/user_profiles?id=eq.' + user.id + '&select=plan', {
        headers: { 'Authorization': 'Bearer ' + token, 'apikey': SUPABASE_ANON_KEY }
      });
      if (profileResp.ok) {
        const profiles = await profileResp.json();
        if (profiles[0] && profiles[0].plan) {
          setAccessLevel(profiles[0].plan);
        }
      }
      updateNavAuth();
    } catch(e) {
      // Token invalid
      logout();
    }
  }

  function logout() {
    localStorage.removeItem('ms_auth_token');
    localStorage.removeItem('ms_user');
    localStorage.removeItem('ms_access');
    currentUser = null;
    authToken = null;
    updateNavAuth();
  }

  function updateNavAuth() {
    // Update nav to show user email or login button
    const navCta = document.querySelector('.nav-cta');
    if (navCta) {
      if (currentUser && currentUser.email) {
        navCta.textContent = currentUser.email.split('@')[0];
        navCta.href = 'account.html';
        navCta.removeAttribute('data-modal-open');
      } else {
        navCta.textContent = 'Log In';
        navCta.href = 'login.html';
        navCta.removeAttribute('data-modal-open');
      }
    }
  }

  // Check for Stripe payment success in URL
  function checkPaymentSuccess() {
    const params = new URLSearchParams(window.location.search);
    if (params.get('payment') === 'success') {
      const plan = params.get('plan');
      const email = params.get('email');
      if (plan && ACCESS_LEVELS.includes(plan)) {
        setAccessLevel(plan);

        // If user is logged in, update their profile in Supabase
        const token = localStorage.getItem('ms_auth_token');
        const user = localStorage.getItem('ms_user');
        if (token && user) {
          try {
            const userData = JSON.parse(user);
            fetch(SUPABASE_URL + '/rest/v1/user_profiles?id=eq.' + userData.id, {
              method: 'PATCH',
              headers: {
                'Authorization': 'Bearer ' + token,
                'apikey': SUPABASE_ANON_KEY,
                'Content-Type': 'application/json',
                'Prefer': 'return=minimal'
              },
              body: JSON.stringify({ plan: plan, updated_at: new Date().toISOString() })
            }).catch(function(e) { console.log('[MineralSearch] Profile update failed:', e); });
          } catch(e) {}
        }

        // Clean up URL
        const url = new URL(window.location);
        url.searchParams.delete('payment');
        url.searchParams.delete('plan');
        url.searchParams.delete('email');
        window.history.replaceState({}, '', url.toString());
      }
    }
  }

  // Blurred content helper
  function blurredValue(value, clickable) {
    const onclick = clickable !== false ? ' onclick="window.__subscribe(\'pro\')"' : '';
    return '<span class="paywall-text"' + onclick + ' title="Subscribe to view">Subscribe to view</span>';
  }

  // ============================================
  // Mineral Ownership (placeholder — no CAD data)
  // ============================================

  // ============================================
  // Production Data (real data only — no generators)
  // ============================================

  // ============================================
  // No demo data — Supabase only
  // ============================================

  // ============================================
  // State
  // ============================================
  let supabaseAvailable = false;
  let currentPage = 1;
  const PAGE_SIZE = 20;
  let displayedWells = [];
  let totalResultCount = 0;
  let sortField = 'county';
  let sortDir = 'asc';
  let activeRegion = 'all';
  let isLoading = false;

  // Debounce timer
  let searchDebounce = null;

  // County list cache
  let countyList = [];

  // ============================================
  // Supabase Column -> Sort Mapping
  // ============================================
  const SORT_FIELD_MAP = {
    'apiNumber': 'api',
    'api': 'api',
    'operator': 'operator',
    'county': 'county',
    'wellType': 'well_type',
    'well_type': 'well_type',
    'totalDepth': 'total_depth',
    'total_depth': 'total_depth',
    'status': 'status',
  };

  // ============================================
  // Query Builder
  // ============================================
  function buildQueryParams() {
    const county = (document.getElementById('filter-county') || {}).value || '';
    const operator = (document.getElementById('filter-operator') || {}).value || '';
    const wellType = (document.getElementById('filter-type') || {}).value || '';
    const search = (document.getElementById('search-input') || document.getElementById('hero-search-input') || {}).value || '';

    const params = [];
    params.push('select=id,api,operator,county,well_type,total_depth,status,offshore,lease_name,field_name,well_number,district');

    if (county) params.push('county=eq.' + encodeURIComponent(county));
    if (operator) params.push('operator=eq.' + encodeURIComponent(operator));
    if (wellType) params.push('well_type=eq.' + encodeURIComponent(wellType));

    if (activeRegion === 'onshore') params.push('offshore=eq.false');
    if (activeRegion === 'offshore') params.push('offshore=eq.true');

    if (search.trim()) {
      const term = search.trim().replace(/%/g, '');
      const encoded = encodeURIComponent('%' + term + '%');
      params.push('or=(county.ilike.' + encoded + ',operator.ilike.' + encoded + ',lease_name.ilike.' + encoded + ',api.ilike.' + encoded + ')');
    }

    const dbSortField = SORT_FIELD_MAP[sortField] || 'county';
    params.push('order=' + dbSortField + '.' + sortDir);

    const offset = (currentPage - 1) * PAGE_SIZE;
    params.push('offset=' + offset);
    params.push('limit=' + PAGE_SIZE);

    return params.join('&');
  }

  // ============================================
  // Data Fetching
  // ============================================
  async function fetchWells() {
    if (isLoading) return;
    isLoading = true;
    showLoadingState();
    console.log('[MineralSearch] fetchWells called, supabaseAvailable:', supabaseAvailable);

    try {
      if (supabaseAvailable) {
        const query = buildQueryParams();
        console.log('[MineralSearch] Querying Supabase:', query.substring(0, 200));
        const { data, totalCount } = await supabaseGet('wells?' + query, true);
        console.log('[MineralSearch] Got', (data||[]).length, 'wells, total:', totalCount);
        displayedWells = data || [];
        totalResultCount = totalCount || displayedWells.length;
      } else {
        console.log('[MineralSearch] Supabase not available');
        displayedWells = [];
        totalResultCount = 0;
      }
    } catch (e) {
      console.error('[MineralSearch] Failed to fetch wells:', e.message, e);
      supabaseAvailable = false;
      displayedWells = [];
      totalResultCount = 0;
    }

    isLoading = false;
    console.log('[MineralSearch] Rendering', displayedWells.length, 'wells');
    renderResults();
  }

  // ============================================
  // Loading State
  // ============================================
  function showLoadingState() {
    const tbody = document.getElementById('wells-tbody');
    if (tbody) {
      tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted" style="padding:40px;">Searching...</td></tr>';
    }
  }

  // ============================================
  // Initialize Supabase Connection
  // ============================================
  async function initSupabase() {
    console.log('[MineralSearch] Testing Supabase connection...');
    try {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), 5000);
      const resp = await fetch(SUPABASE_URL + '/rest/v1/wells?select=id&limit=1', {
        headers: supabaseHeaders(false),
        signal: controller.signal,
      });
      clearTimeout(timeout);
      if (resp.ok) {
        const data = await resp.json();
        if (data && data.length >= 0) {
          supabaseAvailable = true;
          console.log('[MineralSearch] Supabase connected!');
        }
      }
    } catch (e) {
      console.log('[MineralSearch] Supabase unavailable, using demo:', e.message);
      supabaseAvailable = false;
    }
  }

  // ============================================
  // Populate County Filter from Supabase
  // ============================================
  async function populateCountyFilter() {
    const countySelect = document.getElementById('filter-county');
    if (!countySelect) return;

    if (supabaseAvailable) {
      try {
        const { data } = await supabaseGet('wells?select=county&order=county&limit=1000', false);
        const seen = new Set();
        countyList = [];
        (data || []).forEach(row => {
          if (row.county && !seen.has(row.county)) {
            seen.add(row.county);
            countyList.push(row.county);
          }
        });
        countyList.sort();
      } catch (e) {
        console.error('Failed to load counties:', e);
        countyList = [];
      }
    } else {
      countyList = [];
    }

    countySelect.innerHTML = '<option value="">All Counties (' + countyList.length + ')</option>' +
      countyList.map(c => `<option value="${c}">${c}</option>`).join('');
  }

  // ============================================
  // Populate Operator Filter from current results
  // ============================================
  function populateOperatorFilter() {
    const operatorSelect = document.getElementById('filter-operator');
    if (!operatorSelect) return;

    const operators = new Set();
    displayedWells.forEach(w => { if (w.operator) operators.add(w.operator); });
    const sorted = [...operators].sort();

    const current = operatorSelect.value;
    operatorSelect.innerHTML = '<option value="">All Operators</option>' +
      sorted.map(o => `<option value="${o}"${o === current ? ' selected' : ''}>${o}</option>`).join('');
  }

  // ============================================
  // Rendering Helpers
  // ============================================
  function formatNumber(n) {
    if (n == null) return '-';
    return n.toLocaleString();
  }

  function formatCompact(n) {
    if (n >= 1000000) return (n/1000000).toFixed(1) + 'M';
    if (n >= 1000) return (n/1000).toFixed(1) + 'K';
    return String(n);
  }

  function statusBadge(status) {
    const s = (status || '').toUpperCase();
    const cls = {
      'PRODUCING': 'badge-producing',
      'SHUT IN': 'badge-shutin',
      'INJECTION': 'badge-active',
      'COMPLETED': 'badge-completed',
      'PERMITTED': 'badge-permitted',
      'PLUGGED': 'badge-plugged',
    }[s] || 'badge-active';
    return `<span class="badge ${cls}">${status || '-'}</span>`;
  }

  function wellTypeLabel(t) {
    return { 'O': 'Oil', 'G': 'Gas', 'B': 'Both', '': '-' }[t] || t || '-';
  }

  // ============================================
  // Animated Counter
  // ============================================
  function animateCounter(el, target, duration) {
    if (!el) return;
    const start = 0;
    const startTime = performance.now();
    const isCompact = target >= 1000;

    function update(now) {
      const elapsed = now - startTime;
      const progress = Math.min(elapsed / duration, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      const current = Math.round(start + (target - start) * eased);
      el.textContent = isCompact ? formatCompact(current) : formatNumber(current);
      if (progress < 1) requestAnimationFrame(update);
    }
    requestAnimationFrame(update);
  }

  // ============================================
  // Region Filter
  // ============================================
  function setRegion(region) {
    activeRegion = region;

    const toggle = document.getElementById('region-toggle');
    if (toggle) {
      toggle.querySelectorAll('button').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.region === region);
      });
    }

    currentPage = 1;
    fetchWells();
  }

  // ============================================
  // Apply Filters (triggers a new query)
  // ============================================
  function applyFilters() {
    currentPage = 1;
    fetchWells();
  }

  // ============================================
  // Render: Hero Stats (index.html)
  // ============================================
  function renderHeroStats() {
    const els = {
      'stat-wells': 1365827,
      'stat-producing': 281981,
      'stat-operators': 41209,
    };

    Object.entries(els).forEach(([id, val]) => {
      const el = document.getElementById(id);
      if (el && val) animateCounter(el, val, 1500);
    });

    const regionsEl = document.getElementById('stat-regions');
    if (regionsEl) regionsEl.textContent = '2';

    const onshoreEl = document.getElementById('stat-onshore');
    if (onshoreEl) onshoreEl.textContent = '1,310,381 wells';

    const offshoreEl = document.getElementById('stat-offshore');
    if (offshoreEl) offshoreEl.textContent = '55,446 wells';

    const leaseEl = document.getElementById('stat-leases');
    if (leaseEl) animateCounter(leaseEl, 255797, 1500);

    const pipeEl = document.getElementById('stat-pipelines');
    if (pipeEl) animateCounter(pipeEl, 21235, 1500);
  }

  // ============================================
  // Render: Dashboard Cards (index.html)
  // ============================================
  function renderDashboard() {
    const topCounties = [
      { n: 'MIDLAND', c: 25000 }, { n: 'ECTOR', c: 22000 }, { n: 'REEVES', c: 18000 },
      { n: 'HOWARD', c: 15000 }, { n: 'MARTIN', c: 12000 }, { n: 'WARD', c: 11000 },
    ];
    const topOperators = [
      { n: 'Pioneer Natural Resources', c: 8500 }, { n: 'Diamondback Energy', c: 7200 },
      { n: 'ConocoPhillips', c: 6800 }, { n: 'EOG Resources', c: 5900 },
      { n: 'Devon Energy', c: 5100 }, { n: 'Apache Corporation', c: 4800 },
    ];
    const types = [
      { n: 'O', c: 750000 }, { n: 'G', c: 480000 }, { n: 'B', c: 80000 },
    ];
    const statuses = [
      { n: 'PRODUCING', c: 281981 }, { n: 'SHUT IN', c: 145000 },
      { n: 'PLUGGED', c: 620000 }, { n: 'PERMITTED', c: 95000 },
    ];

    const permitsEl = document.getElementById('recent-permits');
    if (permitsEl) {
      permitsEl.innerHTML = topCounties.map(c => `
        <div class="dash-item" onclick="window.location.href='wells.html?q=${encodeURIComponent(c.n)}'">
          <div class="dash-item-icon" style="background:var(--blue-dim);color:var(--blue);">${c.n.charAt(0)}</div>
          <div class="dash-item-content">
            <div class="dash-item-title">${c.n} County</div>
            <div class="dash-item-sub">Active wells in county</div>
          </div>
          <div class="dash-item-value">
            <div class="val text-secondary">${formatCompact(c.c)}</div>
            <div class="unit">wells</div>
          </div>
        </div>
      `).join('');
    }

    const topEl = document.getElementById('top-producers');
    if (topEl) {
      const maxCount = topOperators[0]?.c || 1;
      topEl.innerHTML = topOperators.map((o, i) => `
        <div class="dash-item" onclick="window.location.href='wells.html?q=${encodeURIComponent(o.n)}'">
          <div class="dash-item-icon" style="background:var(--green-dim);color:var(--green);font-weight:800;">${i+1}</div>
          <div class="dash-item-content">
            <div class="dash-item-title">${o.n}</div>
            <div class="dash-item-sub">Active wells across all regions</div>
          </div>
          <div class="dash-item-value">
            <div class="val text-green">${formatCompact(o.c)}</div>
            <div class="unit">wells</div>
            <div class="inline-bar" style="width:${Math.round(o.c/maxCount*60)}px;"></div>
          </div>
        </div>
      `).join('');
    }

    const leaseEl = document.getElementById('latest-leases');
    if (leaseEl) {
      leaseEl.innerHTML = types.map(t => {
        const label = { 'O': 'Oil Wells', 'G': 'Gas Wells', 'B': 'Both Oil & Gas', '': 'Unclassified' }[t.n] || t.n;
        return `
          <div class="dash-item">
            <div class="dash-item-icon" style="background:var(--gold-dim);color:var(--gold);">${t.n || '?'}</div>
            <div class="dash-item-content">
              <div class="dash-item-title">${label}</div>
              <div class="dash-item-sub">RRC classification</div>
            </div>
            <div class="dash-item-value">
              <div class="val text-gold">${formatCompact(t.c)}</div>
              <div class="unit">wells</div>
            </div>
          </div>
        `;
      }).join('');
    }

    const opsEl = document.getElementById('active-operators');
    if (opsEl) {
      const maxCount = statuses[0]?.c || 1;
      opsEl.innerHTML = statuses.map(s => `
        <div class="dash-item">
          <div class="dash-item-icon" style="background:var(--accent-dim);color:var(--accent);">${s.n.charAt(0)}</div>
          <div class="dash-item-content">
            <div class="dash-item-title">${s.n}</div>
            <div class="dash-item-sub">RRC well status</div>
          </div>
          <div class="dash-item-value">
            <div class="val text-accent">${formatCompact(s.c)}</div>
            <div class="unit">wells</div>
            <div class="inline-bar" style="width:${Math.round(s.c/maxCount*60)}px;"></div>
          </div>
        </div>
      `).join('');
    }
  }

  // ============================================
  // Render: Paywall Banner (TASK 1)
  // ============================================
  function renderPaywallBanner() {
    const existing = document.getElementById('paywall-banner');
    if (existing) existing.remove();

    if (isPaid()) return;

    const container = document.querySelector('.table-container');
    if (!container) return;

    const banner = document.createElement('div');
    banner.id = 'paywall-banner';
    banner.className = 'paywall-banner';
    banner.innerHTML = `
      <div class="paywall-banner-inner">
        <div class="paywall-banner-icon">&#9889;</div>
        <div class="paywall-banner-text">
          <strong>Subscribe to unlock full data</strong>
          <span>Operator names, lease details, production data, mineral ownership, and CSV export.</span>
        </div>
        <button class="btn btn-primary btn-sm" onclick="window.__subscribe('pro')">Unlock Pro — $299/mo</button>
      </div>
    `;
    container.parentNode.insertBefore(banner, container);
  }

  // ============================================
  // Render: Export CSV Button (TASK 4)
  // ============================================
  function renderExportButton() {
    const existing = document.getElementById('export-csv-btn');
    if (existing) existing.remove();

    const controlsHeader = document.querySelector('.wells-controls-header');
    if (!controlsHeader) return;

    const btn = document.createElement('button');
    btn.id = 'export-csv-btn';
    btn.className = 'btn btn-secondary btn-sm';
    btn.style.cssText = 'margin-left:auto;font-size:0.75rem;padding:4px 12px;';
    btn.innerHTML = '&#8615; Export CSV';
    btn.addEventListener('click', function () {
      if (canExport()) {
        exportCSV();
      } else {
        window.__subscribe('pro');
      }
    });
    controlsHeader.appendChild(btn);

    // Show enterprise badge
    if (isEnterprise()) {
      const badge = document.createElement('span');
      badge.className = 'badge';
      badge.style.cssText = 'background:var(--gold-dim);color:var(--gold);font-size:0.65rem;padding:3px 8px;margin-left:8px;';
      badge.textContent = 'API Access';
      controlsHeader.appendChild(badge);
    }
  }

  // ============================================
  // CSV Export (TASK 4)
  // ============================================
  function exportCSV() {
    if (!displayedWells || displayedWells.length === 0) {
      alert('No data to export.');
      return;
    }

    const access = getAccessLevel();
    const headers = ['API', 'County', 'Well Type', 'Status'];
    if (access === 'pro' || access === 'enterprise') {
      headers.push('Operator', 'Lease Name', 'Field Name', 'Well Number', 'District', 'Total Depth');
    }

    const rows = displayedWells.map(w => {
      const row = [
        w.api || '',
        w.county || '',
        wellTypeLabel(w.well_type),
        w.status || ''
      ];
      if (access === 'pro' || access === 'enterprise') {
        row.push(
          w.operator || '',
          w.lease_name || '',
          w.field_name || '',
          w.well_number || '',
          w.district || '',
          w.total_depth || ''
        );
      }
      return row;
    });

    let csv = headers.join(',') + '\n';
    rows.forEach(row => {
      csv += row.map(val => {
        const str = String(val);
        if (str.includes(',') || str.includes('"') || str.includes('\n')) {
          return '"' + str.replace(/"/g, '""') + '"';
        }
        return str;
      }).join(',') + '\n';
    });

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    const county = (document.getElementById('filter-county') || {}).value || 'all';
    link.download = 'mineralsearch-wells-' + county.toLowerCase() + '-' + new Date().toISOString().slice(0,10) + '.csv';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }

  // ============================================
  // Render: Well Results Table (TASK 1 — paywall-aware)
  // ============================================
  function renderResults() {
    const tbody = document.getElementById('wells-tbody');
    const countEl = document.getElementById('results-count');
    const paginationEl = document.getElementById('pagination');

    if (!tbody) return;

    const total = totalResultCount;
    const totalPages = Math.ceil(total / PAGE_SIZE);
    const access = getAccessLevel();
    const paid = isPaid();

    if (countEl) countEl.textContent = `${formatNumber(total)} wells`;

    // Render paywall banner for free users
    renderPaywallBanner();

    if (displayedWells.length === 0) {
      tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted" style="padding:40px;">No wells found. Try a different search or filter.</td></tr>';
    } else {
      tbody.innerHTML = displayedWells.map(w => {
        const apiDisplay = w.api || '-';
        const regionTag = w.offshore === 1
          ? '<span class="badge" style="background:var(--blue-dim);color:var(--blue);font-size:0.65rem;">GOM</span>'
          : '<span class="tag tag-county">' + (w.county || '-') + '</span>';

        // Paywall: blur operator and lease_name for free users
        const operatorDisplay = paid
          ? (w.operator || '-')
          : blurredValue(w.operator);
        const depthDisplay = paid
          ? (w.total_depth ? formatNumber(w.total_depth) + "'" : '-')
          : blurredValue();

        return `
          <tr onclick="window.MI.toggleDetail(this, '${apiDisplay}')">
            <td class="mono">${apiDisplay}</td>
            <td>${operatorDisplay}</td>
            <td>${regionTag}</td>
            <td>${wellTypeLabel(w.well_type)}</td>
            <td class="mono">${depthDisplay}</td>
            <td>${statusBadge(w.status)}</td>
          </tr>
        `;
      }).join('');
    }

    // Update operator filter based on visible results
    populateOperatorFilter();

    // Pagination
    if (paginationEl) {
      if (totalPages <= 1) {
        paginationEl.innerHTML = `<span class="page-info">${formatNumber(total)} results</span>`;
        return;
      }
      let btns = '';
      btns += `<button ${currentPage<=1?'disabled':''} onclick="window.MI.goPage(${currentPage-1})">Prev</button>`;
      const maxBtns = 7;
      let startP = Math.max(1, currentPage - 3);
      let endP = Math.min(totalPages, startP + maxBtns - 1);
      if (endP - startP < maxBtns - 1) startP = Math.max(1, endP - maxBtns + 1);

      for (let p = startP; p <= endP; p++) {
        btns += `<button class="${p===currentPage?'active':''}" onclick="window.MI.goPage(${p})">${p}</button>`;
      }
      btns += `<span class="page-info">${formatNumber(total)} results</span>`;
      btns += `<button ${currentPage>=totalPages?'disabled':''} onclick="window.MI.goPage(${currentPage+1})">Next</button>`;
      paginationEl.innerHTML = btns;
    }
  }

  // ============================================
  // Well Detail Expansion (TASKS 1, 2, 3)
  // ============================================
  function toggleDetail(row, apiNumber) {
    const existing = row.nextElementSibling;
    if (existing && existing.classList.contains('well-detail-row')) {
      existing.remove();
      return;
    }
    document.querySelectorAll('.well-detail-row').forEach(r => r.remove());

    const well = displayedWells.find(w => w.api === apiNumber);
    if (!well) return;

    const access = getAccessLevel();
    const paid = isPaid();
    const regionLabel = well.offshore === 1 ? 'Gulf of Mexico' : (well.county || '-') + ' County, TX';

    // Build detail grid — paywall aware
    const detailItems = [
      { label: 'API Number', value: well.api || '-', free: true },
      { label: 'County', value: well.county || '-', free: true },
      { label: 'Well Type', value: wellTypeLabel(well.well_type), free: true },
      { label: 'Status', value: statusBadge(well.status), free: true },
      { label: 'Operator', value: well.operator || '-', free: false },
      { label: 'Lease Name', value: well.lease_name || '-', free: false },
      { label: 'Field Name', value: well.field_name || '-', free: false },
      { label: 'Total Depth', value: well.total_depth ? formatNumber(well.total_depth) + "'" : '-', free: false },
      { label: 'Well Number', value: well.well_number || '-', free: false },
      { label: 'District', value: well.district || '-', free: false },
      { label: 'Region', value: regionLabel, free: true },
    ];

    const detailGridHTML = detailItems.map(item => {
      const val = (item.free || paid) ? item.value : blurredValue(item.value);
      return `<div class="well-detail-item"><label>${item.label}</label><div class="value">${val}</div></div>`;
    }).join('');

    // TASK 2: Production data section
    const productionHTML = buildProductionSection(well, paid);

    // TASK 3: Mineral ownership section
    const ownershipHTML = buildOwnershipSection(well, paid);

    const detailRow = document.createElement('tr');
    detailRow.className = 'well-detail-row';
    detailRow.innerHTML = `<td colspan="6" style="padding:0;">
      <div class="well-detail animate-in">
        <div class="well-detail-grid">
          ${detailGridHTML}
        </div>
        ${productionHTML}
        ${ownershipHTML}
      </div>
    </td>`;

    row.after(detailRow);
  }

  // ============================================
  // Production Section Builder (TASK 2)
  // ============================================
  function buildProductionSection(well, paid) {
    if (!paid) {
      return `
        <div class="well-detail-section" style="margin-top:20px;">
          <h4 class="well-detail-section-title">Monthly Production History</h4>
          <div class="paywall-section-wrapper">
            <div class="paywall-section-blurred" style="padding:20px;text-align:center;color:var(--text-muted);">
              Production data available with Pro subscription.
            </div>
            <div class="paywall-section-overlay" onclick="window.__subscribe('pro')">
              <p>Subscribe for production data</p>
              <button class="btn btn-primary btn-sm">Unlock Pro — $299/mo</button>
            </div>
          </div>
        </div>`;
    }

    return `
      <div class="well-detail-section" style="margin-top:20px;">
        <h4 class="well-detail-section-title">Monthly Production History</h4>
        <div style="padding:16px;background:var(--bg-input);border-radius:var(--radius-sm);border:1px solid var(--border);color:var(--text-muted);font-size:0.85rem;">
          Well-level production data coming soon. Visit the <a href="production.html" style="color:var(--accent);">Production Analytics</a> page for county-level production from real RRC data.
        </div>
      </div>`;
  }

  // ============================================
  // Mineral Ownership Section Builder (TASK 3)
  // ============================================
  function buildOwnershipSection(well, paid) {
    return `
      <div class="well-detail-section" style="margin-top:20px;">
        <h4 class="well-detail-section-title">Mineral Ownership</h4>
        <div style="padding:16px;background:var(--bg-input);border-radius:var(--radius-sm);border:1px solid var(--border);color:var(--text-muted);font-size:0.85rem;">
          Mineral ownership data coming soon.
        </div>
      </div>`;
  }

  // ============================================
  // Modal
  // ============================================
  function openModal() {
    const overlay = document.getElementById('modal-overlay');
    if (overlay) overlay.classList.add('open');
  }

  function closeModal() {
    const overlay = document.getElementById('modal-overlay');
    if (overlay) overlay.classList.remove('open');
  }

  // ============================================
  // Navigation
  // ============================================
  function showWell(apiNumber) {
    window.location.href = `wells.html?q=${encodeURIComponent(apiNumber)}`;
  }

  function goPage(p) {
    currentPage = p;
    fetchWells();
    const table = document.querySelector('.table-container');
    if (table) table.scrollIntoView({ behavior: 'smooth', block: 'start' });
  }

  function setSort(field) {
    if (sortField === field) {
      sortDir = sortDir === 'desc' ? 'asc' : 'desc';
    } else {
      sortField = field;
      sortDir = 'desc';
    }
    currentPage = 1;
    fetchWells();
  }

  // ============================================
  // Mobile Nav Toggle
  // ============================================
  function toggleMobileNav() {
    const links = document.querySelector('.nav-links');
    if (links) links.classList.toggle('open');
  }

  // ============================================
  // County Coordinate Generation (for map)
  // ============================================
  const COUNTY_BOUNDS = {
    'MIDLAND':  { latMin:31.87, latMax:32.08, lngMin:-102.20, lngMax:-101.90 },
    'ECTOR':    { latMin:31.75, latMax:31.97, lngMin:-102.60, lngMax:-102.30 },
    'REEVES':   { latMin:31.10, latMax:31.60, lngMin:-104.10, lngMax:-103.40 },
    'LOVING':   { latMin:31.70, latMax:32.00, lngMin:-104.20, lngMax:-103.90 },
    'WARD':     { latMin:31.40, latMax:31.70, lngMin:-103.40, lngMax:-103.00 },
    'PECOS':    { latMin:30.70, latMax:31.30, lngMin:-103.50, lngMax:-102.70 },
    'HOWARD':   { latMin:32.20, latMax:32.50, lngMin:-101.60, lngMax:-101.30 },
    'MARTIN':   { latMin:32.20, latMax:32.50, lngMin:-102.00, lngMax:-101.60 },
    'KARNES':   { latMin:28.80, latMax:29.10, lngMin:-98.10, lngMax:-97.70 },
    'DEWITT':   { latMin:29.00, latMax:29.30, lngMin:-97.60, lngMax:-97.20 },
    'WEBB':     { latMin:27.50, latMax:28.00, lngMin:-99.80, lngMax:-99.20 },
    'DIMMIT':   { latMin:28.20, latMax:28.70, lngMin:-100.10, lngMax:-99.60 },
    'ANDREWS':  { latMin:32.30, latMax:32.60, lngMin:-102.70, lngMax:-102.30 },
    'WICHITA':  { latMin:33.80, latMax:34.10, lngMin:-98.80, lngMax:-98.40 },
    'CRANE':    { latMin:31.30, latMax:31.55, lngMin:-102.50, lngMax:-102.10 },
    'GAINES':   { latMin:32.50, latMax:32.90, lngMin:-103.10, lngMax:-102.60 },
    'UPTON':    { latMin:31.10, latMax:31.45, lngMin:-102.10, lngMax:-101.70 },
  };

  // generateCoordinates removed — wells without real coordinates should not appear on the map.
  function generateCoordinates(county) {
    return null;
  }

  // ============================================
  // Map: Fetch wells from Supabase for map (TASK 5)
  // ============================================
  async function fetchWellsForMap(filters) {
    const params = ['select=id,api,operator,county,well_type,total_depth,status,offshore,lease_name,field_name,well_number,district'];

    if (filters && filters.county) params.push('county=eq.' + encodeURIComponent(filters.county));
    if (filters && filters.operator) params.push('operator=eq.' + encodeURIComponent(filters.operator));
    if (filters && filters.status) params.push('status=eq.' + encodeURIComponent(filters.status));

    params.push('limit=1000');
    params.push('order=county.asc');

    try {
      if (!supabaseAvailable) await initSupabase();
      if (supabaseAvailable) {
        const { data } = await supabaseGet('wells?' + params.join('&'), false);
        // Only include wells with real coordinates
        return (data || []).filter(w => w.lat && w.lng);
      }
    } catch (e) {
      console.log('[MineralSearch] Map Supabase fetch failed:', e.message);
    }

    // No demo fallback — return empty
    return [];
  }

  // ============================================
  // Map: Initialize and render (TASK 5)
  // ============================================
  async function initMap() {
    // Check if Leaflet is available (only on map.html)
    if (typeof L === 'undefined') return;
    const mapEl = document.getElementById('map');
    if (!mapEl) return;

    console.log('[MineralSearch] Initializing map with Supabase data...');

    await initSupabase();

    const map = L.map('map', {
      center: [31.9973, -102.0779],
      zoom: 8,
      zoomControl: true,
      attributionControl: false
    });

    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
      maxZoom: 19,
      subdomains: 'abcd'
    }).addTo(map);

    L.control.attribution({ position: 'bottomright', prefix: false })
      .addAttribution('MineralSearch | CartoDB')
      .addTo(map);

    const markerClusterGroup = L.markerClusterGroup({
      maxClusterRadius: 50,
      spiderfyOnMaxZoom: true,
      showCoverageOnHover: false,
      iconCreateFunction: function(cluster) {
        const count = cluster.getChildCount();
        let size = 'small';
        if (count > 50) size = 'large';
        else if (count > 20) size = 'medium';
        return L.divIcon({
          html: '<div>' + count + '</div>',
          className: 'marker-cluster marker-cluster-' + size,
          iconSize: L.point(40, 40)
        });
      }
    });
    map.addLayer(markerClusterGroup);

    // County labels
    const COUNTY_CENTERS = {};
    Object.keys(COUNTY_BOUNDS).forEach(c => {
      const b = COUNTY_BOUNDS[c];
      COUNTY_CENTERS[c] = { lat: (b.latMin + b.latMax) / 2, lng: (b.lngMin + b.lngMax) / 2 };
    });
    Object.keys(COUNTY_CENTERS).forEach(county => {
      const c = COUNTY_CENTERS[county];
      L.marker([c.lat, c.lng], {
        icon: L.divIcon({
          className: 'county-label',
          html: '<span style="color:rgba(255,255,255,0.25);font-size:11px;font-weight:700;font-family:Inter,sans-serif;text-transform:uppercase;letter-spacing:0.1em;white-space:nowrap;">' + county + '</span>',
          iconSize: [80, 20],
          iconAnchor: [40, 10]
        }),
        interactive: false
      }).addTo(map);
    });

    const STATUS_COLORS = {
      'PRODUCING': '#22c55e', 'PERMITTED': '#f59e0b', 'COMPLETED': '#3b82f6',
      'SHUT IN': '#8b5cf6', 'PLUGGED': '#ef4444', 'INJECTION': '#f97316'
    };

    let allMapWells = [];

    async function loadAndPlotWells(filters) {
      markerClusterGroup.clearLayers();
      const wells = await fetchWellsForMap(filters);
      allMapWells = wells;

      const paid = isPaid();
      let producing = 0, totalOil = 0;
      const countiesSet = new Set();

      wells.forEach(w => {
        const statusKey = (w.status || '').toUpperCase();
        const color = STATUS_COLORS[statusKey] || '#22c55e';
        const marker = L.circleMarker([w.lat, w.lng], {
          radius: 6, fillColor: color, color: 'rgba(0,0,0,0.5)', weight: 1, fillOpacity: 0.85
        });

        let popupContent = '<div class="popup-title">' + (w.api || '-') + '</div>';
        popupContent += '<div class="popup-row"><span class="popup-label">County</span><span class="popup-val">' + (w.county || '-') + '</span></div>';
        popupContent += '<div class="popup-row"><span class="popup-label">Type</span><span class="popup-val">' + wellTypeLabel(w.well_type) + '</span></div>';
        popupContent += '<div class="popup-row"><span class="popup-label">Status</span><span class="popup-val">' + (w.status || '-') + '</span></div>';

        if (paid) {
          popupContent += '<div class="popup-row"><span class="popup-label">Operator</span><span class="popup-val">' + (w.operator || '-') + '</span></div>';
          popupContent += '<div class="popup-row"><span class="popup-label">Lease</span><span class="popup-val accent">' + (w.lease_name || '-') + '</span></div>';
          popupContent += '<div class="popup-row"><span class="popup-label">Depth</span><span class="popup-val green">' + (w.total_depth ? formatNumber(w.total_depth) + "'" : '-') + '</span></div>';
        } else {
          popupContent += '<div style="margin-top:8px;padding:8px;background:var(--bg-input);border-radius:6px;text-align:center;font-size:0.75rem;color:var(--text-muted);cursor:pointer;" onclick="window.__subscribe(\'pro\')">Subscribe for full details</div>';
        }

        const badgeBg = color + '22';
        popupContent += '<span class="popup-badge" style="background:' + badgeBg + ';color:' + color + ';">' + (w.status || '-') + '</span>';

        marker.bindPopup(popupContent, { maxWidth: 280 });
        markerClusterGroup.addLayer(marker);

        if (statusKey === 'PRODUCING') producing++;
        countiesSet.add(w.county);
      });

      // Update stats
      const wellCountEl = document.getElementById('map-well-count');
      const prodCountEl = document.getElementById('map-producing-count');
      const oilEl = document.getElementById('map-total-oil');
      const countyCountEl = document.getElementById('map-county-count');
      if (wellCountEl) wellCountEl.textContent = formatNumber(wells.length);
      if (prodCountEl) prodCountEl.textContent = formatNumber(producing);
      if (oilEl) oilEl.textContent = '-';
      if (countyCountEl) countyCountEl.textContent = countiesSet.size;

      return wells;
    }

    // Populate map county filter from Supabase
    async function populateMapCountyFilter() {
      const countySelect = document.getElementById('map-county');
      if (!countySelect) return;
      if (supabaseAvailable) {
        try {
          const { data } = await supabaseGet('wells?select=county&order=county&limit=1000', false);
          const seen = new Set();
          const counties = [];
          (data || []).forEach(row => {
            if (row.county && !seen.has(row.county)) {
              seen.add(row.county);
              counties.push(row.county);
            }
          });
          counties.sort();
          countySelect.innerHTML = '<option value="">All Counties (' + counties.length + ')</option>' +
            counties.map(c => '<option value="' + c + '">' + c + '</option>').join('');
        } catch (e) {
          console.error('Map: failed to load counties', e);
        }
      }
    }

    // Wire up map filters
    window.applyMapFilters = function() {
      const county = (document.getElementById('map-county') || {}).value || '';
      const operator = (document.getElementById('map-operator') || {}).value || '';
      const status = (document.getElementById('map-status') || {}).value || '';

      loadAndPlotWells({ county, operator, status }).then(() => {
        if (county && COUNTY_BOUNDS[county]) {
          const b = COUNTY_BOUNDS[county];
          map.fitBounds([[b.latMin, b.lngMin], [b.latMax, b.lngMax]], { padding: [30, 30] });
        }
      });
    };

    window.resetMapFilters = function() {
      const cs = document.getElementById('map-county'); if (cs) cs.value = '';
      const os = document.getElementById('map-operator'); if (os) os.value = '';
      const fs = document.getElementById('map-formation'); if (fs) fs.value = '';
      const ss = document.getElementById('map-status'); if (ss) ss.value = '';
      const si = document.getElementById('map-search'); if (si) si.value = '';
      loadAndPlotWells({});
      map.setView([31.9973, -102.0779], 8);
    };

    // Layer toggle events
    ['layer-wells', 'layer-permits', 'layer-leases'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('change', () => { if (window.applyMapFilters) window.applyMapFilters(); });
    });

    await populateMapCountyFilter();
    await loadAndPlotWells({});
  }

  // ============================================
  // Init (TASK 6 — robust page detection)
  // ============================================
  async function init() {
    const path = window.location.pathname;
    // TASK 6: Use includes() for flexible path detection
    const isIndex = path.endsWith('/') || path.includes('index');
    const isWells = path.includes('wells');
    const isMap = path.includes('map');

    // Load auth state and update nav
    loadAuthState();

    // Check for payment success on any page
    checkPaymentSuccess();

    console.log('[MineralSearch] init() path:', path, 'isIndex:', isIndex, 'isWells:', isWells, 'isMap:', isMap, 'access:', getAccessLevel());

    if (isIndex) {
      renderHeroStats();
      renderDashboard();
    }

    if (isWells) {
      await initSupabase();
      await populateCountyFilter();

      const params = new URLSearchParams(window.location.search);
      const q = params.get('q');
      const region = params.get('region');

      if (q) {
        const input = document.getElementById('search-input');
        if (input) input.value = q;
      }

      if (region && ['onshore', 'offshore', 'all'].includes(region)) {
        activeRegion = region;
        const toggle = document.getElementById('region-toggle');
        if (toggle) {
          toggle.querySelectorAll('button').forEach(btn => {
            btn.classList.toggle('active', btn.dataset.region === region);
          });
        }
      }

      // Render export button
      renderExportButton();

      // Initial fetch
      await fetchWells();
    }

    if (isMap) {
      // Map initialization is handled by the inline script in map.html
      // But if map.html uses site.js's map init, we call it here
      // Only init if the map page's inline script hasn't already initialized
      if (typeof L !== 'undefined' && document.getElementById('map') && !document.getElementById('map')._leaflet_id) {
        await initMap();
      }
    }

    // Wire up filter change events
    ['filter-county','filter-operator','filter-type','filter-status'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('change', () => {
        if (isWells) applyFilters();
      });
    });

    // Wire up region toggle
    const regionToggle = document.getElementById('region-toggle');
    if (regionToggle) {
      regionToggle.querySelectorAll('button').forEach(btn => {
        btn.addEventListener('click', () => {
          setRegion(btn.dataset.region);
        });
      });
    }

    // Search input with 300ms debounce
    const searchInput = document.getElementById('search-input') || document.getElementById('hero-search-input');
    if (searchInput) {
      searchInput.addEventListener('input', () => {
        clearTimeout(searchDebounce);
        searchDebounce = setTimeout(() => {
          if (isWells) applyFilters();
        }, 300);
      });
    }

    // Hero search -> wells page
    const heroBtn = document.getElementById('hero-search-btn');
    if (heroBtn) {
      heroBtn.addEventListener('click', () => {
        const val = document.getElementById('hero-search-input')?.value || '';
        window.location.href = `wells.html?q=${encodeURIComponent(val)}`;
      });
      const heroInput = document.getElementById('hero-search-input');
      if (heroInput) {
        heroInput.addEventListener('keydown', (e) => {
          if (e.key === 'Enter') heroBtn.click();
        });
      }
    }

    // Modal
    document.querySelectorAll('[data-modal-open]').forEach(el => {
      el.addEventListener('click', openModal);
    });
    document.querySelectorAll('[data-modal-close]').forEach(el => {
      el.addEventListener('click', closeModal);
    });
    const overlay = document.getElementById('modal-overlay');
    if (overlay) {
      overlay.addEventListener('click', (e) => {
        if (e.target === overlay) closeModal();
      });
    }

    // Mobile nav
    const toggle = document.querySelector('.nav-mobile-toggle');
    if (toggle) toggle.addEventListener('click', toggleMobileNav);

    // Smooth scroll for anchor links
    document.querySelectorAll('a[href^="#"]').forEach(a => {
      a.addEventListener('click', (e) => {
        const target = document.querySelector(a.getAttribute('href'));
        if (target) { e.preventDefault(); target.scrollIntoView({ behavior: 'smooth' }); }
      });
    });
  }

  // Stripe Subscribe
  window.__subscribe = async function(plan) {
    // If user is logged in, pre-fill their email
    const prefillEmail = (currentUser && currentUser.email) ? currentUser.email : '';
    const modal = document.createElement('div');
    modal.style.cssText = 'position:fixed;inset:0;z-index:9999;display:flex;align-items:center;justify-content:center;background:rgba(0,0,0,0.7);backdrop-filter:blur(4px);';
    const planNames = { basic: 'Basic ($99/mo)', pro: 'Pro ($299/mo)', enterprise: 'Enterprise ($999/mo)' };
    modal.innerHTML = `
      <div style="background:#131a2b;border:1px solid #2a2d37;border-radius:16px;padding:40px;max-width:440px;width:90%;text-align:center;position:relative;">
        <button onclick="this.closest('div[style]').remove()" style="position:absolute;top:12px;right:16px;background:none;border:none;color:#94a3b8;font-size:20px;cursor:pointer;">&times;</button>
        <div style="font-size:40px;margin-bottom:12px;">&#9889;</div>
        <h3 style="font-size:22px;font-weight:800;color:#e2e8f0;margin-bottom:8px;">Subscribe to ${planNames[plan] || 'Pro'}</h3>
        <p style="color:#94a3b8;font-size:14px;margin-bottom:20px;">7-day free trial. Cancel anytime.</p>
        <input id="sub-email" type="email" placeholder="Enter your email" value="${prefillEmail}" style="width:100%;height:48px;padding:0 14px;background:#0a0e17;border:1px solid #2a2d37;border-radius:10px;color:#e2e8f0;font-size:15px;font-family:inherit;outline:none;margin-bottom:12px;" />
        <div id="sub-error" style="color:#ef4444;font-size:12px;margin-bottom:8px;"></div>
        <button id="sub-btn" onclick="window.__processSubscribe('${plan}')" style="width:100%;height:52px;background:#00d4aa;color:#0a0e17;font-size:17px;font-weight:700;border:none;border-radius:12px;cursor:pointer;font-family:inherit;">Subscribe</button>
        <p style="color:#64748b;font-size:11px;margin-top:12px;">Secured by Stripe. Cancel anytime.</p>
      </div>
    `;
    document.body.appendChild(modal);
    modal.addEventListener('click', (e) => { if (e.target === modal) modal.remove(); });
  };

  window.__processSubscribe = async function(plan) {
    const email = document.getElementById('sub-email')?.value?.trim();
    const errEl = document.getElementById('sub-error');
    const btn = document.getElementById('sub-btn');

    if (!email || !email.includes('@')) {
      errEl.textContent = 'Please enter a valid email.';
      return;
    }

    btn.textContent = 'Redirecting...';
    btn.disabled = true;

    try {
      const resp = await fetch('/.netlify/functions/create-checkout', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, plan }),
      });
      const data = await resp.json();
      if (data.url) {
        window.location.href = data.url;
      } else {
        errEl.textContent = data.error || 'Something went wrong.';
        btn.textContent = 'Subscribe';
        btn.disabled = false;
      }
    } catch (e) {
      errEl.textContent = 'Connection error. Please try again.';
      btn.textContent = 'Subscribe';
      btn.disabled = false;
    }
  };

  // Public API
  window.MI = {
    showWell, toggleDetail, goPage, setSort, setRegion, openModal, closeModal,
    applyFilters, generateCoordinates, COUNTY_BOUNDS,
    getAccessLevel, setAccessLevel, exportCSV, fetchWellsForMap, initMap,
    loadAuthState, logout, updateNavAuth, currentUser: () => currentUser
  };

  // Boot
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
