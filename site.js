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
  function resolveFilterValue(inputId, hiddenId, list) {
    const input = document.getElementById(inputId);
    const hidden = document.getElementById(hiddenId);
    if (input && hidden && !hidden.value && input.value.trim()) {
      const term = input.value.trim().toUpperCase();
      if (list) {
        const match = list.find(c => c === term);
        if (match) { hidden.value = match; input.value = match; }
      } else {
        hidden.value = term;
      }
    }
    return (hidden || input || {}).value || '';
  }

  function buildQueryParams() {
    const county = resolveFilterValue('filter-county', 'filter-county-value', TX_COUNTIES);
    const operator = resolveFilterValue('filter-operator', 'filter-operator-value', null);
    const wellType = (document.getElementById('filter-type') || {}).value || '';
    const search = (document.getElementById('search-input') || document.getElementById('hero-search-input') || {}).value || '';

    const params = [];
    params.push('select=id,api,operator,county,well_type,total_depth,status,offshore,lease_name,field_name,well_number,district');

    if (county) params.push('county=eq.' + encodeURIComponent(county));
    if (operator) {
      params.push('operator=eq.' + encodeURIComponent(operator).replace(/\./g, '%2E'));
    }
    if (wellType) params.push('well_type=eq.' + encodeURIComponent(wellType));

    if (activeRegion === 'onshore') params.push('offshore=eq.false');
    if (activeRegion === 'offshore') params.push('offshore=eq.true');

    if (search.trim()) {
      const term = search.trim().replace(/%/g, '');
      // If it looks like an API number (starts with 42- or has dashes), use exact match
      if (/^\d{2}-\d{3}-/.test(term)) {
        params.push('api=eq.' + encodeURIComponent(term));
      } else {
        const encoded = encodeURIComponent(term + '%').replace(/\./g, '%2E');
        params.push('or=(operator.ilike.' + encoded + ',lease_name.ilike.' + encoded + ')');
      }
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
        const hasIlike = query.includes('ilike.');
        console.log('[MineralSearch] Querying Supabase:', query.substring(0, 200));
        const { data, totalCount } = await supabaseGet('wells?' + query, !hasIlike);
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
  // All 275 Texas counties with RRC well data
  const TX_COUNTIES = ["ANDERSON","ANDREWS","ANGELINA","ARANSAS","ARCHER","ARMSTRONG","ATASCOSA","AUSTIN","BAILEY","BANDERA","BASTROP","BAYLOR","BEE","BELL","BEXAR","BLANCO","BORDEN","BOSQUE","BOWIE","BRAZORIA","BRAZOS","BREWSTER","BRISCOE","BROOKS","BROWN","BURLESON","BURNET","CALDWELL","CALHOUN","CALLAHAN","CAMERON","CAMP","CARSON","CASS","CASTRO","CHAMBERS","CHEROKEE","CHILDRESS","CLAY","COCHRAN","COKE","COLEMAN","COLLIN","COLLINGSWORTH","COLORADO","COMAL","COMANCHE","CONCHO","COOKE","CORYELL","COTTLE","CRANE","CROCKETT","CROSBY","CULBERSON","DALLAM","DALLAS","DAWSON","DE WITT","DEAF SMITH","DELTA","DENTON","DICKENS","DIMMIT","DONLEY","DUVAL","EASTLAND","ECTOR","EDWARDS","EL PASO","ELLIS","ERATH","FALLS","FANNIN","FAYETTE","FISHER","FLOYD","FOARD","FORT BEND","FRANKLIN","FREESTONE","FRIO","GAINES","GALVESTON","GARZA","GILLESPIE","GLASSCOCK","GOLIAD","GONZALES","GRAY","GRAYSON","GREGG","GRIMES","GUADALUPE","HALE","HALL","HAMILTON","HANSFORD","HARDEMAN","HARDIN","HARRIS","HARRISON","HARTLEY","HASKELL","HAYS","HEMPHILL","HENDERSON","HIDALGO","HILL","HOCKLEY","HOOD","HOPKINS","HOUSTON","HOWARD","HUDSPETH","HUNT","HUTCHINSON","IRION","JACK","JACKSON","JASPER","JEFF DAVIS","JEFFERSON","JIM HOGG","JIM WELLS","JOHNSON","JONES","KARNES","KAUFMAN","KENDALL","KENEDY","KENT","KERR","KIMBLE","KING","KINNEY","KLEBERG","KNOX","LA SALLE","LAMAR","LAMB","LAMPASAS","LAVACA","LEE","LEON","LIBERTY","LIMESTONE","LIPSCOMB","LIVE OAK","LLANO","LOVING","LUBBOCK","LYNN","MADISON","MARION","MARTIN","MASON","MATAGORDA","MAVERICK","MCCULLOCH","MCLENNAN","MCMULLEN","MEDINA","MENARD","MIDLAND","MILAM","MILLS","MITCHELL","MONTAGUE","MONTGOMERY","MOORE","MORRIS","MOTLEY","NACOGDOCHES","NAVARRO","NEWTON","NOLAN","NUECES","OCHILTREE","OLDHAM","ORANGE","PALO PINTO","PANOLA","PARKER","PARMER","PECOS","POLK","POTTER","PRESIDIO","RAINS","RANDALL","REAGAN","REAL","RED RIVER","REEVES","REFUGIO","ROBERTS","ROBERTSON","ROCKWALL","RUNNELS","RUSK","SABINE","SAN AUGUSTINE","SAN JACINTO","SAN PATRICIO","SAN SABA","SCHLEICHER","SCURRY","SHACKELFORD","SHELBY","SHERMAN","SMITH","SOMERVELL","STARR","STEPHENS","STERLING","STONEWALL","SUTTON","SWISHER","TARRANT","TAYLOR","TERRELL","TERRY","THROCKMORTON","TITUS","TOM GREEN","TRAVIS","TRINITY","TYLER","UPSHUR","UPTON","UVALDE","VAL VERDE","VAN ZANDT","VICTORIA","WALKER","WALLER","WARD","WASHINGTON","WEBB","WHARTON","WHEELER","WICHITA","WILBARGER","WILLACY","WILLIAMSON","WILSON","WINKLER","WISE","WOOD","YOAKUM","YOUNG","ZAPATA","ZAVALA"];

  async function populateCountyFilter() {
    const input = document.getElementById('filter-county');
    const hidden = document.getElementById('filter-county-value');
    const dropdown = document.getElementById('county-dropdown');
    if (!input || !dropdown) return;
    countyList = TX_COUNTIES;

    input.addEventListener('input', function() {
      const term = input.value.trim().toUpperCase();
      if (hidden) hidden.value = '';
      if (term.length < 1) { dropdown.style.display = 'none'; return; }
      const matches = TX_COUNTIES.filter(c => c.includes(term)).slice(0, 15);
      if (!matches.length) { dropdown.style.display = 'none'; return; }
      dropdown.innerHTML = matches.map(c =>
        '<div style="padding:8px 12px;cursor:pointer;font-size:0.85rem;color:var(--text);border-bottom:1px solid var(--border);" onmousedown="event.preventDefault()" onclick="document.getElementById(\'filter-county\').value=this.textContent;document.getElementById(\'filter-county-value\').value=this.textContent;document.getElementById(\'county-dropdown\').style.display=\'none\';if(window.MineralSearch)window.MineralSearch.applyFilters();">' + c + '</div>'
      ).join('');
      dropdown.style.display = 'block';
    });

    input.addEventListener('blur', () => { setTimeout(() => { dropdown.style.display = 'none'; }, 200); });
    input.addEventListener('keydown', function(e) {
      if (e.key === 'Escape') { input.value = ''; if (hidden) hidden.value = ''; dropdown.style.display = 'none'; if(window.MineralSearch) window.MineralSearch.applyFilters(); }
      if (e.key === 'Enter') { dropdown.style.display = 'none'; if(window.MineralSearch) window.MineralSearch.applyFilters(); }
    });
  }

  // ============================================
  // Populate Operator Filter from current results
  // ============================================
  // Operator search-as-you-type
  let operatorSearchTimeout = null;
  function initOperatorSearch() {
    const input = document.getElementById('filter-operator');
    const hidden = document.getElementById('filter-operator-value');
    const dropdown = document.getElementById('operator-dropdown');
    if (!input || !dropdown) return;

    input.addEventListener('input', function() {
      const term = input.value.trim();
      if (hidden) hidden.value = '';
      clearTimeout(operatorSearchTimeout);
      if (term.length < 2) { dropdown.style.display = 'none'; return; }
      operatorSearchTimeout = setTimeout(async () => {
        try {
          const encoded = encodeURIComponent(term + '%');
          const resp = await fetch(SUPABASE_URL + '/rest/v1/wells?select=operator&operator=ilike.' + encoded + '&limit=200', {
            headers: { 'apikey': SUPABASE_ANON_KEY, 'Authorization': 'Bearer ' + SUPABASE_ANON_KEY }
          });
          const data = await resp.json();
          if (!data || !data.length) { dropdown.style.display = 'none'; return; }
          const unique = [...new Set(data.map(r => r.operator).filter(Boolean))].sort().slice(0, 20);
          if (!unique.length) { dropdown.style.display = 'none'; return; }
          dropdown.innerHTML = unique.map(name =>
            '<div style="padding:8px 12px;cursor:pointer;font-size:0.85rem;color:var(--text);border-bottom:1px solid var(--border);" onmousedown="event.preventDefault()" onclick="document.getElementById(\'filter-operator\').value=this.textContent;document.getElementById(\'filter-operator-value\').value=this.textContent;document.getElementById(\'operator-dropdown\').style.display=\'none\';if(window.MineralSearch)window.MineralSearch.applyFilters();">' + name + '</div>'
          ).join('');
          dropdown.style.display = 'block';
        } catch(e) { dropdown.style.display = 'none'; }
      }, 300);
    });

    input.addEventListener('blur', () => { setTimeout(() => { dropdown.style.display = 'none'; }, 200); });

    // Clear button behavior — if they clear the input, reset the filter
    input.addEventListener('keydown', function(e) {
      if (e.key === 'Escape') { input.value = ''; if (hidden) hidden.value = ''; dropdown.style.display = 'none'; if(window.MineralSearch) window.MineralSearch.applyFilters(); }
      if (e.key === 'Enter') { dropdown.style.display = 'none'; if(window.MineralSearch) window.MineralSearch.applyFilters(); }
    });
  }

  function populateOperatorFilter() {
    // No-op — operators now use search-as-you-type
    initOperatorSearch();
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
  // Mineral Ownership Section Builder
  // ============================================
  // Real mineral ownership data from Anderson County CAD (True Prodigy API)
  const WELL_OWNERSHIP = {
    '42-001-00131017': {
      lease: 'FLOCKHART A.C.',
      field: 'TENNESSEE COLONY (ROD.)',
      totalAccounted: '97.5358%',
      owners: [
        { owner: 'PETRALIS ENERGY PARTNERS', interest: '75.7823%' },
        { owner: 'HILL LAND & MINERALS LLC', interest: '1.8311%' },
        { owner: 'BLECKLEY FAMILY TRUST', interest: '1.3824%' },
        { owner: '2DFLYS LLC', interest: '1.3824%' },
        { owner: 'WALKER SARAH K', interest: '1.0546%' },
        { owner: 'GUADALUPE ROYALTIES LLC', interest: '1.0255%' },
        { owner: 'COMBS GORDON MITCHELL GST', interest: '0.9155%' },
        { owner: 'HOUSTON OIL & GAS CO INC', interest: '0.8818%' },
        { owner: 'SABINE ROYALTY TRUST', interest: '0.8204%' },
        { owner: 'BASSO LIVIA S REV TRUST DCSD', interest: '0.7813%' },
        { owner: 'JORDAN MARGARET ROYALTIES INC EST', interest: '0.5859%' },
        { owner: 'BRADLEY J L & SONS', interest: '0.5859%' },
        { owner: 'LARRY STAUB INC', interest: '0.5273%' },
        { owner: 'BRESLAUER CAROL M', interest: '0.5209%' },
        { owner: 'JACKSON DARYL WAYNE', interest: '0.5208%' },
        { owner: 'SEMPRA ENERGY CASH BAL PLAN', interest: '0.4923%' },
        { owner: 'NELSON LOIS MERLENE', interest: '0.3845%' },
        { owner: 'MOORE DIANNE ELIZABETH EASTON', interest: '0.3815%' },
        { owner: 'GORTON PAUL S', interest: '0.3815%' },
        { owner: 'GORTON GLENN G', interest: '0.3815%' },
        { owner: 'EASTON DONALD EDWARD', interest: '0.3815%' },
        { owner: 'HENRY GARY', interest: '0.3744%' },
        { owner: 'HENRY DANA', interest: '0.3744%' },
        { owner: 'GLENN ANNA SEXTON', interest: '0.3744%' },
        { owner: 'LESS J R & G N FAMILY TR AGCY', interest: '0.3690%' },
        { owner: 'MEADOWS MITCHELL', interest: '0.3515%' },
        { owner: 'MEADOWS JOHN STEPHEN', interest: '0.3515%' },
        { owner: "O'GUYNN EDITH DAVIS", interest: '0.3052%' },
        { owner: 'FLUKINGER MARY REID', interest: '0.3052%' },
        { owner: 'DAVIS J BRYAN', interest: '0.3052%' },
        { owner: 'DAVIS CREDIT SHELTER TRUST', interest: '0.3052%' },
        { owner: 'UNIVERSITY OF TEXAS SYSTEM', interest: '0.2931%' },
        { owner: 'NEWTON REAGAN', interest: '0.2500%' },
        { owner: 'UNTERBERG SUSAN APPLEMAN AGCY', interest: '0.2051%' },
        { owner: 'HUGHES WILLIAM M', interest: '0.1953%' },
        { owner: 'HOLOTIK JIM M', interest: '0.1953%' },
        { owner: 'HARDY WILLIAM', interest: '0.1953%' },
        { owner: 'CLAY JERRY H OIL & GAS PROP LP', interest: '0.1953%' },
        { owner: 'THETFORD MICHAEL M', interest: '0.1831%' },
        { owner: 'HEMBREE SANDRA T', interest: '0.1831%' },
        { owner: 'MEADOWS ELIZABETH', interest: '0.1758%' },
        { owner: 'MEADOWS ALEX', interest: '0.1758%' },
        { owner: 'BRIDGWATER GILBERT ALAN', interest: '0.1283%' },
        { owner: 'RANDLE CAROL JAN', interest: '0.1282%' },
        { owner: 'PARKER KATELYNN', interest: '0.1282%' },
        { owner: 'MCDANIEL CHRISTI JONES', interest: '0.1282%' },
        { owner: 'MCCLELLAN WILLIAM ALAN', interest: '0.1282%' },
        { owner: 'JONES STACY LOUISE', interest: '0.1282%' },
        { owner: 'KIDD BARRON U AGENCY', interest: '0.1024%' },
      ],
      deedHistory: [],
      source: 'Anderson County Appraisal District & County Clerk',
      lastUpdated: 'Mar 20, 2026',
    }
  };

  function buildOwnershipSection(well, paid) {
    const ownership = WELL_OWNERSHIP[well.api];

    if (!ownership) {
      return `
        <div class="well-detail-section" style="margin-top:20px;">
          <h4 class="well-detail-section-title">Mineral Ownership</h4>
          <div style="padding:16px;background:var(--bg-input);border-radius:var(--radius-sm);border:1px solid var(--border);color:var(--text-muted);font-size:0.85rem;">
            Mineral ownership data is being compiled for this well. Check back soon.
          </div>
        </div>`;
    }

    if (!paid) {
      return `
        <div class="well-detail-section" style="margin-top:20px;">
          <h4 class="well-detail-section-title">Mineral Ownership</h4>
          <div style="padding:16px;background:var(--bg-input);border-radius:var(--radius-sm);border:1px solid var(--border);text-align:center;">
            <div style="color:var(--text);font-weight:600;margin-bottom:6px;">Ownership data available</div>
            <div style="color:var(--text-muted);font-size:0.85rem;margin-bottom:12px;">${ownership.owners.length} mineral interest owners on file &middot; ${ownership.totalAccounted} accounted for</div>
            <div style="cursor:pointer;color:var(--accent);font-weight:600;font-size:0.85rem;" onclick="window.__subscribe('pro')">Subscribe to view full ownership breakdown</div>
          </div>
        </div>`;
    }

    let ownersHTML = ownership.owners.map((o, i) => `
      <div style="display:flex;justify-content:space-between;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);${i === 0 ? 'background:rgba(0,212,170,0.05);margin:0 -16px;padding:8px 16px;' : ''}">
        <div style="color:var(--text);font-size:0.85rem;${i === 0 ? 'font-weight:700;' : ''}">${o.owner}</div>
        <div style="color:var(--accent);font-weight:600;font-family:JetBrains Mono,monospace;font-size:0.85rem;white-space:nowrap;margin-left:12px;">${o.interest}</div>
      </div>
    `).join('');

    return `
      <div class="well-detail-section" style="margin-top:20px;">
        <h4 class="well-detail-section-title">Mineral Ownership Breakdown</h4>
        <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
          <span style="color:var(--text-muted);font-size:0.8rem;">${ownership.owners.length} mineral interest owners</span>
          <span style="color:var(--accent);font-size:0.8rem;font-weight:600;">${ownership.totalAccounted} accounted for</span>
        </div>
        <div style="background:var(--bg-input);border-radius:var(--radius-sm);border:1px solid var(--border);padding:4px 16px;max-height:500px;overflow-y:auto;">
          ${ownersHTML}
        </div>
        <div style="margin-top:8px;font-size:0.75rem;color:var(--text-muted);">
          Source: ${ownership.source} &middot; Last updated: ${ownership.lastUpdated}
        </div>
      </div>` + (ownership.deedHistory && ownership.deedHistory.length > 0 ? `
      <div class="well-detail-section" style="margin-top:20px;">
        <h4 class="well-detail-section-title">Deed & Transfer History</h4>
        <div style="background:var(--bg-input);border-radius:var(--radius-sm);border:1px solid var(--border);padding:4px 16px;">
          ${ownership.deedHistory.map(h => `
            <div style="padding:10px 0;border-bottom:1px solid var(--border);display:flex;gap:12px;">
              <div style="color:var(--accent);font-family:JetBrains Mono,monospace;font-size:0.8rem;white-space:nowrap;min-width:110px;">${h.date}</div>
              <div style="color:var(--text-secondary);font-size:0.85rem;line-height:1.5;">${h.text}</div>
            </div>
          `).join('')}
        </div>
        <div style="margin-top:8px;font-size:0.75rem;color:var(--text-muted);">
          Source: Anderson County Clerk — publicsearch.us
        </div>
      </div>` : '');
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
    'ANDERSON': { latMin: 31.5041, latMax: 32.0845, lngMin: -96.0648, lngMax: -95.2589 },
    'ANDREWS': { latMin: 32.0858, latMax: 32.5234, lngMin: -103.0647, lngMax: -102.211 },
    'ANGELINA': { latMin: 31.0264, latMax: 31.5269, lngMin: -95.0055, lngMax: -94.1296 },
    'ARANSAS': { latMin: 27.8395, latMax: 28.3192, lngMin: -97.2608, lngMax: -96.7905 },
    'ARCHER': { latMin: 33.3954, latMax: 33.836, lngMin: -98.9539, lngMax: -98.4207 },
    'ARMSTRONG': { latMin: 34.7474, latMax: 35.1832, lngMin: -101.6294, lngMax: -101.0863 },
    'ATASCOSA': { latMin: 28.6127, latMax: 29.2507, lngMin: -98.8049, lngMax: -98.0983 },
    'AUSTIN': { latMin: 29.6017, latMax: 30.0968, lngMin: -96.622, lngMax: -96.0046 },
    'BAILEY': { latMin: 33.8246, latMax: 34.3129, lngMin: -103.0473, lngMax: -102.6151 },
    'BANDERA': { latMin: 29.5518, latMax: 29.9077, lngMin: -99.6033, lngMax: -98.7788 },
    'BASTROP': { latMin: 29.7865, latMax: 30.4196, lngMin: -97.6494, lngMax: -97.0245 },
    'BAYLOR': { latMin: 33.3975, latMax: 33.8342, lngMin: -99.4757, lngMax: -98.9529 },
    'BEE': { latMin: 28.1297, latMax: 28.7196, lngMin: -98.0898, lngMax: -97.3756 },
    'BELL': { latMin: 30.7524, latMax: 31.3202, lngMin: -97.9138, lngMax: -97.0701 },
    'BEXAR': { latMin: 29.1144, latMax: 29.7607, lngMin: -98.8066, lngMax: -98.1169 },
    'BLANCO': { latMin: 29.9376, latMax: 30.5021, lngMin: -98.5917, lngMax: -98.1256 },
    'BORDEN': { latMin: 32.524, latMax: 32.9636, lngMin: -101.6913, lngMax: -101.1734 },
    'BOSQUE': { latMin: 31.5878, latMax: 32.2074, lngMin: -98.0055, lngMax: -97.2767 },
    'BOWIE': { latMin: 33.237, latMax: 33.716, lngMin: -94.7469, lngMax: -94.0429 },
    'BRAZORIA': { latMin: 28.8256, latMax: 29.5991, lngMin: -95.874, lngMax: -95.0571 },
    'BRAZOS': { latMin: 30.3303, latMax: 30.9737, lngMin: -96.6018, lngMax: -96.08 },
    'BREWSTER': { latMin: 28.9716, latMax: 30.6659, lngMin: -103.8007, lngMax: -102.3207 },
    'BRISCOE': { latMin: 34.3122, latMax: 34.7484, lngMin: -101.4721, lngMax: -100.9449 },
    'BROOKS': { latMin: 26.7809, latMax: 27.2652, lngMin: -98.5366, lngMax: -97.9855 },
    'BROWN': { latMin: 31.4482, latMax: 32.0796, lngMin: -99.2035, lngMax: -98.6684 },
    'BURLESON': { latMin: 30.2955, latMax: 30.7308, lngMin: -96.9636, lngMax: -96.2718 },
    'BURNET': { latMin: 30.4262, latMax: 31.035, lngMin: -98.4592, lngMax: -97.8281 },
    'CALDWELL': { latMin: 29.6307, latMax: 30.0716, lngMin: -97.8986, lngMax: -97.3158 },
    'CALHOUN': { latMin: 28.0608, latMax: 28.7303, lngMin: -96.9304, lngMax: -96.3227 },
    'CALLAHAN': { latMin: 32.0791, latMax: 32.5153, lngMin: -99.6314, lngMax: -99.114 },
    'CAMERON': { latMin: 25.8374, latMax: 26.4112, lngMin: -97.8622, lngMax: -97.1461 },
    'CAMP': { latMin: 32.9024, latMax: 33.0773, lngMin: -95.1526, lngMax: -94.7199 },
    'CARSON': { latMin: 35.1817, latMax: 35.6253, lngMin: -101.6235, lngMax: -101.0859 },
    'CASS': { latMin: 32.8795, latMax: 33.3121, lngMin: -94.654, lngMax: -94.0427 },
    'CASTRO': { latMin: 34.3127, latMax: 34.7482, lngMin: -102.5258, lngMax: -101.998 },
    'CHAMBERS': { latMin: 29.5242, latMax: 29.8899, lngMin: -94.9815, lngMax: -94.3534 },
    'CHEROKEE': { latMin: 31.4257, latMax: 32.138, lngMin: -95.4621, lngMax: -94.8659 },
    'CHILDRESS': { latMin: 34.3118, latMax: 34.7475, lngMin: -100.4178, lngMax: -99.9975 },
    'CLAY': { latMin: 33.4663, latMax: 34.1571, lngMin: -98.4236, lngMax: -97.9457 },
    'COCHRAN': { latMin: 33.3882, latMax: 33.8251, lngMin: -103.0567, lngMax: -102.5948 },
    'COKE': { latMin: 31.6925, latMax: 32.0867, lngMin: -100.8254, lngMax: -100.2351 },
    'COLEMAN': { latMin: 31.4098, latMax: 32.0821, lngMin: -99.7224, lngMax: -99.1959 },
    'COLLIN': { latMin: 32.9815, latMax: 33.4055, lngMin: -96.8441, lngMax: -96.2951 },
    'COLLINGSWORTH': { latMin: 34.7464, latMax: 35.1831, lngMin: -100.541, lngMax: -100.0004 },
    'COLORADO': { latMin: 29.2478, latMax: 29.9615, lngMin: -96.8742, lngMax: -96.1754 },
    'COMAL': { latMin: 29.5945, latMax: 30.038, lngMin: -98.6462, lngMax: -97.9993 },
    'COMANCHE': { latMin: 31.684, latMax: 32.2614, lngMin: -98.9244, lngMax: -98.1566 },
    'CONCHO': { latMin: 31.087, latMax: 31.5803, lngMin: -100.1152, lngMax: -99.6015 },
    'COOKE': { latMin: 33.4164, latMax: 33.9564, lngMin: -97.4871, lngMax: -96.9433 },
    'CORYELL': { latMin: 31.0694, latMax: 31.711, lngMin: -98.18, lngMax: -97.4186 },
    'COTTLE': { latMin: 33.8357, latMax: 34.3141, lngMin: -100.5187, lngMax: -99.9976 },
    'CRANE': { latMin: 31.0862, latMax: 31.6517, lngMin: -102.7677, lngMax: -102.3009 },
    'CROCKETT': { latMin: 30.2873, latMax: 31.0872, lngMin: -102.3905, lngMax: -100.9605 },
    'CROSBY': { latMin: 33.3948, latMax: 33.8336, lngMin: -101.5636, lngMax: -101.0388 },
    'CULBERSON': { latMin: 30.6636, latMax: 32.0005, lngMin: -104.9185, lngMax: -104.0245 },
    'DALLAM': { latMin: 36.0551, latMax: 36.5007, lngMin: -103.0419, lngMax: -102.1625 },
    'DALLAS': { latMin: 32.5452, latMax: 32.9897, lngMin: -97.0387, lngMax: -96.517 },
    'DAWSON': { latMin: 32.5233, latMax: 32.9618, lngMin: -102.2085, lngMax: -101.6887 },
    'DE WITT': { latMin: 28.8133, latMax: 29.3845, lngMin: -97.7551, lngMax: -96.9764 },
    'DEAF SMITH': { latMin: 34.746, latMax: 35.1872, lngMin: -103.0433, lngMax: -102.1675 },
    'DELTA': { latMin: 33.2186, latMax: 33.4954, lngMin: -95.8618, lngMax: -95.3066 },
    'DENTON': { latMin: 32.9874, latMax: 33.4305, lngMin: -97.3985, lngMax: -96.8341 },
    'DICKENS': { latMin: 33.3972, latMax: 33.8357, lngMin: -101.0412, lngMax: -100.5174 },
    'DIMMIT': { latMin: 28.1978, latMax: 28.6481, lngMin: -100.1143, lngMax: -99.3942 },
    'DONLEY': { latMin: 34.7477, latMax: 35.1833, lngMin: -101.0908, lngMax: -100.539 },
    'DUVAL': { latMin: 27.2625, latMax: 28.058, lngMin: -98.8033, lngMax: -98.2325 },
    'EASTLAND': { latMin: 32.078, latMax: 32.5156, lngMin: -99.1187, lngMax: -98.4747 },
    'ECTOR': { latMin: 31.6512, latMax: 32.0872, lngMin: -102.7991, lngMax: -102.287 },
    'EDWARDS': { latMin: 29.6233, latMax: 30.2907, lngMin: -100.7004, lngMax: -99.7541 },
    'EL PASO': { latMin: 31.3872, latMax: 32.0025, lngMin: -106.6456, lngMax: -105.9543 },
    'ELLIS': { latMin: 32.0521, latMax: 32.5493, lngMin: -97.0871, lngMax: -96.3829 },
    'ERATH': { latMin: 31.9175, latMax: 32.513, lngMin: -98.5512, lngMax: -97.8649 },
    'FALLS': { latMin: 30.986, latMax: 31.5223, lngMin: -97.2781, lngMax: -96.5969 },
    'FANNIN': { latMin: 33.3411, latMax: 33.885, lngMin: -96.3847, lngMax: -95.8464 },
    'FAYETTE': { latMin: 29.6281, latMax: 30.1643, lngMin: -97.3179, lngMax: -96.5698 },
    'FISHER': { latMin: 32.5228, latMax: 32.9635, lngMin: -100.6606, lngMax: -100.1442 },
    'FLOYD': { latMin: 33.8305, latMax: 34.3126, lngMin: -101.565, lngMax: -101.0412 },
    'FOARD': { latMin: 33.7338, latMax: 34.242, lngMin: -100.0485, lngMax: -99.4745 },
    'FORT BEND': { latMin: 29.2626, latMax: 29.7886, lngMin: -96.0889, lngMax: -95.4243 },
    'FRANKLIN': { latMin: 32.9618, latMax: 33.3895, lngMin: -95.309, lngMax: -95.1245 },
    'FREESTONE': { latMin: 31.4134, latMax: 32.0125, lngMin: -96.4967, lngMax: -95.7797 },
    'FRIO': { latMin: 28.6406, latMax: 29.0915, lngMin: -99.4139, lngMax: -98.8008 },
    'GAINES': { latMin: 32.5222, latMax: 32.9595, lngMin: -103.0649, lngMax: -102.2027 },
    'GALVESTON': { latMin: 29.0846, latMax: 29.5982, lngMin: -95.2331, lngMax: -94.3708 },
    'GARZA': { latMin: 32.961, latMax: 33.3972, lngMin: -101.5576, lngMax: -101.0384 },
    'GILLESPIE': { latMin: 30.1343, latMax: 30.4999, lngMin: -99.304, lngMax: -98.5879 },
    'GLASSCOCK': { latMin: 31.6509, latMax: 32.0875, lngMin: -101.7761, lngMax: -101.2642 },
    'GOLIAD': { latMin: 28.3887, latMax: 28.9253, lngMin: -97.7785, lngMax: -97.1536 },
    'GONZALES': { latMin: 29.1098, latMax: 29.7847, lngMin: -97.8588, lngMax: -97.1305 },
    'GRAY': { latMin: 35.1821, latMax: 35.6205, lngMin: -101.0864, lngMax: -100.5383 },
    'GRAYSON': { latMin: 33.3978, latMax: 33.9596, lngMin: -96.9447, lngMax: -96.3794 },
    'GREGG': { latMin: 32.36, latMax: 32.6675, lngMin: -94.987, lngMax: -94.5792 },
    'GRIMES': { latMin: 30.2252, latMax: 30.8641, lngMin: -96.1883, lngMax: -95.8043 },
    'GUADALUPE': { latMin: 29.3657, latMax: 29.8582, lngMin: -98.3121, lngMax: -97.631 },
    'HALE': { latMin: 33.8247, latMax: 34.3131, lngMin: -102.0904, lngMax: -101.5636 },
    'HALL': { latMin: 34.3128, latMax: 34.7484, lngMin: -100.9461, lngMax: -100.4158 },
    'HAMILTON': { latMin: 31.4164, latMax: 32.0179, lngMin: -98.4637, lngMax: -97.7664 },
    'HANSFORD': { latMin: 36.0552, latMax: 36.4995, lngMin: -101.6239, lngMax: -101.0848 },
    'HARDEMAN': { latMin: 34.0558, latMax: 34.5793, lngMin: -99.998, lngMax: -99.4751 },
    'HARDIN': { latMin: 30.0974, latMax: 30.5271, lngMin: -94.7327, lngMax: -94.071 },
    'HARRIS': { latMin: 29.4975, latMax: 30.1706, lngMin: -95.9607, lngMax: -94.9085 },
    'HARRISON': { latMin: 32.3266, latMax: 32.7931, lngMin: -94.7022, lngMax: -94.0428 },
    'HARTLEY': { latMin: 35.6225, latMax: 36.0556, lngMin: -103.0422, lngMax: -102.1627 },
    'HASKELL': { latMin: 32.957, latMax: 33.399, lngMin: -99.9913, lngMax: -99.4713 },
    'HAYS': { latMin: 29.7524, latMax: 30.3563, lngMin: -98.2976, lngMax: -97.7088 },
    'HEMPHILL': { latMin: 35.6191, latMax: 36.0565, lngMin: -100.5413, lngMax: -100.0004 },
    'HENDERSON': { latMin: 32.0059, latMax: 32.3589, lngMin: -96.4546, lngMax: -95.4281 },
    'HIDALGO': { latMin: 26.0364, latMax: 26.7831, lngMin: -98.5867, lngMax: -97.8617 },
    'HILL': { latMin: 31.7089, latMax: 32.2655, lngMin: -97.4971, lngMax: -96.7191 },
    'HOCKLEY': { latMin: 33.3883, latMax: 33.8251, lngMin: -102.6154, lngMax: -102.0759 },
    'HOOD': { latMin: 32.2335, latMax: 32.5588, lngMin: -98.0685, lngMax: -97.6154 },
    'HOPKINS': { latMin: 32.9604, latMax: 33.3772, lngMin: -95.8633, lngMax: -95.3077 },
    'HOUSTON': { latMin: 30.9259, latMax: 31.5929, lngMin: -95.7769, lngMax: -94.9581 },
    'HOWARD': { latMin: 32.0871, latMax: 32.5252, lngMin: -101.695, lngMax: -101.1746 },
    'HUDSPETH': { latMin: 30.6292, latMax: 32.0023, lngMin: -105.998, lngMax: -104.9074 },
    'HUNT': { latMin: 32.8372, latMax: 33.4095, lngMin: -96.2973, lngMax: -95.8587 },
    'HUTCHINSON': { latMin: 35.6241, latMax: 36.0554, lngMin: -101.6235, lngMax: -101.0857 },
    'IRION': { latMin: 31.0794, latMax: 31.5287, lngMin: -101.2748, lngMax: -100.6888 },
    'JACK': { latMin: 33.0013, latMax: 33.4671, lngMin: -98.4266, lngMax: -97.9181 },
    'JACKSON': { latMin: 28.6744, latMax: 29.2639, lngMin: -96.9386, lngMax: -96.3092 },
    'JASPER': { latMin: 30.2417, latMax: 31.1581, lngMin: -94.4602, lngMax: -93.8657 },
    'JEFF DAVIS': { latMin: 30.4122, latMax: 31.1052, lngMin: -104.98, lngMax: -103.44 },
    'JEFFERSON': { latMin: 29.5626, latMax: 30.1891, lngMin: -94.4451, lngMax: -93.8377 },
    'JIM HOGG': { latMin: 26.7835, latMax: 27.3591, lngMin: -98.9548, lngMax: -98.4179 },
    'JIM WELLS': { latMin: 27.261, latMax: 28.058, lngMin: -98.2355, lngMax: -97.7892 },
    'JOHNSON': { latMin: 32.1336, latMax: 32.5555, lngMin: -97.6171, lngMax: -97.0861 },
    'JONES': { latMin: 32.5147, latMax: 32.9603, lngMin: -100.1465, lngMax: -99.6118 },
    'KARNES': { latMin: 28.668, latMax: 29.2217, lngMin: -98.191, lngMax: -97.5746 },
    'KAUFMAN': { latMin: 32.3547, latMax: 32.8417, lngMin: -96.5294, lngMax: -96.0755 },
    'KENDALL': { latMin: 29.7165, latMax: 30.139, lngMin: -98.9206, lngMax: -98.414 },
    'KENEDY': { latMin: 26.5979, latMax: 27.2838, lngMin: -97.9859, lngMax: -97.2902 },
    'KENT': { latMin: 32.9629, latMax: 33.3991, lngMin: -101.0389, lngMax: -100.5171 },
    'KERR': { latMin: 29.7814, latMax: 30.2907, lngMin: -99.7576, lngMax: -98.9177 },
    'KIMBLE': { latMin: 30.2867, latMax: 30.711, lngMin: -100.1167, lngMax: -99.3017 },
    'KING': { latMin: 33.3972, latMax: 33.8361, lngMin: -100.5187, lngMax: -99.991 },
    'KINNEY': { latMin: 29.0843, latMax: 29.6239, lngMin: -100.797, lngMax: -100.1112 },
    'KLEBERG': { latMin: 27.2093, latMax: 27.6359, lngMin: -98.0598, lngMax: -97.2227 },
    'KNOX': { latMin: 33.3974, latMax: 33.836, lngMin: -99.9964, lngMax: -99.4724 },
    'LA SALLE': { latMin: 28.0304, latMax: 28.6475, lngMin: -99.3967, lngMax: -98.8001 },
    'LAMAR': { latMin: 33.3777, latMax: 33.943, lngMin: -95.8577, lngMax: -95.3081 },
    'LAMB': { latMin: 33.8247, latMax: 34.3132, lngMin: -102.6156, lngMax: -102.0857 },
    'LAMPASAS': { latMin: 31.0296, latMax: 31.4637, lngMin: -98.5696, lngMax: -97.9071 },
    'LAVACA': { latMin: 29.0632, latMax: 29.6327, lngMin: -97.2401, lngMax: -96.5606 },
    'LEE': { latMin: 30.032, latMax: 30.5572, lngMin: -97.3345, lngMax: -96.6409 },
    'LEON': { latMin: 30.9737, latMax: 31.654, lngMin: -96.3311, lngMax: -95.6555 },
    'LIBERTY': { latMin: 29.8843, latMax: 30.4936, lngMin: -95.1659, lngMax: -94.4422 },
    'LIMESTONE': { latMin: 31.2209, latMax: 31.8149, lngMin: -96.9322, lngMax: -96.2366 },
    'LIPSCOMB': { latMin: 36.0553, latMax: 36.4997, lngMin: -100.5467, lngMax: -100.0004 },
    'LIVE OAK': { latMin: 28.0569, latMax: 28.7869, lngMin: -98.335, lngMax: -97.8088 },
    'LLANO': { latMin: 30.4861, latMax: 30.9221, lngMin: -98.9646, lngMax: -98.351 },
    'LOVING': { latMin: 31.6506, latMax: 32.0006, lngMin: -103.9841, lngMax: -103.3265 },
    'LUBBOCK': { latMin: 33.3896, latMax: 33.8305, lngMin: -102.0857, lngMax: -101.5569 },
    'LYNN': { latMin: 32.9597, latMax: 33.3948, lngMin: -102.0762, lngMax: -101.5567 },
    'MADISON': { latMin: 30.823, latMax: 31.0942, lngMin: -96.241, lngMax: -95.6119 },
    'MARION': { latMin: 32.6871, latMax: 32.8811, lngMin: -94.7094, lngMax: -94.0427 },
    'MARTIN': { latMin: 32.0868, latMax: 32.5253, lngMin: -102.2112, lngMax: -101.6887 },
    'MASON': { latMin: 30.4981, latMax: 30.9412, lngMin: -99.4848, lngMax: -98.9638 },
    'MATAGORDA': { latMin: 28.3908, latMax: 29.2297, lngMin: -96.3783, lngMax: -95.5039 },
    'MAVERICK': { latMin: 28.1968, latMax: 29.0863, lngMin: -100.6675, lngMax: -100.1114 },
    'MCCULLOCH': { latMin: 30.9406, latMax: 31.4937, lngMin: -99.6037, lngMax: -99.0906 },
    'MCLENNAN': { latMin: 31.2442, latMax: 31.8631, lngMin: -97.6052, lngMax: -96.8011 },
    'MCMULLEN': { latMin: 28.0574, latMax: 28.6484, lngMin: -98.8033, lngMax: -98.3343 },
    'MEDINA': { latMin: 29.0902, latMax: 29.6907, lngMin: -99.4139, lngMax: -98.8046 },
    'MENARD': { latMin: 30.7104, latMax: 31.088, lngMin: -100.1162, lngMax: -99.4839 },
    'MIDLAND': { latMin: 31.6513, latMax: 32.087, lngMin: -102.2874, lngMax: -101.7758 },
    'MILAM': { latMin: 30.4573, latMax: 31.1104, lngMin: -97.3155, lngMax: -96.6121 },
    'MILLS': { latMin: 31.2304, latMax: 31.7236, lngMin: -98.995, lngMax: -98.2668 },
    'MITCHELL': { latMin: 32.0854, latMax: 32.5279, lngMin: -101.184, lngMax: -100.6606 },
    'MONTAGUE': { latMin: 33.4337, latMax: 33.9914, lngMin: -97.979, lngMax: -97.484 },
    'MONTGOMERY': { latMin: 30.0279, latMax: 30.6303, lngMin: -95.8302, lngMax: -95.0967 },
    'MOORE': { latMin: 35.6199, latMax: 36.0556, lngMin: -102.163, lngMax: -101.6227 },
    'MORRIS': { latMin: 32.8792, latMax: 33.3657, lngMin: -94.8198, lngMax: -94.6522 },
    'MOTLEY': { latMin: 33.8336, latMax: 34.3142, lngMin: -101.0416, lngMax: -100.5173 },
    'NACOGDOCHES': { latMin: 31.2228, latMax: 31.8456, lngMin: -94.9781, lngMax: -94.3001 },
    'NAVARRO': { latMin: 31.7962, latMax: 32.3289, lngMin: -96.8962, lngMax: -96.0508 },
    'NEWTON': { latMin: 30.2427, latMax: 31.1866, lngMin: -93.9111, lngMax: -93.5083 },
    'NOLAN': { latMin: 32.0814, latMax: 32.5253, lngMin: -100.6654, lngMax: -100.1465 },
    'NUECES': { latMin: 27.5584, latMax: 27.9957, lngMin: -97.9421, lngMax: -97.0448 },
    'OCHILTREE': { latMin: 36.0565, latMax: 36.4997, lngMin: -101.0857, lngMax: -100.546 },
    'OLDHAM': { latMin: 35.183, latMax: 35.6275, lngMin: -103.0428, lngMax: -102.1628 },
    'ORANGE': { latMin: 29.865, latMax: 30.2443, lngMin: -94.118, lngMax: -93.6886 },
    'PALO PINTO': { latMin: 32.5116, latMax: 33.0079, lngMin: -98.5763, lngMax: -98.0561 },
    'PANOLA': { latMin: 31.9732, latMax: 32.3941, lngMin: -94.6019, lngMax: -94.0154 },
    'PARKER': { latMin: 32.5553, latMax: 33.0033, lngMin: -98.0668, lngMax: -97.5442 },
    'PARMER': { latMin: 34.3123, latMax: 34.7476, lngMin: -103.044, lngMax: -102.5252 },
    'PECOS': { latMin: 30.0528, latMax: 31.3713, lngMin: -103.5851, lngMax: -101.7684 },
    'POLK': { latMin: 30.489, latMax: 31.1466, lngMin: -95.2002, lngMax: -94.5379 },
    'POTTER': { latMin: 35.1828, latMax: 35.6202, lngMin: -102.1675, lngMax: -101.6227 },
    'PRESIDIO': { latMin: 29.2586, latMax: 30.6292, lngMin: -104.9803, lngMax: -103.7927 },
    'RAINS': { latMin: 32.7116, latMax: 32.9802, lngMin: -95.9873, lngMax: -95.6349 },
    'RANDALL': { latMin: 34.7474, latMax: 35.1834, lngMin: -102.1688, lngMax: -101.6229 },
    'REAGAN': { latMin: 31.0793, latMax: 31.6515, lngMin: -101.7762, lngMax: -101.267 },
    'REAL': { latMin: 29.6235, latMax: 30.0824, lngMin: -100.0637, lngMax: -99.6026 },
    'RED RIVER': { latMin: 33.3175, latMax: 33.9626, lngMin: -95.311, lngMax: -94.7326 },
    'REEVES': { latMin: 30.7665, latMax: 32.0001, lngMin: -104.1024, lngMax: -103.0104 },
    'REFUGIO': { latMin: 28.0615, latMax: 28.5548, lngMin: -97.5411, lngMax: -96.7646 },
    'ROBERTS': { latMin: 35.6191, latMax: 36.0579, lngMin: -101.0861, lngMax: -100.5396 },
    'ROBERTSON': { latMin: 30.6957, latMax: 31.3571, lngMin: -96.8311, lngMax: -96.2351 },
    'ROCKWALL': { latMin: 32.813, latMax: 32.983, lngMin: -96.519, lngMax: -96.2972 },
    'RUNNELS': { latMin: 31.5767, latMax: 32.0826, lngMin: -100.2361, lngMax: -99.714 },
    'RUSK': { latMin: 31.8438, latMax: 32.4076, lngMin: -94.986, lngMax: -94.4525 },
    'SABINE': { latMin: 31.1342, latMax: 31.6118, lngMin: -94.0489, lngMax: -93.595 },
    'SAN AUGUSTINE': { latMin: 31.0993, latMax: 31.6536, lngMin: -94.3996, lngMax: -93.9832 },
    'SAN JACINTO': { latMin: 30.3195, latMax: 30.9067, lngMin: -95.3592, lngMax: -94.8297 },
    'SAN PATRICIO': { latMin: 27.8213, latMax: 28.1793, lngMin: -97.9088, lngMax: -97.1354 },
    'SAN SABA': { latMin: 30.9203, latMax: 31.4906, lngMin: -99.0924, lngMax: -98.4145 },
    'SCHLEICHER': { latMin: 30.7061, latMax: 31.0887, lngMin: -100.9622, lngMax: -100.1152 },
    'SCURRY': { latMin: 32.5252, latMax: 32.9702, lngMin: -101.1747, lngMax: -100.6559 },
    'SHACKELFORD': { latMin: 32.5147, latMax: 32.9571, lngMin: -99.6122, lngMax: -99.0958 },
    'SHELBY': { latMin: 31.5694, latMax: 31.9802, lngMin: -94.5115, lngMax: -93.7945 },
    'SHERMAN': { latMin: 36.0552, latMax: 36.5007, lngMin: -102.1632, lngMax: -101.6235 },
    'SMITH': { latMin: 32.1354, latMax: 32.687, lngMin: -95.5945, lngMax: -94.9853 },
    'SOMERVELL': { latMin: 32.0873, latMax: 32.3186, lngMin: -97.9456, lngMax: -97.6143 },
    'STARR': { latMin: 26.2352, latMax: 26.7857, lngMin: -99.1717, lngMax: -98.3207 },
    'STEPHENS': { latMin: 32.5147, latMax: 32.9572, lngMin: -99.0961, lngMax: -98.5756 },
    'STERLING': { latMin: 31.5565, latMax: 32.0872, lngMin: -101.2676, lngMax: -100.8216 },
    'STONEWALL': { latMin: 32.9598, latMax: 33.398, lngMin: -100.5192, lngMax: -99.9888 },
    'SUTTON': { latMin: 30.2878, latMax: 30.7104, lngMin: -100.9607, lngMax: -100.1162 },
    'SWISHER': { latMin: 34.3123, latMax: 34.7482, lngMin: -101.9985, lngMax: -101.4714 },
    'TARRANT': { latMin: 32.5487, latMax: 32.9942, lngMin: -97.5506, lngMax: -97.031 },
    'TAYLOR': { latMin: 32.0813, latMax: 32.523, lngMin: -100.1519, lngMax: -99.6296 },
    'TERRELL': { latMin: 29.7791, latMax: 30.6583, lngMin: -102.567, lngMax: -101.6462 },
    'TERRY': { latMin: 32.9586, latMax: 33.3896, lngMin: -102.595, lngMax: -102.0754 },
    'THROCKMORTON': { latMin: 32.9564, latMax: 33.399, lngMin: -99.4724, lngMax: -98.9509 },
    'TITUS': { latMin: 32.9825, latMax: 33.3985, lngMin: -95.1261, lngMax: -94.8088 },
    'TOM GREEN': { latMin: 31.0862, latMax: 31.7054, lngMin: -101.2679, lngMax: -100.1112 },
    'TRAVIS': { latMin: 30.0235, latMax: 30.6282, lngMin: -98.173, lngMax: -97.3695 },
    'TRINITY': { latMin: 30.8246, latMax: 31.3869, lngMin: -95.4348, lngMax: -94.8429 },
    'TYLER': { latMin: 30.526, latMax: 31.0591, lngMin: -94.658, lngMax: -94.051 },
    'UPSHUR': { latMin: 32.5165, latMax: 32.9045, lngMin: -95.1534, lngMax: -94.6724 },
    'UPTON': { latMin: 31.0798, latMax: 31.6516, lngMin: -102.318, lngMax: -101.7752 },
    'UVALDE': { latMin: 29.0863, latMax: 29.6277, lngMin: -100.1123, lngMax: -99.4118 },
    'VAL VERDE': { latMin: 29.2376, latMax: 30.2885, lngMin: -101.7609, lngMax: -100.6991 },
    'VAN ZANDT': { latMin: 32.3551, latMax: 32.8385, lngMin: -96.0768, lngMax: -95.4492 },
    'VICTORIA': { latMin: 28.4858, latMax: 29.104, lngMin: -97.3059, lngMax: -96.6423 },
    'WALKER': { latMin: 30.5044, latMax: 31.0581, lngMin: -95.8631, lngMax: -95.3275 },
    'WALLER': { latMin: 29.7279, latMax: 30.2456, lngMin: -96.1922, lngMax: -95.8033 },
    'WARD': { latMin: 31.2669, latMax: 31.6518, lngMin: -103.6109, lngMax: -102.7672 },
    'WASHINGTON': { latMin: 30.0443, latMax: 30.3999, lngMin: -96.7946, lngMax: -96.0808 },
    'WEBB': { latMin: 27.2594, latMax: 28.2046, lngMin: -100.2122, lngMax: -98.7981 },
    'WHARTON': { latMin: 28.9633, latMax: 29.6338, lngMin: -96.6403, lngMax: -95.8419 },
    'WHEELER': { latMin: 35.1827, latMax: 35.6197, lngMin: -100.5402, lngMax: -100.0004 },
    'WICHITA': { latMin: 33.834, latMax: 34.2126, lngMin: -98.9532, lngMax: -98.423 },
    'WILBARGER': { latMin: 33.8339, latMax: 34.4588, lngMin: -99.4757, lngMax: -98.9524 },
    'WILLACY': { latMin: 26.2993, latMax: 26.6118, lngMin: -98.0042, lngMax: -97.2249 },
    'WILLIAMSON': { latMin: 30.4028, latMax: 30.9044, lngMin: -98.0499, lngMax: -97.1552 },
    'WILSON': { latMin: 28.8823, latMax: 29.4418, lngMin: -98.4073, lngMax: -97.7284 },
    'WINKLER': { latMin: 31.6513, latMax: 32.0872, lngMin: -103.3275, lngMax: -102.7976 },
    'WISE': { latMin: 32.9908, latMax: 33.434, lngMin: -97.9216, lngMax: -97.383 },
    'WOOD': { latMin: 32.541, latMax: 33.0134, lngMin: -95.6654, lngMax: -95.15 },
    'YOAKUM': { latMin: 32.9585, latMax: 33.3885, lngMin: -103.0647, lngMax: -102.5942 },
    'YOUNG': { latMin: 32.9523, latMax: 33.3975, lngMin: -98.9539, lngMax: -98.4204 },
    'ZAPATA': { latMin: 26.5717, latMax: 27.3192, lngMin: -99.4538, lngMax: -98.9542 },
    'ZAVALA': { latMin: 28.6406, latMax: 29.0913, lngMin: -100.1143, lngMax: -99.409 },
  };

  // generateCoordinates removed — wells without real coordinates should not appear on the map.
  // Only wells with real lat/lng from RRC or Supabase will be shown.
  function generateCoordinates(county) {
    // Return null — do not generate fake coordinates
    return null;
  }

  // ============================================
  // Map: Fetch wells from Supabase for map (TASK 5)
  // ============================================
  async function fetchWellsForMap(filters) {
    const hasFilters = (filters && (filters.county || filters.operator || filters.status));
    const baseParams = ['select=id,api,operator,county,well_type,total_depth,status,offshore,lease_name,field_name,well_number,district,lat,lng'];

    // If no filters, only show wells with real coordinates (can't load 1.3M)
    // If filters applied, show ALL matching wells with generated coords as fallback
    if (!hasFilters) {
      baseParams.push('lat=not.is.null');
      baseParams.push('lng=not.is.null');
    }

    if (filters && filters.county) baseParams.push('county=eq.' + encodeURIComponent(filters.county));
    if (filters && filters.operator) baseParams.push('operator=ilike.' + encodeURIComponent('%' + filters.operator + '%'));
    if (filters && filters.status) baseParams.push('status=eq.' + encodeURIComponent(filters.status));

    baseParams.push('order=id.asc');

    // Cap filtered results at 500, unfiltered loads all coords via pagination
    if (hasFilters) baseParams.push('limit=500');

    const fetchHeaders = {
      'apikey': SUPABASE_ANON_KEY,
      'Authorization': 'Bearer ' + SUPABASE_ANON_KEY,
    };
    let allWells = [];

    try {
      if (!supabaseAvailable) await initSupabase();
      if (!supabaseAvailable) return [];

      const url = SUPABASE_URL + '/rest/v1/wells?' + baseParams.join('&');

      if (hasFilters) {
        // Single request for filtered results (capped at 500)
        const resp = await fetch(url, { headers: fetchHeaders });
        if (!resp.ok) return [];
        const data = await resp.json();
        // Only include wells with real coordinates — do not generate fake positions
        allWells = (data || []).filter(w => w.lat && w.lng);
      } else {
        // Paginate all wells with real coordinates
        let page = 0;
        while (true) {
          const start = page * 1000;
          const resp = await fetch(url, { headers: { ...fetchHeaders, 'Range': start + '-' + (start + 999) } });
          if (!resp.ok && resp.status !== 206) break;
          const data = await resp.json();
          allWells = allWells.concat((data || []).filter(w => w.lat && w.lng));
          if (page % 10 === 0) console.log('[MineralSearch] Map loaded ' + allWells.length + ' wells...');
          if (!data || data.length < 1000) break;
          page++;
        }
      }

      console.log('[MineralSearch] Map complete: ' + allWells.length + ' wells');
    } catch (e) {
      console.log('[MineralSearch] Map Supabase fetch failed:', e.message);
    }

    return allWells;
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
      center: [31.5, -99.5],
      zoom: 6,
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

    // Map county search-as-you-type
    function populateMapCountyFilter(wells) {
      const input = document.getElementById('map-county');
      const hidden = document.getElementById('map-county-value');
      const dropdown = document.getElementById('map-county-dropdown');
      if (!input || !dropdown) return;
      const mapCounties = [...new Set((wells || []).map(w => w.county).filter(Boolean))].sort();

      input.addEventListener('input', function() {
        const term = input.value.trim().toUpperCase();
        if (hidden) hidden.value = '';
        if (term.length < 1) { dropdown.style.display = 'none'; return; }
        const matches = mapCounties.filter(c => c.includes(term)).slice(0, 15);
        if (!matches.length) { dropdown.style.display = 'none'; return; }
        dropdown.innerHTML = matches.map(c =>
          '<div style="padding:8px 12px;cursor:pointer;font-size:0.85rem;color:var(--text);border-bottom:1px solid var(--border);" onmousedown="event.preventDefault()" onclick="document.getElementById(\'map-county\').value=this.textContent;document.getElementById(\'map-county-value\').value=this.textContent;document.getElementById(\'map-county-dropdown\').style.display=\'none\';if(window.applyMapFilters)window.applyMapFilters();">' + c + '</div>'
        ).join('');
        dropdown.style.display = 'block';
      });

      input.addEventListener('blur', () => { setTimeout(() => { dropdown.style.display = 'none'; }, 200); });
      input.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') { input.value = ''; if (hidden) hidden.value = ''; dropdown.style.display = 'none'; if(window.applyMapFilters) window.applyMapFilters(); }
        if (e.key === 'Enter') { dropdown.style.display = 'none'; if(window.applyMapFilters) window.applyMapFilters(); }
      });
    }

    // Wire up map filters
    window.applyMapFilters = function() {
      // Auto-resolve typed text to exact county/operator match
      const countyInput = document.getElementById('map-county');
      const countyHidden = document.getElementById('map-county-value');
      if (countyInput && countyHidden && !countyHidden.value && countyInput.value.trim()) {
        const match = TX_COUNTIES.find(c => c === countyInput.value.trim().toUpperCase());
        if (match) { countyHidden.value = match; countyInput.value = match; }
      }
      const opInput = document.getElementById('map-operator');
      const opHidden = document.getElementById('map-operator-value');
      if (opInput && opHidden && !opHidden.value && opInput.value.trim()) {
        opHidden.value = opInput.value.trim().toUpperCase();
      }

      const county = (countyHidden || countyInput || {}).value || '';
      const operator = (opHidden || opInput || {}).value || '';
      const status = (document.getElementById('map-status') || {}).value || '';

      loadAndPlotWells({ county, operator, status }).then(() => {
        if (county && COUNTY_BOUNDS[county]) {
          const b = COUNTY_BOUNDS[county];
          map.fitBounds([[b.latMin, b.lngMin], [b.latMax, b.lngMax]], { padding: [30, 30] });
        }
      });
    };

    window.resetMapFilters = function() {
      ['map-county','map-county-value','map-operator','map-operator-value','map-status','map-search'].forEach(id => {
        const el = document.getElementById(id); if (el) el.value = '';
      });
      loadAndPlotWells({});
      map.setView([31.5, -99.5], 6);
    };

    // Layer toggle events
    ['layer-wells', 'layer-permits', 'layer-leases'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('change', () => { if (window.applyMapFilters) window.applyMapFilters(); });
    });

    const initialWells = await loadAndPlotWells({});
    populateMapCountyFilter(initialWells);

    // Operator search-as-you-type for map
    (function() {
      const input = document.getElementById('map-operator');
      const hidden = document.getElementById('map-operator-value');
      const dropdown = document.getElementById('map-operator-dropdown');
      if (!input || !dropdown) return;
      let t;
      input.addEventListener('input', function() {
        const term = input.value.trim();
        if (hidden) hidden.value = '';
        clearTimeout(t);
        if (term.length < 2) { dropdown.style.display = 'none'; return; }
        t = setTimeout(async () => {
          try {
            const encoded = encodeURIComponent('%' + term + '%');
            const resp = await fetch(SUPABASE_URL + '/rest/v1/operators?select=operator_name&operator_name=ilike.' + encoded + '&order=operator_name&limit=20', {
              headers: { 'apikey': SUPABASE_ANON_KEY, 'Authorization': 'Bearer ' + SUPABASE_ANON_KEY }
            });
            const data = await resp.json();
            if (!data || !data.length) { dropdown.style.display = 'none'; return; }
            dropdown.innerHTML = data.map(r =>
              '<div style="padding:8px 12px;cursor:pointer;font-size:0.85rem;color:var(--text);border-bottom:1px solid var(--border);" onmousedown="event.preventDefault()" onclick="document.getElementById(\'map-operator\').value=this.textContent;document.getElementById(\'map-operator-value\').value=this.textContent;document.getElementById(\'map-operator-dropdown\').style.display=\'none\';if(window.applyMapFilters)window.applyMapFilters();">' + (r.operator_name || '') + '</div>'
            ).join('');
            dropdown.style.display = 'block';
          } catch(e) { dropdown.style.display = 'none'; }
        }, 300);
      });
      input.addEventListener('blur', () => { setTimeout(() => { dropdown.style.display = 'none'; }, 200); });
      input.addEventListener('keydown', function(e) {
        if (e.key === 'Escape') { input.value = ''; if (hidden) hidden.value = ''; dropdown.style.display = 'none'; if(window.applyMapFilters) window.applyMapFilters(); }
        if (e.key === 'Enter') { dropdown.style.display = 'none'; if(window.applyMapFilters) window.applyMapFilters(); }
      });
    })();
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
