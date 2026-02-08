// Main application logic, API, navigation, and OIDC authentication
// Dependencies: utils.js, pages.js

const API = window.location.origin;
let currentPage = 'dashboard';

// Global state
window.pendingTxs = [];
window.usersOffset = 0;
window.txOffset = 0;
window.oidcUser = null; // {authenticated, email, name, role}

// Get API key from input
function getKey() {
    return document.getElementById('apiKeyInput').value;
}

// API request helper
async function api(endpoint, body = null) {
    const opts = {
        method: endpoint.startsWith('/auth/') ? 'GET' : 'POST',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'same-origin'
    };
    const apiKey = getKey();
    if (apiKey) {
        opts.headers['X-API-Key'] = apiKey;
    }
    if (body !== null) opts.body = JSON.stringify(body);
    const resp = await fetch(API + endpoint, opts);
    const text = await resp.text();
    if (!resp.ok) throw new Error(text || `HTTP ${resp.status}`);
    try { return JSON.parse(text); } catch { return text; }
}

// Check OIDC authentication status
async function checkOIDCAuth() {
    try {
        const resp = await fetch(API + '/auth/me', { credentials: 'same-origin' });
        if (resp.ok) {
            window.oidcUser = await resp.json();
            document.getElementById('userName').textContent = window.oidcUser.name || window.oidcUser.email;
            document.getElementById('userInfo').style.display = 'flex';
            document.getElementById('ssoLoginBtn').style.display = 'none';
            loadPageData(currentPage);
        } else {
            window.oidcUser = null;
            document.getElementById('ssoLoginBtn').style.display = 'block';
            document.getElementById('userInfo').style.display = 'none';
        }
    } catch (e) {
        // OIDC not available or error - hide SSO UI
        document.getElementById('ssoLoginBtn').style.display = 'none';
        document.getElementById('userInfo').style.display = 'none';
    }
}

// Logout from OIDC
async function logoutOIDC() {
    try {
        await fetch(API + '/auth/logout', { method: 'POST', credentials: 'same-origin' });
        window.oidcUser = null;
        document.getElementById('ssoLoginBtn').style.display = 'block';
        document.getElementById('userInfo').style.display = 'none';
        toast('Logged out successfully');
        navigate('dashboard');
    } catch (e) {
        toast('Logout failed: ' + e.message, 'error');
    }
}

// Navigation
function navigate(page) {
    currentPage = page;
    document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
    document.getElementById('page-' + page).classList.add('active');
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n => {
        if (n.getAttribute('onclick') === `navigate('${page}')`) n.classList.add('active');
    });
    document.getElementById('sidebar').classList.remove('open');
    window.location.hash = page;
    loadPageData(page);
}

function loadPageData(page) {
    // Require either API key or OIDC session
    if (!getKey() && !window.oidcUser) return;
    switch (page) {
        case 'dashboard': loadDashboard(); break;
        case 'users': loadUsers(); break;
        case 'vouchers': loadVouchers(); break;
        case 'privileges': loadPrivileges(); break;
        case 'transactions': loadTransactions(); break;
        case 'products': loadProducts(); break;
        case 'apikeys': loadAPIKeys(); break;
    }
}

// Initialization
document.addEventListener('DOMContentLoaded', () => {
    // API key input event listeners
    document.getElementById('apiKeyInput').addEventListener('change', () => loadPageData(currentPage));
    document.getElementById('apiKeyInput').addEventListener('keyup', (e) => {
        if (e.key === 'Enter') loadPageData(currentPage);
    });

    // Check OIDC authentication on page load
    checkOIDCAuth();

    // Hash navigation
    const hash = window.location.hash.slice(1);
    if (hash && document.getElementById('page-' + hash)) {
        navigate(hash);
    }

    renderPending();
});
