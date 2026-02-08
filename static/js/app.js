// Main application logic, API, navigation, and authentication
// Dependencies: utils.js, pages.js

const API = window.location.origin;
let currentPage = 'dashboard';
let currentUserPage = 'overview';

// Global state
window.pendingTxs = [];
window.usersOffset = 0;
window.txOffset = 0;
window.authState = null; // { isAuthenticated, isAdmin, username, email, method }

// Get API key from localStorage
function getKey() {
    return localStorage.getItem('apiKey') || '';
}

// Set API key in localStorage
function setKey(key) {
    localStorage.setItem('apiKey', key);
}

// Clear API key from localStorage
function clearKey() {
    localStorage.removeItem('apiKey');
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

// Check authentication status and determine view to show
async function checkAuth() {
    // Check if API key is set (assume admin)
    const apiKey = getKey();
    if (apiKey) {
        // Verify API key works by making a test request
        try {
            await api('/getStats', {});
            window.authState = {
                isAuthenticated: true,
                isAdmin: true,
                username: 'Admin',
                email: null,
                method: 'apikey'
            };
            showView('admin');
            return;
        } catch (e) {
            // Invalid API key, clear it
            clearKey();
        }
    }

    // Check OIDC authentication
    try {
        const resp = await fetch(API + '/auth/me', { credentials: 'same-origin' });
        if (resp.ok) {
            const oidcUser = await resp.json();
            window.authState = {
                isAuthenticated: true,
                isAdmin: oidcUser.role === 'admin',
                username: oidcUser.name || oidcUser.email,
                email: oidcUser.email,
                method: 'oidc'
            };
            showView(window.authState.isAdmin ? 'admin' : 'user');
            return;
        } else {
            // Not authenticated but OIDC is available - show SSO button on login page
            document.getElementById('ssoLoginBtnPage').style.display = 'block';
        }
    } catch (e) {
        // OIDC not available - hide SSO button
        document.getElementById('ssoLoginBtnPage').style.display = 'none';
    }

    // Not authenticated, show login page
    window.authState = null;
    showView('login');
}

// Show specific view (login, user, or admin)
function showView(view) {
    document.getElementById('loginView').style.display = view === 'login' ? 'flex' : 'none';
    document.getElementById('userView').style.display = view === 'user' ? 'flex' : 'none';
    document.getElementById('adminView').style.display = view === 'admin' ? 'flex' : 'none';

    if (view === 'user') {
        // Set user name in topbar
        document.getElementById('userViewName').textContent = window.authState.username;
        // Load user data
        loadUserData();
    } else if (view === 'admin') {
        // Set admin name in topbar
        document.getElementById('adminUserName').textContent = window.authState.username;
        // Load admin dashboard
        loadPageData(currentPage);
    }
}

// Login with API key
function loginWithAPIKey() {
    const apiKey = document.getElementById('apiKeyLoginInput').value.trim();
    if (!apiKey) {
        toast('Please enter an API key', 'error');
        return;
    }
    setKey(apiKey);
    checkAuth();
}

// Logout
async function logout() {
    // If OIDC, call logout endpoint
    if (window.authState && window.authState.method === 'oidc') {
        try {
            await fetch(API + '/auth/logout', { method: 'POST', credentials: 'same-origin' });
        } catch (e) {
            // Ignore errors
        }
    }

    // Clear API key
    clearKey();

    // Reset state
    window.authState = null;
    window.pendingTxs = [];

    // Show login page
    showView('login');
    toast('Logged out successfully');
}

// Admin navigation
function navigate(page) {
    currentPage = page;
    document.querySelectorAll('#adminView .page').forEach(p => p.classList.remove('active'));
    document.getElementById('page-' + page).classList.add('active');
    document.querySelectorAll('#sidebar .nav-item').forEach(n => n.classList.remove('active'));
    document.querySelectorAll('#sidebar .nav-item').forEach(n => {
        if (n.getAttribute('onclick') === `navigate('${page}')`) n.classList.add('active');
    });
    document.getElementById('sidebar').classList.remove('open');
    window.location.hash = page;
    loadPageData(page);
}

// User navigation
function navigateUser(page) {
    currentUserPage = page;
    document.querySelectorAll('#userView .page').forEach(p => p.classList.remove('active'));
    document.getElementById('user-page-' + page).classList.add('active');
    document.querySelectorAll('#userSidebar .nav-item').forEach(n => n.classList.remove('active'));
    document.querySelectorAll('#userSidebar .nav-item').forEach(n => {
        if (n.getAttribute('onclick') === `navigateUser('${page}')`) n.classList.add('active');
    });
    document.getElementById('userSidebar').classList.remove('open');
    window.location.hash = 'user-' + page;
    loadUserPageData(page);
}

// Load admin page data
function loadPageData(page) {
    if (!window.authState || !window.authState.isAdmin) return;
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

// Load user page data
function loadUserPageData(page) {
    if (!window.authState || window.authState.isAdmin) return;
    switch (page) {
        case 'overview': loadUserOverview(); break;
        case 'transactions': loadUserTransactions(); break;
        case 'vouchers': loadUserVouchers(); break;
        case 'privileges': loadUserPrivileges(); break;
    }
}

// Load all user data
function loadUserData() {
    loadUserPageData(currentUserPage);
}

// Toggle sidebar
function toggleSidebar(sidebarId = 'sidebar') {
    document.getElementById(sidebarId).classList.toggle('open');
}

// Initialization
document.addEventListener('DOMContentLoaded', () => {
    // Check authentication on page load
    checkAuth();

    // Enable Enter key on API key login
    document.getElementById('apiKeyLoginInput').addEventListener('keyup', (e) => {
        if (e.key === 'Enter') loginWithAPIKey();
    });

    // Hash navigation
    const hash = window.location.hash.slice(1);
    if (hash && hash.startsWith('user-')) {
        currentUserPage = hash.substring(5);
    } else if (hash && document.getElementById('page-' + hash)) {
        currentPage = hash;
    }

    renderPending();
});
