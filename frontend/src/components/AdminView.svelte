<script>
import Router, { location } from 'svelte-spa-router';
import { authState } from '../stores/auth.js';
import { clearKey, api } from '../lib/api.js';
import { pendingTxs } from '../stores/pending.js';
import { theme, toggleTheme } from '../stores/theme.js';

export let checkAuth;

// Import admin pages (we'll create these in Phase 4)
import Dashboard from '../pages/admin/Dashboard.svelte';
import Users from '../pages/admin/Users.svelte';
import Purchases from '../pages/admin/Purchases.svelte';
import Vouchers from '../pages/admin/Vouchers.svelte';
import Privileges from '../pages/admin/Privileges.svelte';
import Transactions from '../pages/admin/Transactions.svelte';
import Products from '../pages/admin/Products.svelte';
import APIKeys from '../pages/admin/APIKeys.svelte';
import Firmware from '../pages/admin/Firmware.svelte';

const routes = {
    '/': Dashboard,
    '/dashboard': Dashboard,
    '/users': Users,
    '/purchases': Purchases,
    '/vouchers': Vouchers,
    '/privileges': Privileges,
    '/transactions': Transactions,
    '/products': Products,
    '/apikeys': APIKeys,
    '/firmware': Firmware,
};

let sidebarOpen = false;

async function handleLogout() {
    // If OIDC, call logout endpoint
    if ($authState && $authState.method === 'oidc') {
        try {
            await fetch(window.location.origin + '/auth/logout', { method: 'POST', credentials: 'same-origin' });
        } catch (e) {
            // Ignore errors
        }
    }

    // Clear API key
    clearKey();

    // Reset state
    authState.set(null);
    pendingTxs.set([]);

    // Re-check auth (will show login view)
    checkAuth();
}

function toggleSidebar() {
    sidebarOpen = !sidebarOpen;
}

// Check if a route is active
function isActive(path) {
    if (path === '/dashboard') {
        return $location === '/' || $location === '/dashboard';
    }
    return $location === path;
}
</script>

<div id="adminView" class="admin-view" style="width: 100%; min-height: 100vh; display: flex;">
    <!-- Overlay for mobile sidebar -->
    {#if sidebarOpen}
        <div class="sidebar-overlay" on:click={toggleSidebar} on:keydown={(e) => e.key === 'Escape' && toggleSidebar()} role="button" tabindex="-1" aria-label="Close sidebar"></div>
    {/if}

    <aside class="sidebar" class:open={sidebarOpen} id="sidebar">
        <div class="sidebar-header">
            <h1>Cashless Server</h1>
            <div class="subtitle">Admin Panel</div>
        </div>
        <nav>
            <div class="nav-section">Overview</div>
            <a class="nav-item" class:active={isActive('/dashboard')} href="#/dashboard" on:click={toggleSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <rect x="3" y="3" width="7" height="7"/><rect x="14" y="3" width="7" height="7"/><rect x="14" y="14" width="7" height="7"/><rect x="3" y="14" width="7" height="7"/>
                </svg>
                Dashboard
            </a>

            <div class="nav-section">Management</div>
            <a class="nav-item" class:active={isActive('/users')} href="#/users" on:click={toggleSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87m-4-12a4 4 0 0 1 0 7.75"/></svg>
                Users
            </a>
            <a class="nav-item" class:active={isActive('/purchases')} href="#/purchases" on:click={toggleSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="9" cy="21" r="1"/><circle cx="20" cy="21" r="1"/><path d="M1 1h4l2.68 13.39a2 2 0 0 0 2 1.61h9.72a2 2 0 0 0 2-1.61L23 6H6"/></svg>
                Purchases
            </a>
            <a class="nav-item" class:active={isActive('/vouchers')} href="#/vouchers" on:click={toggleSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="7" width="20" height="15" rx="2" ry="2"/><polyline points="17 2 12 7 7 2"/></svg>
                Vouchers
            </a>
            <a class="nav-item" class:active={isActive('/privileges')} href="#/privileges" on:click={toggleSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
                Privileges
            </a>
            <a class="nav-item" class:active={isActive('/transactions')} href="#/transactions" on:click={toggleSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>
                Transactions
            </a>
            <a class="nav-item" class:active={isActive('/products')} href="#/products" on:click={toggleSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polyline points="20 6 9 17 4 12"/></svg>
                Products
            </a>

            <div class="nav-section">Settings</div>
            <a class="nav-item" class:active={isActive('/apikeys')} href="#/apikeys" on:click={toggleSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"/></svg>
                API Keys
            </a>
            <a class="nav-item" class:active={isActive('/firmware')} href="#/firmware" on:click={toggleSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="4" y="4" width="16" height="16" rx="2"/><rect x="9" y="9" width="6" height="6"/><line x1="9" y1="1" x2="9" y2="4"/><line x1="15" y1="1" x2="15" y2="4"/><line x1="9" y1="20" x2="9" y2="23"/><line x1="15" y1="20" x2="15" y2="23"/><line x1="1" y1="9" x2="4" y2="9"/><line x1="1" y1="15" x2="4" y2="15"/><line x1="20" y1="9" x2="23" y2="9"/><line x1="20" y1="15" x2="23" y2="15"/></svg>
                Firmware
            </a>
        </nav>
    </aside>

    <div class="main">
        <div class="topbar">
            <button class="hamburger" on:click={toggleSidebar} aria-label="Toggle menu">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 24px; height: 24px;"><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="18" x2="21" y2="18"/></svg>
            </button>
            <div style="flex: 1;"></div>
            <button class="btn btn-sm btn-secondary" on:click={toggleTheme} aria-label="Toggle dark mode" style="margin-right: 12px;">
                {#if $theme === 'dark'}
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 16px; height: 16px;">
                        <circle cx="12" cy="12" r="5"/>
                        <line x1="12" y1="1" x2="12" y2="3"/>
                        <line x1="12" y1="21" x2="12" y2="23"/>
                        <line x1="4.22" y1="4.22" x2="5.64" y2="5.64"/>
                        <line x1="18.36" y1="18.36" x2="19.78" y2="19.78"/>
                        <line x1="1" y1="12" x2="3" y2="12"/>
                        <line x1="21" y1="12" x2="23" y2="12"/>
                        <line x1="4.22" y1="19.78" x2="5.64" y2="18.36"/>
                        <line x1="18.36" y1="5.64" x2="19.78" y2="4.22"/>
                    </svg>
                {:else}
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" style="width: 16px; height: 16px;">
                        <path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"/>
                    </svg>
                {/if}
            </button>
            <span style="color: var(--text); font-size: 14px; margin-right: 12px;">{$authState?.username || 'Admin'}</span>
            <button class="btn btn-sm btn-secondary" on:click={handleLogout}>Logout</button>
        </div>

        <div class="content">
            <Router {routes} />
        </div>
    </div>
</div>

<style>
/* Styles are inherited from styles.css */
</style>
