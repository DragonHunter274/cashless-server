<script>
import Router, { location } from 'svelte-spa-router';
import { authState } from '../stores/auth.js';
import { clearKey } from '../lib/api.js';
import { theme, toggleTheme } from '../stores/theme.js';

export let checkAuth;

// Import user pages (we'll create these in Phase 5)
import Overview from '../pages/user/Overview.svelte';
import Transactions from '../pages/user/Transactions.svelte';
import Vouchers from '../pages/user/Vouchers.svelte';
import Privileges from '../pages/user/Privileges.svelte';

const routes = {
    '/': Overview,
    '/overview': Overview,
    '/transactions': Transactions,
    '/vouchers': Vouchers,
    '/privileges': Privileges,
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

    // Re-check auth (will show login view)
    checkAuth();
}

function toggleSidebar() {
    sidebarOpen = !sidebarOpen;
}

function closeSidebar() {
    sidebarOpen = false;
}

// Check if a route is active
function isActive(path) {
    if (path === '/overview') {
        return $location === '/' || $location === '/overview';
    }
    return $location === path;
}
</script>

<div id="userView" class="user-view" style="width: 100%; min-height: 100vh; display: flex;">
    <!-- Overlay for mobile sidebar -->
    {#if sidebarOpen}
        <div class="sidebar-overlay" on:click={closeSidebar} on:keydown={(e) => e.key === 'Escape' && closeSidebar()} role="button" tabindex="-1" aria-label="Close sidebar"></div>
    {/if}

    <aside class="sidebar" class:open={sidebarOpen} id="userSidebar">
        <div class="sidebar-header">
            <h1>Cashless Server</h1>
            <div class="subtitle">My Account</div>
        </div>
        <nav>
            <a class="nav-item" class:active={isActive('/overview')} href="#/overview" on:click={closeSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>
                My Overview
            </a>
            <a class="nav-item" class:active={isActive('/transactions')} href="#/transactions" on:click={closeSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="12" y1="1" x2="12" y2="23"/><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 0 1 0 7H6"/></svg>
                My Transactions
            </a>
            <a class="nav-item" class:active={isActive('/vouchers')} href="#/vouchers" on:click={closeSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="2" y="7" width="20" height="15" rx="2" ry="2"/><polyline points="17 2 12 7 7 2"/></svg>
                My Vouchers
            </a>
            <a class="nav-item" class:active={isActive('/privileges')} href="#/privileges" on:click={closeSidebar}>
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>
                My Privileges
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
            <span style="color: var(--text); font-size: 14px; margin-right: 12px;">{$authState?.username || 'User'}</span>
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
