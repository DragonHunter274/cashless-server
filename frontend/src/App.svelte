<script>
import { onMount } from 'svelte';
import { authState, isAdmin, isUser } from './stores/auth.js';
import { api, getKey, clearKey } from './lib/api.js';
import Toast from './components/Toast.svelte';
import LoginView from './components/LoginView.svelte';
import AdminView from './components/AdminView.svelte';
import UserView from './components/UserView.svelte';

let currentView = 'login'; // 'login', 'admin', or 'user'
let oidcAvailable = false;
let checkingAuth = true;

// Check authentication status and determine view to show
async function checkAuth() {
    checkingAuth = true;

    // Check if API key is set (assume admin)
    const apiKey = getKey();
    if (apiKey) {
        // Verify API key works by making a test request
        try {
            await api('/getStats', {});
            authState.set({
                isAuthenticated: true,
                isAdmin: true,
                username: 'Admin',
                email: null,
                method: 'apikey'
            });
            currentView = 'admin';
            checkingAuth = false;
            return;
        } catch (e) {
            // Invalid API key, clear it
            clearKey();
        }
    }

    // Check OIDC authentication
    try {
        const resp = await fetch(window.location.origin + '/auth/me', { credentials: 'same-origin' });
        if (resp.ok) {
            const oidcUser = await resp.json();
            const isAdminOrSuper = oidcUser.role === 'admin' || oidcUser.role === 'superadmin';
            authState.set({
                isAuthenticated: true,
                isAdmin: isAdminOrSuper,
                isSuperAdmin: oidcUser.is_superadmin || false,
                username: oidcUser.name || oidcUser.email,
                email: oidcUser.email,
                method: 'oidc'
            });
            currentView = isAdminOrSuper ? 'admin' : 'user';
            checkingAuth = false;
            return;
        } else {
            // Not authenticated but OIDC is available - show SSO button on login page
            oidcAvailable = true;
        }
    } catch (e) {
        // OIDC not available - hide SSO button
        oidcAvailable = false;
    }

    // Not authenticated, show login page
    authState.set(null);
    currentView = 'login';
    checkingAuth = false;
}

onMount(() => {
    checkAuth();
});
</script>

<Toast />

{#if checkingAuth}
    <div style="display: flex; align-items: center; justify-content: center; height: 100vh;">
        <p>Loading...</p>
    </div>
{:else if currentView === 'login'}
    <LoginView {oidcAvailable} {checkAuth} />
{:else if currentView === 'admin'}
    <AdminView {checkAuth} />
{:else if currentView === 'user'}
    <UserView {checkAuth} />
{/if}

<style>
/* Global styles are loaded from styles.css */
:global(html),
:global(body) {
    margin: 0;
    padding: 0;
    width: 100%;
    height: 100%;
    display: block !important;
}

:global(#app) {
    width: 100%;
    height: 100%;
    min-height: 100vh;
    display: block;
}
</style>
