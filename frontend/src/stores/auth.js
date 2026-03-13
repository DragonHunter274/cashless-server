import { writable, derived } from 'svelte/store';

// Authentication state
// Structure: { isAuthenticated: boolean, isAdmin: boolean, username: string, email: string, method: string }
export const authState = writable(null);

// Derived stores for convenience
export const isAuthenticated = derived(authState, $auth => $auth !== null);
export const isAdmin = derived(authState, $auth => $auth?.isAdmin || false);
export const isUser = derived(authState, $auth => $auth !== null && $auth?.isAdmin === false);
export const isSuperAdmin = derived(authState, $auth => $auth?.isSuperAdmin || false);
