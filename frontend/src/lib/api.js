// API helper for making authenticated requests to the backend

const API = window.location.origin;

// Get API key from localStorage
export function getKey() {
    return localStorage.getItem('apiKey') || '';
}

// Set API key in localStorage
export function setKey(key) {
    localStorage.setItem('apiKey', key);
}

// Clear API key from localStorage
export function clearKey() {
    localStorage.removeItem('apiKey');
}

// API request helper
// Maintains exact same behavior as vanilla JS version
export async function api(endpoint, body = null) {
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

// Multipart form upload helper (e.g. firmware images).
// Do not set Content-Type manually - the browser sets the multipart boundary.
export async function apiUpload(endpoint, formData) {
    const opts = {
        method: 'POST',
        headers: {},
        credentials: 'same-origin',
        body: formData
    };
    const apiKey = getKey();
    if (apiKey) {
        opts.headers['X-API-Key'] = apiKey;
    }
    const resp = await fetch(API + endpoint, opts);
    const text = await resp.text();
    if (!resp.ok) throw new Error(text || `HTTP ${resp.status}`);
    try { return JSON.parse(text); } catch { return text; }
}
