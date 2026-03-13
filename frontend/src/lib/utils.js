// Utility functions for formatting, debouncing, etc.

// Debounce helper
export function debounce(fn, ms) {
    let timer;
    return function() {
        clearTimeout(timer);
        timer = setTimeout(() => fn(), ms);
    };
}

// Format cents to currency string
export function fmt(cents) {
    return '$' + (Math.abs(cents) / 100).toFixed(2);
}

// Format ISO date to locale string
export function fmtDate(iso) {
    return new Date(iso).toLocaleString();
}

// Get badge class for status
export function getBadgeClass(status) {
    return `badge badge-${status}`;
}

// Format amount with color coding (returns object for Svelte)
export function formatAmount(cents) {
    const cls = cents >= 0 ? 'amount-positive' : 'amount-negative';
    const sign = cents >= 0 ? '+' : '-';
    return {
        class: cls,
        text: `${sign}${fmt(cents)}`
    };
}

// Escape HTML to prevent XSS
export function esc(str) {
    if (str === null || str === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(str);
    return div.innerHTML;
}
