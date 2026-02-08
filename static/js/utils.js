// Utility functions for formatting, toasts, modals, etc.

// Debounce helper
function debounce(fn, ms) {
    let timer;
    return function() {
        clearTimeout(timer);
        timer = setTimeout(() => fn(), ms);
    };
}

// Format cents to currency string
function fmt(cents) {
    return '$' + (Math.abs(cents) / 100).toFixed(2);
}

// Format ISO date to locale string
function fmtDate(iso) {
    return new Date(iso).toLocaleString();
}

// Create badge HTML
function badge(status) {
    return `<span class="badge badge-${status}">${status}</span>`;
}

// Create amount HTML with color coding
function amountHtml(cents) {
    const cls = cents >= 0 ? 'amount-positive' : 'amount-negative';
    const sign = cents >= 0 ? '+' : '-';
    return `<span class="${cls}">${sign}${fmt(cents)}</span>`;
}

// Escape HTML to prevent XSS
function esc(str) {
    if (str === null || str === undefined) return '';
    const div = document.createElement('div');
    div.textContent = String(str);
    return div.innerHTML;
}

// Show toast notification
function toast(msg, type = 'success') {
    const el = document.createElement('div');
    el.className = `toast toast-${type}`;
    el.textContent = msg;
    document.getElementById('toasts').appendChild(el);
    setTimeout(() => el.remove(), 4000);
}

// Show modal
function showModal(id) {
    document.getElementById(id).classList.add('active');
}

// Close modal
function closeModal(id) {
    document.getElementById(id).classList.remove('active');
}

// Toggle sidebar (for mobile)
function toggleSidebar() {
    document.getElementById('sidebar').classList.toggle('open');
}
