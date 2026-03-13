import { writable } from 'svelte/store';

// Toast notification store
// Each toast has: id, message, type ('success' or 'error')
const toasts = writable([]);

let nextId = 0;

// Add a toast notification
export function addToast(message, type = 'success') {
    const id = nextId++;
    toasts.update(t => [...t, { id, message, type }]);

    // Auto-remove after 4 seconds
    setTimeout(() => {
        removeToast(id);
    }, 4000);
}

// Remove a toast notification
export function removeToast(id) {
    toasts.update(t => t.filter(toast => toast.id !== id));
}

export default toasts;
