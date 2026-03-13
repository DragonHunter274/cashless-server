import { writable } from 'svelte/store';

// Check for saved theme preference or default to 'light'
const savedTheme = typeof window !== 'undefined' ? localStorage.getItem('theme') || 'light' : 'light';

export const theme = writable(savedTheme);

// Subscribe to theme changes and update localStorage + HTML class
theme.subscribe(value => {
    if (typeof window !== 'undefined') {
        localStorage.setItem('theme', value);
        if (value === 'dark') {
            document.documentElement.classList.add('dark');
        } else {
            document.documentElement.classList.remove('dark');
        }
    }
});

export function toggleTheme() {
    theme.update(current => current === 'light' ? 'dark' : 'light');
}
