import { writable } from 'svelte/store';

// Pagination offsets for different pages
export const usersOffset = writable(0);
export const txOffset = writable(0);
