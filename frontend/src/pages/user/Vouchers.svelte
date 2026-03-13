<script>
import { onMount } from 'svelte';
import { authState } from '../../stores/auth.js';
import { api } from '../../lib/api.js';
import { fmtDate, getBadgeClass, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';

let loading = true;
let vouchers = [];
let error = null;

async function loadData() {
    const uid = $authState?.username;
    if (!uid) return;

    loading = true;
    error = null;

    try {
        vouchers = await api('/getVouchers', { uid }) || [];
    } catch (e) {
        error = e.message;
        addToast('Failed to load vouchers: ' + e.message, 'error');
    } finally {
        loading = false;
    }
}

onMount(() => {
    loadData();
});
</script>

<div class="page active">
    <div class="page-header">
        <h1>My Vouchers</h1>
    </div>

    {#if loading}
        <div class="card">
            <p>Loading...</p>
        </div>
    {:else if error}
        <div class="card">
            <p style="color: var(--danger);">{error}</p>
        </div>
    {:else if vouchers.length === 0}
        <div class="card">
            <div class="empty-state">
                <p>No vouchers found</p>
            </div>
        </div>
    {:else}
        <div class="card">
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>ID</th>
                            <th>Machine</th>
                            <th>Status</th>
                            <th>Created</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each vouchers as v}
                            <tr>
                                <td>{v.id}</td>
                                <td>{esc(v.machine_id)}</td>
                                <td><span class={getBadgeClass(v.used ? 'used' : 'available')}>{v.used ? 'used' : 'available'}</span></td>
                                <td>{fmtDate(v.created_at)}</td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>
    {/if}
</div>
