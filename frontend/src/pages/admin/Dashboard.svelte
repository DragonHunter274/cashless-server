<script>
import { onMount } from 'svelte';
import { api } from '../../lib/api.js';
import { fmt, fmtDate, formatAmount, getBadgeClass, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';

let loading = true;
let stats = null;
let recentTransactions = [];
let error = null;

async function loadData() {
    loading = true;
    error = null;

    try {
        const s = await api('/getStats', {});
        stats = s;
        recentTransactions = s.recent_transactions || [];
    } catch (e) {
        error = e.message;
        addToast('Failed to load dashboard: ' + e.message, 'error');
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
        <h1>Dashboard</h1>
    </div>

    {#if loading}
        <div class="card">
            <p>Loading...</p>
        </div>
    {:else if error}
        <div class="card">
            <p style="color: var(--danger);">{error}</p>
        </div>
    {:else if stats}
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px;">
            <div class="stat-card">
                <div class="stat-label">Total Users</div>
                <div class="stat-value">{stats.total_users}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Revenue</div>
                <div class="stat-value revenue">{fmt(-stats.total_revenue)}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Confirmed Txs</div>
                <div class="stat-value">{stats.confirmed_transactions}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Pending Txs</div>
                <div class="stat-value pending">{stats.pending_transactions}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Failed Txs</div>
                <div class="stat-value failed">{stats.failed_transactions}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Active Vouchers</div>
                <div class="stat-value">{stats.active_vouchers}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Used Vouchers</div>
                <div class="stat-value">{stats.used_vouchers}</div>
            </div>
            <div class="stat-card">
                <div class="stat-label">Privileges</div>
                <div class="stat-value">{stats.total_privileges}</div>
            </div>
        </div>

        <div class="card">
            <h3 style="font-size: 16px; font-weight: 600; margin-bottom: 16px;">Recent Activity</h3>
            {#if recentTransactions.length === 0}
                <div class="empty-state">
                    <p>No transactions yet</p>
                </div>
            {:else}
                <div class="table-wrap">
                    <table>
                        <thead>
                            <tr>
                                <th>ID</th>
                                <th>User</th>
                                <th>Amount</th>
                                <th>Product</th>
                                <th>Status</th>
                                <th>Date</th>
                            </tr>
                        </thead>
                        <tbody>
                            {#each recentTransactions as tx}
                                {@const amtFormatted = formatAmount(tx.amount)}
                                <tr>
                                    <td>{tx.transaction_id}</td>
                                    <td>{tx.uid || 'N/A'}</td>
                                    <td><span class={amtFormatted.class}>{amtFormatted.text}</span></td>
                                    <td>{esc(tx.product)}</td>
                                    <td><span class={getBadgeClass(tx.status)}>{tx.status}</span></td>
                                    <td>{fmtDate(tx.created_at)}</td>
                                </tr>
                            {/each}
                        </tbody>
                    </table>
                </div>
            {/if}
        </div>
    {/if}
</div>
