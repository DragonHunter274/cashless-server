<script>
import { onMount } from 'svelte';
import { authState } from '../../stores/auth.js';
import { api } from '../../lib/api.js';
import { fmtDate, formatAmount, getBadgeClass, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';

let loading = true;
let transactions = [];
let error = null;

async function loadData() {
    const uid = $authState?.username;
    if (!uid) return;

    loading = true;
    error = null;

    try {
        transactions = await api('/getTransactions', { uid, limit: 100 }) || [];
    } catch (e) {
        error = e.message;
        addToast('Failed to load transactions: ' + e.message, 'error');
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
        <h1>My Transactions</h1>
    </div>

    {#if loading}
        <div class="card">
            <p>Loading...</p>
        </div>
    {:else if error}
        <div class="card">
            <p style="color: var(--danger);">{error}</p>
        </div>
    {:else if transactions.length === 0}
        <div class="card">
            <div class="empty-state">
                <p>No transactions found</p>
            </div>
        </div>
    {:else}
        <div class="card">
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Amount</th>
                            <th>Product</th>
                            <th>Machine</th>
                            <th>Method</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each transactions as tx}
                            {@const amtFormatted = formatAmount(tx.amount)}
                            <tr>
                                <td>{fmtDate(tx.created_at)}</td>
                                <td><span class={amtFormatted.class}>{amtFormatted.text}</span></td>
                                <td>{esc(tx.product)}</td>
                                <td>{esc(tx.machine_id)}</td>
                                <td>{esc(tx.payment_method)}</td>
                                <td><span class={getBadgeClass(tx.status)}>{tx.status}</span></td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>
    {/if}
</div>
