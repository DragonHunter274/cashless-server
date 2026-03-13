<script>
import { onMount } from 'svelte';
import { api } from '../../lib/api.js';
import { fmtDate, formatAmount, getBadgeClass, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';
import { txOffset } from '../../stores/pagination.js';
import { isSuperAdmin } from '../../stores/auth.js';

let loading = true;
let transactions = [];
let error = null;
let uidFilter = '';
let limit = 50;
let superAdminMode = false;

// Edit modal state
let editModalOpen = false;
let editTx = { id: 0, uid: '', amount: 0, product: '', status: 'confirmed', payment_method: '', machine_id: '' };

async function loadTransactions() {
    loading = true;
    error = null;

    try {
        const body = { limit, offset: $txOffset };
        const uid = uidFilter.trim();
        if (uid) body.uid = uid;

        const txs = await api('/getTransactions', body) || [];
        transactions = txs;
    } catch (e) {
        error = e.message;
        addToast('Failed to load transactions: ' + e.message, 'error');
    } finally {
        loading = false;
    }
}

function handlePrevPage() {
    txOffset.update(n => Math.max(0, n - limit));
    loadTransactions();
}

function handleNextPage() {
    txOffset.update(n => n + limit);
    loadTransactions();
}

function handleFilterChange() {
    txOffset.set(0);
    loadTransactions();
}

function openEditModal(tx) {
    editTx = {
        id: tx.transaction_id,
        uid: tx.uid || '',
        amount: tx.amount,
        product: tx.product,
        status: tx.status,
        payment_method: tx.payment_method,
        machine_id: tx.machine_id
    };
    editModalOpen = true;
}

function closeEditModal() {
    editModalOpen = false;
}

async function saveEdit() {
    try {
        const body = {
            id: editTx.id,
            uid: editTx.uid || null,
            amount: parseInt(editTx.amount, 10),
            product: editTx.product,
            status: editTx.status,
            payment_method: editTx.payment_method,
            machine_id: editTx.machine_id
        };
        await api('/editTransaction', body);
        addToast('Transaction updated', 'success');
        editModalOpen = false;
        loadTransactions();
    } catch (e) {
        addToast('Failed to update: ' + e.message, 'error');
    }
}

async function deleteTransaction(id) {
    if (!confirm('Delete transaction #' + id + '? This cannot be undone.')) return;

    try {
        await api('/deleteTransaction', { id });
        addToast('Transaction deleted', 'success');
        loadTransactions();
    } catch (e) {
        addToast('Failed to delete: ' + e.message, 'error');
    }
}

onMount(() => {
    loadTransactions();
});
</script>

<div class="page active">
    <div class="page-header" style="display: flex; align-items: center; justify-content: space-between;">
        <h1>Transactions</h1>
        {#if $isSuperAdmin}
            <label style="display: flex; align-items: center; gap: 8px; font-size: 14px; cursor: pointer; user-select: none;">
                <input type="checkbox" bind:checked={superAdminMode} style="width: 16px; height: 16px; cursor: pointer;" />
                Super Admin Mode
            </label>
        {/if}
    </div>

    <div class="card" style="margin-bottom: 20px;">
        <div style="display: grid; grid-template-columns: 1fr auto; gap: 12px; align-items: end;">
            <div class="form-group" style="margin-bottom: 0;">
                <label for="txFilterUID">Filter by User ID</label>
                <input
                    type="text"
                    id="txFilterUID"
                    bind:value={uidFilter}
                    on:input={handleFilterChange}
                    placeholder="Leave empty for all"
                />
            </div>
            <div class="form-group" style="margin-bottom: 0;">
                <label for="txFilterLimit">Limit</label>
                <select id="txFilterLimit" bind:value={limit} on:change={handleFilterChange}>
                    <option value={25}>25</option>
                    <option value={50}>50</option>
                    <option value={100}>100</option>
                    <option value={200}>200</option>
                </select>
            </div>
        </div>
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
                            <th>ID</th>
                            <th>User</th>
                            <th>Amount</th>
                            <th>Product</th>
                            <th>Machine</th>
                            <th>Method</th>
                            <th>Status</th>
                            <th>Date</th>
                            {#if superAdminMode}
                                <th>Actions</th>
                            {/if}
                        </tr>
                    </thead>
                    <tbody>
                        {#each transactions as tx}
                            {@const amtFormatted = formatAmount(tx.amount)}
                            <tr>
                                <td>{tx.transaction_id}</td>
                                <td>{tx.uid ? esc(tx.uid) : 'N/A'}</td>
                                <td><span class={amtFormatted.class}>{amtFormatted.text}</span></td>
                                <td>{esc(tx.product)}</td>
                                <td>{esc(tx.machine_id)}</td>
                                <td>{esc(tx.payment_method)}</td>
                                <td><span class={getBadgeClass(tx.status)}>{tx.status}</span></td>
                                <td>{fmtDate(tx.created_at)}</td>
                                {#if superAdminMode}
                                    <td style="white-space: nowrap;">
                                        <button class="btn btn-sm btn-secondary" on:click={() => openEditModal(tx)}>Edit</button>
                                        <button class="btn btn-sm btn-danger" on:click={() => deleteTransaction(tx.transaction_id)}>Delete</button>
                                    </td>
                                {/if}
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>

            <div class="pagination" style="margin-top: 16px; display: flex; justify-content: space-between; align-items: center;">
                <button class="btn btn-sm btn-secondary" on:click={handlePrevPage} disabled={$txOffset === 0}>Prev</button>
                <span class="page-info">Showing {$txOffset + 1}-{$txOffset + transactions.length}</span>
                <button class="btn btn-sm btn-secondary" on:click={handleNextPage} disabled={transactions.length < limit}>Next</button>
            </div>
        </div>
    {/if}
</div>

{#if editModalOpen}
    <div class="modal-overlay active" on:click={closeEditModal} role="presentation">
        <div class="modal" on:click|stopPropagation on:keydown role="dialog" aria-modal="true" tabindex="-1">
            <h3>Edit Transaction #{editTx.id}</h3>
            <div class="form-group">
                <label for="editUID">User ID</label>
                <input type="text" id="editUID" bind:value={editTx.uid} placeholder="Leave empty for no user" />
            </div>
            <div class="form-group">
                <label for="editAmount">Amount (cents)</label>
                <input type="number" id="editAmount" bind:value={editTx.amount} />
            </div>
            <div class="form-group">
                <label for="editProduct">Product</label>
                <input type="text" id="editProduct" bind:value={editTx.product} />
            </div>
            <div class="form-group">
                <label for="editStatus">Status</label>
                <select id="editStatus" bind:value={editTx.status}>
                    <option value="pending">pending</option>
                    <option value="confirmed">confirmed</option>
                    <option value="failed">failed</option>
                </select>
            </div>
            <div class="form-group">
                <label for="editMethod">Payment Method</label>
                <input type="text" id="editMethod" bind:value={editTx.payment_method} />
            </div>
            <div class="form-group">
                <label for="editMachine">Machine ID</label>
                <input type="text" id="editMachine" bind:value={editTx.machine_id} />
            </div>
            <div class="modal-actions">
                <button class="btn btn-secondary" on:click={closeEditModal}>Cancel</button>
                <button class="btn btn-primary" on:click={saveEdit}>Save</button>
            </div>
        </div>
    </div>
{/if}
