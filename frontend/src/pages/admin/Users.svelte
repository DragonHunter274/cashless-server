<script>
import { onMount } from 'svelte';
import { api } from '../../lib/api.js';
import { fmt, fmtDate, formatAmount, getBadgeClass, esc, debounce } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';
import { usersOffset } from '../../stores/pagination.js';

let loading = true;
let users = [];
let total = 0;
let error = null;
let search = '';

// User detail state
let selectedUID = null;
let userDetail = null;
let detailLoading = false;

// Modal state
let showCreateUserModal = false;
let showTopUpModal = false;
let newUserUID = '';
let topUpUID = '';
let topUpAmount = '';

const limit = 25;

async function loadUsers() {
    loading = true;
    error = null;

    try {
        const data = await api('/getUsers', {
            search: search.trim(),
            limit,
            offset: $usersOffset
        });
        users = data.users || [];
        total = data.total || 0;
    } catch (e) {
        error = e.message;
        addToast('Failed to load users: ' + e.message, 'error');
    } finally {
        loading = false;
    }
}

const debouncedLoadUsers = debounce(loadUsers, 300);

function handleSearchInput(e) {
    search = e.target.value;
    usersOffset.set(0);
    debouncedLoadUsers();
}

function handlePrevPage() {
    usersOffset.update(n => Math.max(0, n - limit));
    loadUsers();
}

function handleNextPage() {
    usersOffset.update(n => n + limit);
    loadUsers();
}

async function showUserDetail(uid) {
    selectedUID = uid;
    detailLoading = true;
    userDetail = null;

    try {
        const [balance, txs, vouchers, privileges] = await Promise.all([
            api('/getBalance', { uid }),
            api('/getTransactions', { uid, limit: 20 }),
            api('/getVouchers', { uid }),
            api('/getPrivileges', { uid })
        ]);

        userDetail = {
            uid,
            balance: balance.balance,
            transactions: txs || [],
            vouchers: vouchers || [],
            privileges: privileges || []
        };
    } catch (e) {
        addToast('Failed to load user details: ' + e.message, 'error');
    } finally {
        detailLoading = false;
    }
}

function closeUserDetail() {
    selectedUID = null;
    userDetail = null;
}

function openCreateUserModal() {
    showCreateUserModal = true;
    newUserUID = '';
    setTimeout(() => document.getElementById('newUserUID')?.focus(), 100);
}

function closeCreateUserModal() {
    showCreateUserModal = false;
    newUserUID = '';
}

async function createUserAction() {
    const uid = newUserUID.trim();
    if (!uid) {
        addToast('Enter a UID', 'error');
        return;
    }

    try {
        await api('/createUser', { uid });
        addToast('User created: ' + uid, 'success');
        closeCreateUserModal();
        loadUsers();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

function openTopUpModal(uid) {
    topUpUID = uid;
    topUpAmount = '';
    showTopUpModal = true;
    setTimeout(() => document.getElementById('topUpAmount')?.focus(), 100);
}

function closeTopUpModal() {
    showTopUpModal = false;
    topUpUID = '';
    topUpAmount = '';
}

async function topUpAction() {
    const amount = parseInt(topUpAmount);
    if (!amount || amount <= 0) {
        addToast('Enter a valid amount', 'error');
        return;
    }

    try {
        await api('/topUp', { uid: topUpUID, amount });
        addToast(`Topped up ${topUpUID} with ${fmt(amount)}`, 'success');
        closeTopUpModal();
        showUserDetail(topUpUID);
        loadUsers();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

onMount(() => {
    loadUsers();
});

$: totalPages = Math.ceil(total / limit);
$: currentPage = Math.floor($usersOffset / limit) + 1;
</script>

<div class="page active">
    <div class="page-header">
        <h1>Users</h1>
        <button class="btn btn-primary" on:click={openCreateUserModal}>Create User</button>
    </div>

    <div class="card" style="margin-bottom: 20px;">
        <input
            type="text"
            placeholder="Search users..."
            value={search}
            on:input={handleSearchInput}
            style="width: 100%; padding: 8px 12px; border: 1px solid var(--border); border-radius: 6px; background: var(--surface); color: var(--text);"
        />
    </div>

    {#if loading}
        <div class="card">
            <p>Loading...</p>
        </div>
    {:else if error}
        <div class="card">
            <p style="color: var(--danger);">{error}</p>
        </div>
    {:else if users.length === 0}
        <div class="card">
            <div class="empty-state">
                <p>No users found</p>
            </div>
        </div>
    {:else}
        <div class="card">
            <div class="table-wrap">
                <table>
                    <thead>
                        <tr>
                            <th>UID</th>
                            <th>Balance</th>
                            <th>Created</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each users as u}
                            {@const amtFormatted = formatAmount(u.balance)}
                            <tr class="clickable-row" on:click={() => showUserDetail(u.uid)}>
                                <td><strong>{esc(u.uid)}</strong></td>
                                <td><span class={amtFormatted.class}>{amtFormatted.text}</span></td>
                                <td>{fmtDate(u.created_at)}</td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>

            <div class="pagination" style="margin-top: 16px; display: flex; justify-content: space-between; align-items: center;">
                <button class="btn btn-sm btn-secondary" on:click={handlePrevPage} disabled={$usersOffset === 0}>Prev</button>
                <span class="page-info">Page {currentPage} of {totalPages} ({total} users)</span>
                <button class="btn btn-sm btn-secondary" on:click={handleNextPage} disabled={currentPage >= totalPages}>Next</button>
            </div>
        </div>
    {/if}

    {#if selectedUID && userDetail}
        <div class="card" style="margin-top: 20px;">
            <div class="detail-panel">
                <div class="detail-header">
                    <h3>{esc(userDetail.uid)}</h3>
                    <div>
                        <button class="btn btn-sm btn-primary" on:click={() => openTopUpModal(userDetail.uid)}>Top Up</button>
                        <button class="btn btn-sm btn-secondary" on:click={closeUserDetail}>Close</button>
                    </div>
                </div>
                <div class="detail-balance {userDetail.balance >= 0 ? 'amount-positive' : 'amount-negative'}">
                    {fmt(userDetail.balance)}
                </div>

                <div class="detail-section">
                    <h4>Vouchers ({userDetail.vouchers.length})</h4>
                    {#if userDetail.vouchers.length === 0}
                        <p style="color:var(--text-muted);font-size:13px;">None</p>
                    {:else}
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
                                    {#each userDetail.vouchers as v}
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
                    {/if}
                </div>

                <div class="detail-section">
                    <h4>Privileges ({userDetail.privileges.length})</h4>
                    {#if userDetail.privileges.length === 0}
                        <p style="color:var(--text-muted);font-size:13px;">None</p>
                    {:else}
                        <div class="table-wrap">
                            <table>
                                <thead>
                                    <tr>
                                        <th>Machine</th>
                                        <th>Free Vend</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {#each userDetail.privileges as p}
                                        <tr>
                                            <td>{esc(p.machine_id)}</td>
                                            <td><span class={getBadgeClass(p.free_vend ? 'yes' : 'no')}>{p.free_vend ? 'yes' : 'no'}</span></td>
                                        </tr>
                                    {/each}
                                </tbody>
                            </table>
                        </div>
                    {/if}
                </div>

                <div class="detail-section">
                    <h4>Recent Transactions ({userDetail.transactions.length})</h4>
                    {#if userDetail.transactions.length === 0}
                        <p style="color:var(--text-muted);font-size:13px;">None</p>
                    {:else}
                        <div class="table-wrap">
                            <table>
                                <thead>
                                    <tr>
                                        <th>Date</th>
                                        <th>Amount</th>
                                        <th>Product</th>
                                        <th>Machine</th>
                                        <th>Status</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {#each userDetail.transactions as tx}
                                        {@const amtFormatted = formatAmount(tx.amount)}
                                        <tr>
                                            <td>{fmtDate(tx.created_at)}</td>
                                            <td><span class={amtFormatted.class}>{amtFormatted.text}</span></td>
                                            <td>{esc(tx.product)}</td>
                                            <td>{esc(tx.machine_id)}</td>
                                            <td><span class={getBadgeClass(tx.status)}>{tx.status}</span></td>
                                        </tr>
                                    {/each}
                                </tbody>
                            </table>
                        </div>
                    {/if}
                </div>
            </div>
        </div>
    {:else if selectedUID && detailLoading}
        <div class="card" style="margin-top: 20px;">
            <p>Loading user details...</p>
        </div>
    {/if}
</div>

<!-- Create User Modal -->
{#if showCreateUserModal}
    <div class="modal-overlay" on:click={closeCreateUserModal} role="button" tabindex="-1" on:keydown={(e) => e.key === 'Escape' && closeCreateUserModal()}>
        <div class="modal" on:click|stopPropagation on:keydown role="dialog" aria-modal="true" tabindex="-1">
            <div class="modal-header">
                <h3>Create User</h3>
                <button class="close-btn" on:click={closeCreateUserModal} aria-label="Close">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label for="newUserUID">User ID</label>
                    <input
                        type="text"
                        id="newUserUID"
                        bind:value={newUserUID}
                        on:keydown={(e) => e.key === 'Enter' && createUserAction()}
                        placeholder="Enter user ID"
                    />
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" on:click={closeCreateUserModal}>Cancel</button>
                <button class="btn btn-primary" on:click={createUserAction}>Create</button>
            </div>
        </div>
    </div>
{/if}

<!-- Top Up Modal -->
{#if showTopUpModal}
    <div class="modal-overlay" on:click={closeTopUpModal} role="button" tabindex="-1" on:keydown={(e) => e.key === 'Escape' && closeTopUpModal()}>
        <div class="modal" on:click|stopPropagation on:keydown role="dialog" aria-modal="true" tabindex="-1">
            <div class="modal-header">
                <h3>Top Up User</h3>
                <button class="close-btn" on:click={closeTopUpModal} aria-label="Close">&times;</button>
            </div>
            <div class="modal-body">
                <div class="form-group">
                    <label for="topUpUID">User ID</label>
                    <input
                        type="text"
                        id="topUpUID"
                        value={topUpUID}
                        disabled
                    />
                </div>
                <div class="form-group">
                    <label for="topUpAmount">Amount (cents)</label>
                    <input
                        type="number"
                        id="topUpAmount"
                        bind:value={topUpAmount}
                        on:keydown={(e) => e.key === 'Enter' && topUpAction()}
                        placeholder="e.g., 1000 for €10.00"
                    />
                </div>
            </div>
            <div class="modal-footer">
                <button class="btn btn-secondary" on:click={closeTopUpModal}>Cancel</button>
                <button class="btn btn-primary" on:click={topUpAction}>Top Up</button>
            </div>
        </div>
    </div>
{/if}
