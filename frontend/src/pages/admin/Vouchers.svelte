<script>
import { onMount } from 'svelte';
import { api } from '../../lib/api.js';
import { fmtDate, getBadgeClass, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';

let loading = true;
let vouchers = [];
let error = null;
let uidFilter = '';

let cvUID = '';
let cvMachine = '';

async function loadVouchers() {
    loading = true;
    error = null;

    try {
        const uid = uidFilter.trim();
        const data = await api('/getVouchers', uid ? { uid } : {});
        vouchers = data || [];
    } catch (e) {
        error = e.message;
        addToast('Failed to load vouchers: ' + e.message, 'error');
    } finally {
        loading = false;
    }
}

async function createVoucherAction() {
    const uid = cvUID.trim();
    const machine_id = cvMachine.trim();

    if (!uid || !machine_id) {
        addToast('Fill all fields', 'error');
        return;
    }

    try {
        await api('/createVoucher', { uid, machine_id });
        addToast('Voucher created', 'success');
        cvUID = '';
        cvMachine = '';
        loadVouchers();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

async function deleteVoucher(id) {
    if (!confirm('Delete this voucher?')) return;

    try {
        await api('/deleteVoucher', { id });
        addToast('Voucher deleted', 'success');
        loadVouchers();
    } catch (e) {
        addToast(e.message, 'error');
    }
}

onMount(() => {
    loadVouchers();
});
</script>

<div class="page active">
    <div class="page-header">
        <h1>Vouchers</h1>
    </div>

    <div class="card" style="margin-bottom: 20px;">
        <h3 style="font-size: 16px; font-weight: 600; margin-bottom: 16px;">Create Voucher</h3>
        <div style="display: grid; grid-template-columns: 1fr 1fr auto; gap: 12px; align-items: end;">
            <div class="form-group" style="margin-bottom: 0;">
                <label for="cvUID">User ID</label>
                <input type="text" id="cvUID" bind:value={cvUID} placeholder="Enter user ID" />
            </div>
            <div class="form-group" style="margin-bottom: 0;">
                <label for="cvMachine">Machine ID</label>
                <input type="text" id="cvMachine" bind:value={cvMachine} placeholder="e.g., VM01" />
            </div>
            <button class="btn btn-primary" on:click={createVoucherAction}>Create</button>
        </div>
    </div>

    <div class="card" style="margin-bottom: 20px;">
        <div class="form-group" style="margin-bottom: 0;">
            <label for="voucherFilterUID">Filter by User ID</label>
            <input
                type="text"
                id="voucherFilterUID"
                bind:value={uidFilter}
                on:input={loadVouchers}
                placeholder="Leave empty for all"
            />
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
                            <th>User</th>
                            <th>Machine</th>
                            <th>Status</th>
                            <th>Created</th>
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each vouchers as v}
                            <tr>
                                <td>{v.id}</td>
                                <td>{esc(v.uid)}</td>
                                <td>{esc(v.machine_id)}</td>
                                <td><span class={getBadgeClass(v.used ? 'used' : 'available')}>{v.used ? 'used' : 'available'}</span></td>
                                <td>{fmtDate(v.created_at)}</td>
                                <td>
                                    {#if !v.used}
                                        <button class="btn btn-sm btn-danger" on:click={() => deleteVoucher(v.id)}>Delete</button>
                                    {/if}
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        </div>
    {/if}
</div>
