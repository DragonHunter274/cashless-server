<script>
import { onMount } from 'svelte';
import { api } from '../../lib/api.js';
import { fmt, esc } from '../../lib/utils.js';
import { addToast } from '../../stores/toast.js';
import { pendingTxs } from '../../stores/pending.js';

let dpUID = '';
let dpAmount = '';
let dpProduct = '';
let dpMachine = '';

let cpAmount = '';
let cpProduct = '';
let cpMachine = '';

async function makeDigitalPurchase() {
    const uid = dpUID.trim();
    const amount = parseInt(dpAmount);
    const product = parseInt(dpProduct);
    const machine_id = dpMachine.trim();

    if (!uid || !amount || !product || !machine_id) {
        addToast('Fill all fields', 'error');
        return;
    }

    try {
        const res = await api('/makePurchase', { uid, amount, product, machine_id });
        pendingTxs.update(txs => [...txs, { ...res, uid, amount, product }]);
        addToast('Purchase created (pending). ID: ' + res.transaction_id, 'success');
    } catch (e) {
        addToast(e.message, 'error');
    }
}

async function makeCashPurchaseAction() {
    const amount = parseInt(cpAmount);
    const product = parseInt(cpProduct);
    const machine_id = cpMachine.trim();

    if (!amount || !product || !machine_id) {
        addToast('Fill all fields', 'error');
        return;
    }

    try {
        const res = await api('/makeCashPurchase', { amount, product, machine_id });
        addToast('Cash purchase confirmed. ID: ' + res.transaction_id, 'success');
        cpAmount = '';
        cpProduct = '';
    } catch (e) {
        addToast(e.message, 'error');
    }
}

async function confirmPurchase(id) {
    try {
        await api('/confirmPurchase', { transaction_id: id });
        pendingTxs.update(txs => txs.filter(tx => tx.transaction_id !== id));
        addToast('Purchase confirmed', 'success');
    } catch (e) {
        addToast(e.message, 'error');
    }
}

onMount(() => {
    // No data loading needed, just forms
});
</script>

<div class="page active">
    <div class="page-header">
        <h1>Purchases</h1>
    </div>

    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 24px;">
        <!-- Digital Purchase Form -->
        <div class="card">
            <h3 style="font-size: 16px; font-weight: 600; margin-bottom: 16px;">Digital Purchase</h3>
            <div class="form-group">
                <label for="dpUID">User ID</label>
                <input type="text" id="dpUID" bind:value={dpUID} placeholder="Enter user ID" />
            </div>
            <div class="form-group">
                <label for="dpAmount">Amount (cents)</label>
                <input type="number" id="dpAmount" bind:value={dpAmount} placeholder="e.g., 150" />
            </div>
            <div class="form-group">
                <label for="dpProduct">Product ID</label>
                <input type="number" id="dpProduct" bind:value={dpProduct} placeholder="e.g., 1" />
            </div>
            <div class="form-group">
                <label for="dpMachine">Machine ID</label>
                <input type="text" id="dpMachine" bind:value={dpMachine} placeholder="e.g., VM01" />
            </div>
            <button class="btn btn-primary" on:click={makeDigitalPurchase}>Create Purchase</button>
        </div>

        <!-- Cash Purchase Form -->
        <div class="card">
            <h3 style="font-size: 16px; font-weight: 600; margin-bottom: 16px;">Cash Purchase</h3>
            <div class="form-group">
                <label for="cpAmount">Amount (cents)</label>
                <input type="number" id="cpAmount" bind:value={cpAmount} placeholder="e.g., 150" />
            </div>
            <div class="form-group">
                <label for="cpProduct">Product ID</label>
                <input type="number" id="cpProduct" bind:value={cpProduct} placeholder="e.g., 1" />
            </div>
            <div class="form-group">
                <label for="cpMachine">Machine ID</label>
                <input type="text" id="cpMachine" bind:value={cpMachine} placeholder="e.g., VM01" />
            </div>
            <button class="btn btn-primary" on:click={makeCashPurchaseAction}>Record Cash Purchase</button>
        </div>
    </div>

    <!-- Pending Transactions -->
    <div class="card">
        <h3 style="font-size: 16px; font-weight: 600; margin-bottom: 16px;">Pending Transactions</h3>
        {#if $pendingTxs.length === 0}
            <div class="empty-state">
                <p>No pending purchases</p>
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
                            <th>Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {#each $pendingTxs as tx}
                            <tr>
                                <td>{tx.transaction_id}</td>
                                <td>{esc(tx.uid || 'N/A')}</td>
                                <td>{fmt(tx.amount)}</td>
                                <td>{tx.product}</td>
                                <td>
                                    <button class="btn btn-sm btn-primary" on:click={() => confirmPurchase(tx.transaction_id)}>Confirm</button>
                                </td>
                            </tr>
                        {/each}
                    </tbody>
                </table>
            </div>
        {/if}
    </div>
</div>
